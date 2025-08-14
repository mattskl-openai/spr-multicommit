use std::env;
use std::error::Error;
use std::process::Command;

const HELP: &str = r#"
spr creates a series of GitHub PRs that are 'stacked' by managing the base branch
Each commit that should start a new PR should be tagged with `pr:<unique name>`
spr update creates 1 PR per commit with a `pr:<tag>`
Any intermediate commits without a tag get added to the first ancestor PR that has a tag, as a separate commit
"#;

fn print_help() {
    println!("{HELP}");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // Print usage info when requested and exit early
    if args.iter().any(|a| a == "--help" || a == "--h") {
        print_help();
        return;
    }

    // Dispatch to the `update` subcommand when invoked
    if args.get(1).map(|s| s.as_str()) == Some("update") {
        let dry_run = args.iter().any(|a| a == "--dry-run");
        let mut runner = RealRunner;
        if let Err(e) = update_with_runner(dry_run, &mut runner) {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
}

#[derive(Debug, Clone)]
struct Commit {
    hash: String,
    message: String,
}

#[derive(Debug, Clone)]
struct Pr {
    tag: String,
    base: String,
    commits: Vec<Commit>,
}

#[derive(Clone)]
struct CmdOutput {
    status: i32,
    stdout: String,
}

trait Runner {
    fn run(&mut self, cmd: &str, args: &[&str]) -> Result<CmdOutput, Box<dyn Error>>;
}

struct RealRunner;

impl Runner for RealRunner {
    fn run(&mut self, cmd: &str, args: &[&str]) -> Result<CmdOutput, Box<dyn Error>> {
        let output = Command::new(cmd).args(args).output()?;
        Ok(CmdOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8(output.stdout)?,
        })
    }
}

fn update_with_runner<R: Runner>(dry_run: bool, runner: &mut R) -> Result<(), Box<dyn Error>> {
    // Determine base branch and stack prefix from the environment
    let main_branch = env::var("SPR_MAIN_BRANCH").unwrap_or_else(|_| "main".to_string());
    let spr_name = env::var("SPR_NAME").unwrap_or_else(|_| "spr".to_string());

    // Collect commits that are newer than the base branch so we can figure out
    // which PRs need to be updated
    let out = runner.run(
        "git",
        &[
            "log",
            &format!("{}..HEAD", main_branch),
            "--oneline",
            "--reverse",
        ],
    )?;
    if out.status != 0 {
        return Err("git log failed".into());
    }
    let log = out.stdout;

    // Track the set of PRs we need to update and the most recent tag seen
    let mut prs: Vec<Pr> = Vec::new();
    let mut current_tag: Option<String> = None;

    // Walk each commit ahead of the base branch
    for line in log.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, ' ');
        let hash = parts.next().unwrap().to_string();
        let message = parts.next().unwrap_or("").to_string();

        // Start a new PR whenever a commit carries a `pr:<tag>` marker
        if let Some(tag) = extract_tag(&message) {
            current_tag = Some(tag.clone());
            let base = prs
                .last()
                .map(|p| format!("{}/{}", spr_name, p.tag))
                .unwrap_or_else(|| main_branch.clone());
            prs.push(Pr {
                tag,
                base,
                commits: Vec::new(),
            });
        } else if current_tag.is_none() {
            // An untagged commit at the start has nowhere to go
            return Err(format!("commit {} has no pr:<tag>", hash).into());
        }

        // Skip commits that already exist on the remote branch
        let tag = current_tag.clone().unwrap();
        let branch = format!("{}/{}", spr_name, tag);
        let res = runner.run(
            "git",
            &[
                "merge-base",
                "--is-ancestor",
                &hash,
                &format!("origin/{}", branch),
            ],
        );
        let already = matches!(res, Ok(ref o) if o.status == 0);
        if already {
            continue;
        }

        // Record the commit (minus any tag marker) for the appropriate PR
        let pr = prs.iter_mut().find(|p| p.tag == tag).unwrap();
        let msg = if extract_tag(&message).is_some() {
            strip_tag(&message)
        } else {
            message
        };
        pr.commits.push(Commit { hash, message: msg });
    }

    // Push updated branches and create PRs when not in dry-run mode
    for pr in prs {
        if pr.commits.is_empty() {
            continue;
        }
        let branch = format!("{}/{}", spr_name, pr.tag);
        let head = pr.commits.last().unwrap().hash.clone();
        if dry_run {
            println!("git push origin {}:refs/heads/{}", head, branch);
            println!("gh pr create --base {} --head {}", pr.base, branch);
        } else {
            runner.run(
                "git",
                &["push", "origin", &format!("{}:refs/heads/{}", head, branch)],
            )?;
            runner.run(
                "gh",
                &["pr", "create", "--base", &pr.base, "--head", &branch],
            )?;
        }
    }

    Ok(())
}

fn extract_tag(message: &str) -> Option<String> {
    for word in message.split_whitespace() {
        if let Some(tag) = word.strip_prefix("pr:") {
            return Some(tag.to_string());
        }
    }
    None
}

fn strip_tag(message: &str) -> String {
    message
        .split_whitespace()
        .filter(|w| !w.starts_with("pr:"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockRunner {
        scripts: Vec<(Vec<String>, CmdOutput)>,
        idx: usize,
    }

    impl MockRunner {
        fn new(scripts: Vec<(Vec<String>, CmdOutput)>) -> Self {
            Self { scripts, idx: 0 }
        }
    }

    impl Runner for MockRunner {
        fn run(&mut self, cmd: &str, args: &[&str]) -> Result<CmdOutput, Box<dyn Error>> {
            let call = std::iter::once(cmd.to_string())
                .chain(args.iter().map(|s| s.to_string()))
                .collect::<Vec<_>>();
            let (expected, out) = &self.scripts[self.idx];
            assert_eq!(call, *expected);
            self.idx += 1;
            Ok(out.clone())
        }
    }

    #[test]
    fn update_dry_run_only_queries_git() {
        // Simulated `git log` with two PRs (addThings2 and addNewThings3)
        let log = "a Add some thigns 2 pr:addThings2\nb Fixes to 2\nc adding some things 3 pr:addNewThings3\n";

        // Only `git` commands should run in dry-run mode
        let scripts = vec![
            (
                vec![
                    "git".into(),
                    "log".into(),
                    "main..HEAD".into(),
                    "--oneline".into(),
                    "--reverse".into(),
                ],
                CmdOutput {
                    status: 0,
                    stdout: log.into(),
                },
            ),
            (
                vec![
                    "git".into(),
                    "merge-base".into(),
                    "--is-ancestor".into(),
                    "a".into(),
                    "origin/spr/addThings2".into(),
                ],
                CmdOutput {
                    status: 1,
                    stdout: String::new(),
                },
            ),
            (
                vec![
                    "git".into(),
                    "merge-base".into(),
                    "--is-ancestor".into(),
                    "b".into(),
                    "origin/spr/addThings2".into(),
                ],
                CmdOutput {
                    status: 1,
                    stdout: String::new(),
                },
            ),
            (
                vec![
                    "git".into(),
                    "merge-base".into(),
                    "--is-ancestor".into(),
                    "c".into(),
                    "origin/spr/addNewThings3".into(),
                ],
                CmdOutput {
                    status: 1,
                    stdout: String::new(),
                },
            ),
        ];
        let mut runner = MockRunner::new(scripts);
        update_with_runner(true, &mut runner).unwrap();
        assert_eq!(runner.idx, 4); // only git commands executed
    }

    #[test]
    fn update_pushes_and_creates_prs() {
        // Simulated `git log` with two PRs (addThings2 and addNewThings3)
        let log = "a Add some thigns 2 pr:addThings2\nb Fixes to 2\nc adding some things 3 pr:addNewThings3\n";

        // In full mode we expect pushes and PR creations after the queries
        let scripts = vec![
            (
                vec![
                    "git".into(),
                    "log".into(),
                    "main..HEAD".into(),
                    "--oneline".into(),
                    "--reverse".into(),
                ],
                CmdOutput {
                    status: 0,
                    stdout: log.into(),
                },
            ),
            (
                vec![
                    "git".into(),
                    "merge-base".into(),
                    "--is-ancestor".into(),
                    "a".into(),
                    "origin/spr/addThings2".into(),
                ],
                CmdOutput {
                    status: 1,
                    stdout: String::new(),
                },
            ),
            (
                vec![
                    "git".into(),
                    "merge-base".into(),
                    "--is-ancestor".into(),
                    "b".into(),
                    "origin/spr/addThings2".into(),
                ],
                CmdOutput {
                    status: 1,
                    stdout: String::new(),
                },
            ),
            (
                vec![
                    "git".into(),
                    "merge-base".into(),
                    "--is-ancestor".into(),
                    "c".into(),
                    "origin/spr/addNewThings3".into(),
                ],
                CmdOutput {
                    status: 1,
                    stdout: String::new(),
                },
            ),
            (
                vec![
                    "git".into(),
                    "push".into(),
                    "origin".into(),
                    "b:refs/heads/spr/addThings2".into(),
                ],
                CmdOutput {
                    status: 0,
                    stdout: String::new(),
                },
            ),
            (
                vec![
                    "gh".into(),
                    "pr".into(),
                    "create".into(),
                    "--base".into(),
                    "main".into(),
                    "--head".into(),
                    "spr/addThings2".into(),
                ],
                CmdOutput {
                    status: 0,
                    stdout: String::new(),
                },
            ),
            (
                vec![
                    "git".into(),
                    "push".into(),
                    "origin".into(),
                    "c:refs/heads/spr/addNewThings3".into(),
                ],
                CmdOutput {
                    status: 0,
                    stdout: String::new(),
                },
            ),
            (
                vec![
                    "gh".into(),
                    "pr".into(),
                    "create".into(),
                    "--base".into(),
                    "spr/addThings2".into(),
                    "--head".into(),
                    "spr/addNewThings3".into(),
                ],
                CmdOutput {
                    status: 0,
                    stdout: String::new(),
                },
            ),
        ];
        let mut runner = MockRunner::new(scripts);
        update_with_runner(false, &mut runner).unwrap();
        assert_eq!(runner.idx, 8);
    }
}
