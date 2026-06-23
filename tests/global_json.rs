use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

fn run_spr(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_spr"))
        .args(args)
        .output()
        .unwrap()
}

fn stdout_json(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn init_detached_update_repo() -> (TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    fs::create_dir(&repo).unwrap();
    git(&repo, ["init", "-b", "main"].as_slice());
    git(
        &repo,
        ["config", "user.email", "spr@example.com"].as_slice(),
    );
    git(&repo, ["config", "user.name", "SPR Tests"].as_slice());
    fs::write(repo.join("README.md"), "init\n").unwrap();
    git(&repo, ["add", "README.md"].as_slice());
    git(&repo, ["commit", "-m", "init"].as_slice());
    let origin = dir.path().join("origin.git");
    git(
        &repo,
        ["init", "--bare", origin.to_str().unwrap()].as_slice(),
    );
    git(
        &repo,
        ["remote", "add", "origin", origin.to_str().unwrap()].as_slice(),
    );
    git(&repo, ["push", "-u", "origin", "main"].as_slice());
    git(&repo, ["checkout", "-b", "stack"].as_slice());
    fs::write(repo.join("alpha.txt"), "alpha\n").unwrap();
    git(&repo, ["add", "alpha.txt"].as_slice());
    git(
        &repo,
        ["commit", "-m", "feat: alpha start pr:alpha"].as_slice(),
    );
    git(&repo, ["checkout", "--detach", "HEAD"].as_slice());
    (dir, repo)
}

fn run_detached_update(repo: &Path, home: &Path, json: bool) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_spr"));
    command
        .env("HOME", home)
        .args(["--cd", repo.to_str().unwrap(), "--base", "main"])
        .args(["--prefix", "test-spr/", "update", "--no-pr"]);
    if json {
        command.arg("--json");
    }
    command.output().unwrap()
}

fn assert_alpha_remote_exists(repo: &Path) {
    assert!(!git(
        repo,
        ["ls-remote", "--heads", "origin", "test-spr/alpha"].as_slice(),
    )
    .trim()
    .is_empty());
}

fn commit_file(repo: &Path, file: &str, contents: &str, message: &str) {
    fs::write(repo.join(file), contents).unwrap();
    git(repo, ["add", file].as_slice());
    git(repo, ["commit", "-m", message].as_slice());
}

fn stack_metadata_path(repo: &Path) -> PathBuf {
    let git_common_dir = git(repo, ["rev-parse", "--git-common-dir"].as_slice());
    let git_common_dir = git_common_dir.trim();
    let git_common_path = Path::new(git_common_dir);
    let git_common_path = if git_common_path.is_absolute() {
        git_common_path.to_path_buf()
    } else {
        repo.join(git_common_path)
    };
    git_common_path.join("spr").join("stack_metadata_v1.json")
}

fn read_stack_metadata(repo: &Path) -> Value {
    serde_json::from_str(&fs::read_to_string(stack_metadata_path(repo)).unwrap()).unwrap()
}

fn write_stack_metadata(repo: &Path, metadata: &Value) {
    fs::write(
        stack_metadata_path(repo),
        serde_json::to_string_pretty(metadata).unwrap(),
    )
    .unwrap();
}

fn mutate_first_stack_record(
    repo: &Path,
    mutate: impl FnOnce(&mut serde_json::Map<String, Value>),
) {
    let mut metadata = read_stack_metadata(repo);
    let stacks = metadata
        .get_mut("stacks")
        .and_then(Value::as_object_mut)
        .unwrap();
    let stack_id = stacks.keys().next().unwrap().clone();
    let stack_record = stacks
        .get_mut(&stack_id)
        .and_then(Value::as_object_mut)
        .unwrap();
    mutate(stack_record);
    write_stack_metadata(repo, &metadata);
}

fn init_stale_absorb_metadata_repo() -> (TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    fs::create_dir(&repo).unwrap();
    git(&repo, ["init", "-b", "main"].as_slice());
    git(
        &repo,
        ["config", "user.email", "spr@example.com"].as_slice(),
    );
    git(&repo, ["config", "user.name", "SPR Tests"].as_slice());
    fs::write(repo.join("README.md"), "init\n").unwrap();
    git(&repo, ["add", "README.md"].as_slice());
    git(&repo, ["commit", "-m", "init"].as_slice());
    let origin = dir.path().join("origin.git");
    git(
        &repo,
        ["init", "--bare", origin.to_str().unwrap()].as_slice(),
    );
    git(
        &repo,
        ["remote", "add", "origin", origin.to_str().unwrap()].as_slice(),
    );
    git(&repo, ["push", "-u", "origin", "main"].as_slice());

    git(&repo, ["checkout", "-b", "stack"].as_slice());
    commit_file(
        &repo,
        "alpha.txt",
        "alpha-1\n",
        "feat: alpha start pr:alpha",
    );
    commit_file(
        &repo,
        "alpha.txt",
        "alpha-1\nalpha-2\n",
        "feat: alpha follow-up",
    );
    commit_file(&repo, "beta.txt", "beta-1\n", "feat: beta start pr:beta");

    let update = run_spr(&[
        "--cd",
        repo.to_str().unwrap(),
        "--base",
        "main",
        "--prefix",
        "dank-spr/",
        "--local-pr-branches",
        "create-or-update",
        "update",
        "--no-pr",
        "--json",
    ]);
    assert!(
        update.status.success(),
        "spr update --no-pr failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&update.stdout),
        String::from_utf8_lossy(&update.stderr)
    );

    git(&repo, ["checkout", "stack"].as_slice());
    commit_file(
        &repo,
        "beta.txt",
        "beta-1\nbeta-stack-drift\n",
        "feat: beta stack drift",
    );
    git(&repo, ["checkout", "dank-spr/alpha"].as_slice());
    commit_file(
        &repo,
        "alpha.txt",
        "alpha-1\nalpha-2\nalpha-branch-tail\n",
        "feat: alpha branch tail",
    );
    (dir, repo)
}

fn assert_absorb_changed_branches(output: std::process::Output, changed_branches: &[&str]) {
    assert!(
        output.status.success(),
        "spr absorb query failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let json = stdout_json(&output);
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["command"], "absorb");
    assert_eq!(json["result"], "query");
    assert_eq!(
        json["data"]["changed_branches"],
        serde_json::json!(changed_branches)
    );
}

fn assert_absorb_completed(output: std::process::Output, destination_branch: &str) {
    assert!(
        output.status.success(),
        "spr absorb failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let json = stdout_json(&output);
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["command"], "absorb");
    assert_eq!(json["result"], "completed");
    assert_eq!(json["destination_branch"], destination_branch);
}

fn assert_absorb_json_error_contains(output: std::process::Output, expected_message: &str) {
    assert!(!output.status.success());
    assert!(output.stderr.is_empty());
    let json = stdout_json(&output);
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["command"], "absorb");
    assert_eq!(json["result"], "error");
    assert_eq!(json["error_kind"], "internal");
    assert!(
        json["message"].as_str().unwrap().contains(expected_message),
        "{json}"
    );
}

#[test]
fn json_help_outputs_structured_root_help() {
    let output = run_spr(&["--json", "--help"]);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let json = stdout_json(&output);
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["command"], "help");
    assert_eq!(json["result"], "help");
    assert_eq!(json["data"]["command_path"], serde_json::json!(["spr"]));
    assert!(json["data"]["options"]
        .as_array()
        .unwrap()
        .iter()
        .any(|option| option["long"] == "json" && option["global"] == true));
    assert!(!json["data"]["rendered_text"].as_str().unwrap().is_empty());
}

#[test]
fn json_help_outputs_structured_nested_help() {
    for args in [
        ["--json", "help", "list", "commit"],
        ["list", "commit", "--help", "--json"],
    ] {
        let output = run_spr(&args);

        assert!(output.status.success(), "args failed: {args:?}");
        assert!(
            output.stderr.is_empty(),
            "stderr for {args:?} was not empty"
        );
        let json = stdout_json(&output);
        assert_eq!(json["command"], "help");
        assert_eq!(json["result"], "help");
        assert_eq!(
            json["data"]["command_path"],
            serde_json::json!(["spr", "list", "commit"])
        );
        assert!(!json["data"]["options"].as_array().unwrap().is_empty());
        assert!(!json["data"]["rendered_text"].as_str().unwrap().is_empty());
    }
}

#[test]
fn json_version_outputs_package_metadata() {
    for args in [["--json", "--version"], ["--version", "--json"]] {
        let output = run_spr(&args);

        assert!(output.status.success(), "args failed: {args:?}");
        assert!(
            output.stderr.is_empty(),
            "stderr for {args:?} was not empty"
        );
        let json = stdout_json(&output);
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["command"], "version");
        assert_eq!(json["result"], "version");
        assert_eq!(json["data"]["name"], "spr");
        assert_eq!(json["data"]["version"], env!("CARGO_PKG_VERSION"));
    }
}

#[test]
fn json_parse_error_outputs_error_object() {
    let output = run_spr(&["--json", "definitely-not-a-command"]);

    assert!(!output.status.success());
    assert!(output.stderr.is_empty());
    let json = stdout_json(&output);
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["command"], "cli");
    assert_eq!(json["result"], "error");
    assert_eq!(json["error_kind"], "invalid_arguments");
    assert!(json["message"]
        .as_str()
        .unwrap()
        .contains("definitely-not-a-command"));
}

#[test]
fn detached_update_human_warns_after_pushing_branch() {
    let (dir, repo) = init_detached_update_repo();

    let output = run_detached_update(&repo, dir.path(), false);

    assert!(output.status.success());
    assert_alpha_remote_exists(&repo);
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("Skipping stack metadata refresh because HEAD is detached"));
}

#[test]
fn detached_update_json_reports_warning_after_pushing_branch() {
    let (dir, repo) = init_detached_update_repo();

    let output = run_detached_update(&repo, dir.path(), true);

    assert!(output.status.success());
    assert_alpha_remote_exists(&repo);
    let json = stdout_json(&output);
    assert!(json["data"]["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|warning| warning
            .as_str()
            .unwrap()
            .contains("Skipping stack metadata refresh because HEAD is detached")));
}

#[test]
fn absorb_apply_json_recovers_same_context_stale_metadata() {
    let (_dir, repo) = init_stale_absorb_metadata_repo();

    let output = run_spr(&[
        "--cd",
        repo.to_str().unwrap(),
        "--base",
        "main",
        "--prefix",
        "dank-spr/",
        "absorb",
        "--from",
        "pr:alpha",
        "--json",
    ]);
    assert_absorb_completed(output, "stack");
    let stack_subjects = git(&repo, ["log", "--format=%s", "-5", "stack"].as_slice());
    assert!(stack_subjects.contains("feat: alpha branch tail"));
}

#[test]
fn absorb_query_json_recovers_same_context_stale_metadata() {
    let (_dir, repo) = init_stale_absorb_metadata_repo();

    let from_pr_branch = run_spr(&[
        "--cd",
        repo.to_str().unwrap(),
        "--base",
        "main",
        "--prefix",
        "dank-spr/",
        "absorb",
        "--from",
        "pr:alpha",
        "--query-changed-branches",
        "--json",
    ]);
    assert_absorb_changed_branches(
        from_pr_branch,
        &["stack", "dank-spr/alpha", "dank-spr/beta"],
    );

    git(&repo, ["checkout", "stack"].as_slice());
    let from_stack_branch = run_spr(&[
        "--cd",
        repo.to_str().unwrap(),
        "--base",
        "main",
        "--prefix",
        "dank-spr/",
        "absorb",
        "--from",
        "pr:alpha",
        "--query-changed-branches",
        "--json",
    ]);
    assert_absorb_changed_branches(
        from_stack_branch,
        &["stack", "dank-spr/alpha", "dank-spr/beta"],
    );
}

#[test]
fn absorb_query_json_rejects_stale_metadata_context_mismatch() {
    let (_dir, repo) = init_stale_absorb_metadata_repo();
    mutate_first_stack_record(&repo, |stack_record| {
        stack_record.insert(
            "prefix".to_string(),
            Value::String("other-spr/".to_string()),
        );
    });

    let output = run_spr(&[
        "--cd",
        repo.to_str().unwrap(),
        "--base",
        "main",
        "--prefix",
        "dank-spr/",
        "absorb",
        "--from",
        "pr:alpha",
        "--query-changed-branches",
        "--json",
    ]);

    assert_absorb_json_error_contains(
        output,
        "uses base `main` and prefix `other-spr/`, but absorb was invoked with base `main` and prefix `dank-spr/`",
    );
}

#[test]
fn absorb_query_json_rejects_ambiguous_stale_stack_branch_aliases() {
    let (_dir, repo) = init_stale_absorb_metadata_repo();
    git(&repo, ["branch", "stack-copy", "stack"].as_slice());
    mutate_first_stack_record(&repo, |stack_record| {
        stack_record.insert(
            "known_branches".to_string(),
            serde_json::json!(["stack", "stack-copy"]),
        );
    });

    let output = run_spr(&[
        "--cd",
        repo.to_str().unwrap(),
        "--base",
        "main",
        "--prefix",
        "dank-spr/",
        "absorb",
        "--from",
        "pr:alpha",
        "--query-changed-branches",
        "--json",
    ]);

    assert_absorb_json_error_contains(
        output,
        "multiple stale-compatible full-stack branch aliases for candidate selector prefix [pr:alpha]",
    );
}
