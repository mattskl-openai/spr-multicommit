//! Adjust the tail of a PR group while preserving ignore blocks.

use anyhow::{anyhow, bail, Result};
use std::collections::HashSet;
use tracing::info;

use crate::commands::common;
use crate::commands::common::CherryPickOp;
use crate::commands::rewrite_resume::{
    self, RewriteCommandKind, RewriteCommandOutcome, RewriteConflictPolicy, RewriteSession,
};
use crate::config::DirtyWorktreePolicy;
use crate::git::git_rev_parse;
use crate::git::git_ro;
use crate::parsing::derive_local_groups_with_ignored;
use crate::selectors::{resolve_group_ordinal, GroupSelector};

fn resolve_fix_pr_target(
    groups: &[crate::parsing::Group],
    target: &GroupSelector,
) -> Result<usize> {
    resolve_group_ordinal(groups, target)
}

fn build_fix_pr_operations(
    all_commits: &[String],
    top_commits: &[String],
    insert_pos: usize,
) -> Vec<CherryPickOp> {
    let mut operations = Vec::new();
    operations.extend(CherryPickOp::from_commits(&all_commits[..=insert_pos]));
    operations.extend(CherryPickOp::from_commits(top_commits));
    if insert_pos + 1 < all_commits.len() {
        operations.extend(CherryPickOp::from_commits(&all_commits[insert_pos + 1..]));
    }
    operations
}

/// Move the last `tail_count` commits (top-of-stack) to become the tail of PR `n` (1-based, bottom→top).
///
/// Ignore blocks are treated as part of the preceding group and are never moved.
/// If the selected tail commits intersect an ignore block, the operation aborts.
///
/// # Errors
///
/// Returns errors when the target index is out of range, when the tail contains
/// `pr:<tag>` markers, when the tail intersects ignored commits, or when Git
/// operations (worktree creation, cherry-picks, reset) fail.
pub fn fix_pr_tail(
    base: &str,
    ignore_tag: &str,
    target: &GroupSelector,
    tail_count: usize,
    safe: bool,
    dry: bool,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<RewriteCommandOutcome> {
    if tail_count == 0 {
        return Ok(RewriteCommandOutcome::Completed);
    }

    let (merge_base, leading_ignored, groups) = derive_local_groups_with_ignored(base, ignore_tag)?;
    let total_groups = groups.len();
    if total_groups == 0 {
        info!("No local PR groups found; nothing to fix.");
        return Ok(RewriteCommandOutcome::Completed);
    }

    let target_n = resolve_fix_pr_target(&groups, target)?;

    // Flatten commits bottom→top
    let mut all_commits: Vec<String> = Vec::new();
    all_commits.extend(leading_ignored.iter().cloned());
    for g in &groups {
        all_commits.extend(g.commits.iter().cloned());
        all_commits.extend(g.ignored_after.iter().cloned());
    }
    if all_commits.is_empty() {
        info!("No commits found; nothing to fix.");
        return Ok(RewriteCommandOutcome::Completed);
    }

    // Determine top M commits (trim if M > total)
    let m = tail_count.min(all_commits.len());
    let top_commits: Vec<String> = all_commits.split_off(all_commits.len() - m);

    // Validate: moved commits must NOT contain pr:<tag> markers
    let mut offenders: Vec<String> = vec![];
    for sha in &top_commits {
        let msg = git_ro(["log", "-n", "1", "--format=%B", sha].as_slice())?;
        if crate::pr_labels::candidate_marker_regex().is_match(&msg) {
            offenders.push(sha.clone());
        }
    }
    if !offenders.is_empty() {
        bail!(
            "Selected tail commit(s) contain pr:<tag> markers; cannot move commits that start or belong to PR groups: {}",
            offenders
                .iter()
                .map(|s| s.chars().take(8).collect::<String>())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Disallow moving commits from ignore blocks; they must stay attached to their PR.
    let mut ignored_set: HashSet<String> = HashSet::new();
    ignored_set.extend(leading_ignored.iter().cloned());
    for g in &groups {
        ignored_set.extend(g.ignored_after.iter().cloned());
    }
    let mut ignored_moved: Vec<String> = top_commits
        .iter()
        .filter(|sha| ignored_set.contains(*sha))
        .cloned()
        .collect();
    if !ignored_moved.is_empty() {
        ignored_moved.sort();
        ignored_moved.dedup();
        bail!(
            "Selected tail commit(s) are in an ignored block; adjust --tail to avoid moving ignored commits: {}",
            ignored_moved
                .iter()
                .map(|s| s.chars().take(8).collect::<String>())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Determine insertion index after last commit of PR N (including its ignore block) within the remainder
    let target = groups
        .get(target_n - 1)
        .ok_or_else(|| anyhow!("PR {} has no commits", target_n))?;
    let last_of_n = if let Some(last_ignored) = target.ignored_after.last() {
        last_ignored.clone()
    } else {
        target
            .commits
            .last()
            .ok_or_else(|| anyhow!("PR {} has no commits", target_n))?
            .clone()
    };
    let insert_pos = all_commits
        .iter()
        .position(|sha| sha == &last_of_n)
        .ok_or_else(|| anyhow!("Could not locate last commit of PR {} in stream", target_n))?;

    common::with_dirty_worktree_policy(dry, "spr fix-pr", dirty_worktree_policy, || {
        let (cur_branch, short) = common::get_current_branch_and_short()?;
        let original_head = git_rev_parse("HEAD")?;
        let original_worktree_root = rewrite_resume::current_repo_root()?;
        let backup_tag = if safe {
            Some(common::create_backup_tag(
                dry,
                "fix-pr",
                &cur_branch,
                &short,
            )?)
        } else {
            None
        };

        let (tmp_path, tmp_branch) = common::create_temp_worktree(dry, "fix", &merge_base, &short)?;
        let steps = rewrite_resume::build_replay_steps(&build_fix_pr_operations(
            &all_commits,
            &top_commits,
            insert_pos,
        ))?;
        rewrite_resume::run_rewrite_session(
            dry,
            RewriteSession {
                command_kind: RewriteCommandKind::FixPr,
                conflict_policy: RewriteConflictPolicy::Suspend,
                original_worktree_root,
                original_branch: cur_branch,
                original_head,
                temp_branch: tmp_branch,
                temp_worktree_path: tmp_path,
                backup_tag,
                steps,
                post_success_hint: Some(
                    "No GitHub changes were made. Run `spr update` after inspecting the rewritten stack."
                        .to_string(),
                ),
            },
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{fix_pr_tail, resolve_fix_pr_target};
    use crate::commands::rewrite_resume::{resume_rewrite, RewriteResumeState};
    use crate::commands::RewriteCommandOutcome;
    use crate::config::DirtyWorktreePolicy;
    use crate::parsing::Group;
    use crate::selectors::{GroupSelector, StableHandle};
    use crate::test_support::{lock_cwd, DirGuard};
    use std::fs;
    use std::path::Path;
    use std::process::Command;
    use tempfile::TempDir;

    fn groups(tags: &[&str]) -> Vec<Group> {
        tags.iter()
            .map(|tag| Group {
                tag: tag.to_string(),
                subjects: vec![format!("feat: {tag}")],
                commits: vec![format!("{tag}1")],
                first_message: Some(format!("feat: {tag} pr:{tag}")),
                ignored_after: Vec::new(),
            })
            .collect()
    }

    #[test]
    fn fix_pr_resolves_stable_handle_to_current_local_ordinal() {
        let groups = groups(&["alpha", "beta", "gamma"]);
        let target = GroupSelector::Stable(StableHandle {
            tag: "beta".to_string(),
        });

        assert_eq!(resolve_fix_pr_target(&groups, &target).unwrap(), 2);
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .current_dir(repo)
            .args(args)
            .output()
            .expect("spawn git");
        assert!(
            out.status.success(),
            "git {:?} failed\nstdout:\n{}\nstderr:\n{}",
            args,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).to_string()
    }

    fn init_fix_pr_repo() -> TempDir {
        let dir = tempfile::tempdir().expect("create temp dir");
        let repo = dir.path();
        git(repo, ["init", "-b", "main"].as_slice());
        git(repo, ["config", "user.email", "spr@example.com"].as_slice());
        git(repo, ["config", "user.name", "SPR Tests"].as_slice());
        fs::write(repo.join("consumer.txt"), "consumer base\n").expect("write consumer");
        git(repo, ["add", "."].as_slice());
        git(repo, ["commit", "-m", "init"].as_slice());
        git(repo, ["checkout", "-b", "stack"].as_slice());

        fs::write(repo.join("alpha.txt"), "alpha 1\n").expect("write alpha seed");
        git(repo, ["add", "alpha.txt"].as_slice());
        git(repo, ["commit", "-m", "feat: alpha pr:alpha"].as_slice());

        fs::write(repo.join("alpha.txt"), "alpha 1\nalpha 2\n").expect("write alpha follow-up");
        git(repo, ["add", "alpha.txt"].as_slice());
        git(repo, ["commit", "-m", "feat: alpha follow-up"].as_slice());

        fs::write(repo.join("beta.txt"), "beta 1\n").expect("write beta seed");
        git(repo, ["add", "beta.txt"].as_slice());
        git(repo, ["commit", "-m", "feat: beta pr:beta"].as_slice());

        fs::write(repo.join("beta.txt"), "beta 1\nbeta 2\n").expect("write beta follow-up");
        git(repo, ["add", "beta.txt"].as_slice());
        git(repo, ["commit", "-m", "feat: beta follow-up"].as_slice());

        fs::write(repo.join("review.txt"), "review fix\n").expect("write review");
        git(repo, ["add", "review.txt"].as_slice());
        git(repo, ["commit", "-m", "fix review comment"].as_slice());

        dir
    }

    fn init_fix_pr_conflict_repo() -> TempDir {
        let dir = tempfile::tempdir().expect("create temp dir");
        let repo = dir.path();
        git(repo, ["init", "-b", "main"].as_slice());
        git(repo, ["config", "user.email", "spr@example.com"].as_slice());
        git(repo, ["config", "user.name", "SPR Tests"].as_slice());
        fs::write(repo.join("story.txt"), "base\n").expect("write base");
        git(repo, ["add", "story.txt"].as_slice());
        git(repo, ["commit", "-m", "init"].as_slice());
        git(repo, ["checkout", "-b", "stack"].as_slice());
        fs::write(repo.join("story.txt"), "alpha-1\n").expect("write alpha");
        git(repo, ["add", "story.txt"].as_slice());
        git(repo, ["commit", "-m", "feat: alpha pr:alpha"].as_slice());
        fs::write(repo.join("story.txt"), "beta-1\n").expect("write beta");
        git(repo, ["add", "story.txt"].as_slice());
        git(repo, ["commit", "-m", "feat: beta pr:beta"].as_slice());
        fs::write(repo.join("story.txt"), "review-fix\n").expect("write review");
        git(repo, ["add", "story.txt"].as_slice());
        git(repo, ["commit", "-m", "fix review comment"].as_slice());
        dir
    }

    fn dirty_worktree(repo: &Path) {
        fs::write(repo.join("consumer.txt"), "consumer dirty\n").expect("dirty tracked file");
        fs::write(repo.join("scratch.txt"), "scratch local\n").expect("write untracked file");
    }

    fn log_subjects(repo: &Path) -> Vec<String> {
        git(repo, ["log", "--format=%s", "-5"].as_slice())
            .lines()
            .map(|line| line.to_string())
            .collect()
    }

    fn expected_rewritten_subjects() -> Vec<String> {
        vec![
            "feat: beta follow-up".to_string(),
            "feat: beta pr:beta".to_string(),
            "fix review comment".to_string(),
            "feat: alpha follow-up".to_string(),
            "feat: alpha pr:alpha".to_string(),
        ]
    }

    #[test]
    fn fix_pr_discard_policy_preserves_current_tracked_reset_behavior() {
        let _lock = lock_cwd();
        let dir = init_fix_pr_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let original_head = git(&repo, ["rev-parse", "HEAD"].as_slice());
        dirty_worktree(&repo);

        fix_pr_tail(
            "main",
            "ignore",
            &GroupSelector::LocalPr(1),
            1,
            false,
            false,
            DirtyWorktreePolicy::Discard,
        )
        .expect("fix-pr should rewrite under discard policy");

        let rewritten_head = git(&repo, ["rev-parse", "HEAD"].as_slice());
        assert_ne!(
            original_head.trim(),
            rewritten_head.trim(),
            "HEAD should move"
        );
        assert_eq!(
            fs::read_to_string(repo.join("consumer.txt")).expect("read consumer"),
            "consumer base\n",
            "discard policy should preserve the current tracked reset behavior"
        );
        assert_eq!(
            fs::read_to_string(repo.join("scratch.txt")).expect("read scratch"),
            "scratch local\n",
            "discard policy should leave untracked files in place"
        );
        assert_eq!(log_subjects(&repo), expected_rewritten_subjects());
    }

    #[test]
    fn fix_pr_stash_policy_restores_tracked_and_untracked_changes() {
        let _lock = lock_cwd();
        let dir = init_fix_pr_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let original_head = git(&repo, ["rev-parse", "HEAD"].as_slice());
        dirty_worktree(&repo);

        fix_pr_tail(
            "main",
            "ignore",
            &GroupSelector::LocalPr(1),
            1,
            false,
            false,
            DirtyWorktreePolicy::Stash,
        )
        .expect("fix-pr should rewrite and restore stashed changes");

        let rewritten_head = git(&repo, ["rev-parse", "HEAD"].as_slice());
        assert_ne!(
            original_head.trim(),
            rewritten_head.trim(),
            "HEAD should move"
        );
        assert_eq!(
            fs::read_to_string(repo.join("consumer.txt")).expect("read consumer"),
            "consumer dirty\n",
            "stash policy should restore tracked changes"
        );
        assert_eq!(
            fs::read_to_string(repo.join("scratch.txt")).expect("read scratch"),
            "scratch local\n",
            "stash policy should restore untracked files"
        );
        assert!(
            git(&repo, ["stash", "list"].as_slice()).trim().is_empty(),
            "stash policy should drop the temporary stash after a successful restore"
        );
        assert_eq!(log_subjects(&repo), expected_rewritten_subjects());
    }

    #[test]
    fn fix_pr_halt_policy_refuses_to_rewrite_dirty_worktree() {
        let _lock = lock_cwd();
        let dir = init_fix_pr_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let original_head = git(&repo, ["rev-parse", "HEAD"].as_slice());
        dirty_worktree(&repo);

        let err = fix_pr_tail(
            "main",
            "ignore",
            &GroupSelector::LocalPr(1),
            1,
            false,
            false,
            DirtyWorktreePolicy::Halt,
        )
        .expect_err("halt policy should refuse a dirty worktree");
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("dirty_worktree=halt"),
            "error should name the blocking policy: {err_text}"
        );

        let current_head = git(&repo, ["rev-parse", "HEAD"].as_slice());
        assert_eq!(
            original_head.trim(),
            current_head.trim(),
            "HEAD should not move"
        );
        assert_eq!(
            fs::read_to_string(repo.join("consumer.txt")).expect("read consumer"),
            "consumer dirty\n",
            "halt policy should leave tracked changes untouched"
        );
        assert_eq!(
            fs::read_to_string(repo.join("scratch.txt")).expect("read scratch"),
            "scratch local\n",
            "halt policy should leave untracked files untouched"
        );
    }

    #[test]
    fn fix_pr_suspends_and_resumes_conflict() {
        let _lock = lock_cwd();
        let dir = init_fix_pr_conflict_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let outcome = fix_pr_tail(
            "main",
            "ignore",
            &GroupSelector::LocalPr(1),
            1,
            false,
            false,
            DirtyWorktreePolicy::Halt,
        )
        .expect("fix-pr should suspend");
        let mut current = outcome;
        let mut last_resume_path = None;
        while let RewriteCommandOutcome::Suspended { resume_path } = current {
            let resume_state: RewriteResumeState =
                serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                    .expect("parse resume state");
            let resolved_contents = if resume_state.suspended_step_index == 1 {
                "review-fix\n"
            } else {
                "review-fix\nbeta-1\n"
            };
            fs::write(
                Path::new(&resume_state.temp_worktree_path).join("story.txt"),
                resolved_contents,
            )
            .expect("resolve fix-pr conflict");
            git(
                Path::new(&resume_state.temp_worktree_path),
                ["add", "story.txt"].as_slice(),
            );
            last_resume_path = Some(resume_path.clone());
            current = resume_rewrite(false, &resume_path).expect("resume fix-pr");
        }

        assert_eq!(current, RewriteCommandOutcome::Completed);
        if let Some(resume_path) = last_resume_path {
            assert!(
                !resume_path.exists(),
                "successful fix-pr resume should delete the resume file"
            );
        }
        assert_eq!(
            fs::read_to_string(repo.join("story.txt")).expect("read final file"),
            "review-fix\nbeta-1\n"
        );
        assert_eq!(
            log_subjects(&repo),
            vec![
                "feat: beta pr:beta".to_string(),
                "fix review comment".to_string(),
                "feat: alpha pr:alpha".to_string(),
                "init".to_string(),
            ]
        );
    }
}
