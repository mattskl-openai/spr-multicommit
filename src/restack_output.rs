//! Output contract for read-only restack previews.

use serde::Serialize;

use crate::json_output::{JsonCommand, JSON_OUTPUT_SCHEMA_VERSION};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RestackPreviewResult {
    Preview,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RestackPreviewOutput {
    pub schema_version: u32,
    pub command: JsonCommand,
    pub result: RestackPreviewResult,
    pub data: RestackPreviewData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RestackPreviewGroup {
    pub stable_handle: String,
    pub commit_count: usize,
    pub ignored_after_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RestackExecutorPlan {
    Noop,
    ResetToBase,
    NativeRebase {
        upstream_exclusive: String,
        commit_count: usize,
    },
    TempWorktreeCherryPick {
        operation_count: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RestackPreviewData {
    pub base_ref: String,
    pub base_sha: Option<String>,
    pub base_ref_was_refreshed: bool,
    pub current_branch: String,
    pub original_head: String,
    pub after_selector: String,
    pub resolved_after_count: usize,
    pub dropped_groups: Vec<RestackPreviewGroup>,
    pub remaining_groups: Vec<RestackPreviewGroup>,
    pub kept_ignored_segment_count: usize,
    pub planned_cherry_pick_operation_count: usize,
    pub planned_executor: RestackExecutorPlan,
    pub would_fetch_origin_when_executed: bool,
    pub would_create_backup_tag: bool,
    pub would_create_temp_worktree: bool,
    pub would_reset_current_branch: bool,
    pub would_refresh_stack_metadata: bool,
    pub not_validated: Vec<String>,
}

pub fn preview(data: RestackPreviewData) -> RestackPreviewOutput {
    RestackPreviewOutput {
        schema_version: JSON_OUTPUT_SCHEMA_VERSION,
        command: JsonCommand::Restack,
        result: RestackPreviewResult::Preview,
        data,
    }
}

impl RestackPreviewOutput {
    pub fn exit_code(&self) -> i32 {
        crate::json_output::EXIT_SUCCESS
    }

    pub fn render_human(&self) -> String {
        render_human_preview("Restack preview", &self.data)
    }
}

fn render_groups(groups: &[RestackPreviewGroup]) -> String {
    if groups.is_empty() {
        "<none>".to_string()
    } else {
        groups
            .iter()
            .map(|group| {
                if group.ignored_after_count == 0 {
                    format!("{} ({} commit(s))", group.stable_handle, group.commit_count)
                } else {
                    format!(
                        "{} ({} commit(s), {} ignored after)",
                        group.stable_handle, group.commit_count, group.ignored_after_count
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

fn render_bool_list(items: &[&str]) -> String {
    items.join("; ")
}

fn render_planned_executor(executor: &RestackExecutorPlan) -> String {
    match executor {
        RestackExecutorPlan::Noop => "no-op".to_string(),
        RestackExecutorPlan::ResetToBase => "reset current branch to target base".to_string(),
        RestackExecutorPlan::NativeRebase {
            upstream_exclusive,
            commit_count,
        } => format!(
            "native git rebase of {commit_count} commit(s) after {upstream_exclusive}; temp-worktree replay if native rebase conflicts"
        ),
        RestackExecutorPlan::TempWorktreeCherryPick { operation_count } => {
            format!("temp-worktree cherry-pick replay ({operation_count} operation(s))")
        }
    }
}

pub fn render_human_preview(title: &str, data: &RestackPreviewData) -> String {
    let is_preview = title.to_ascii_lowercase().contains("preview");
    let base_sha = data.base_sha.as_deref().unwrap_or("<unresolved>");
    let base_note = if data.base_ref_was_refreshed {
        "refreshed before planning"
    } else {
        "local ref; preview did not fetch"
    };
    let execution_label = if is_preview {
        "execution would"
    } else {
        "execution plan"
    };
    let not_validated_label = if is_preview {
        "not validated by preview"
    } else {
        "not validated yet"
    };
    let would_do = render_bool_list(&[
        if data.would_fetch_origin_when_executed {
            "fetch origin"
        } else {
            "use refreshed base ref"
        },
        if data.would_create_backup_tag {
            "create backup tag"
        } else {
            "skip backup tag"
        },
        if data.would_create_temp_worktree {
            "create temp restack worktree"
        } else {
            "skip temp restack worktree on the planned executor"
        },
        if data.would_reset_current_branch {
            "move current branch"
        } else {
            "leave current branch tip unchanged"
        },
        if data.would_refresh_stack_metadata {
            "refresh stack metadata"
        } else {
            "leave stack metadata untouched"
        },
    ]);

    format!(
        "{title}:\n  current branch: {} @ {}\n  target base: {} @ {} ({})\n  selector: --after {} -> keeps first {} PR group(s)\n  drop from local stack: {}\n  replay onto target base: {}\n  preserve ignored blocks before replay: {}\n  planned cherry-pick operations: {}\n  planned executor: {}\n  {execution_label}: {}\n  {not_validated_label}: {}",
        data.current_branch,
        data.original_head,
        data.base_ref,
        base_sha,
        base_note,
        data.after_selector,
        data.resolved_after_count,
        render_groups(&data.dropped_groups),
        render_groups(&data.remaining_groups),
        data.kept_ignored_segment_count,
        data.planned_cherry_pick_operation_count,
        render_planned_executor(&data.planned_executor),
        would_do,
        data.not_validated.join("; ")
    )
}

#[cfg(test)]
mod tests {
    use super::{
        preview, render_human_preview, RestackExecutorPlan, RestackPreviewData,
        RestackPreviewGroup, RestackPreviewResult,
    };

    fn sample_preview_data() -> RestackPreviewData {
        RestackPreviewData {
            base_ref: "origin/main".to_string(),
            base_sha: Some("base123".to_string()),
            base_ref_was_refreshed: false,
            current_branch: "stack".to_string(),
            original_head: "head123".to_string(),
            after_selector: "pr:alpha".to_string(),
            resolved_after_count: 1,
            dropped_groups: vec![RestackPreviewGroup {
                stable_handle: "pr:alpha".to_string(),
                commit_count: 2,
                ignored_after_count: 0,
            }],
            remaining_groups: vec![RestackPreviewGroup {
                stable_handle: "pr:beta".to_string(),
                commit_count: 1,
                ignored_after_count: 1,
            }],
            kept_ignored_segment_count: 0,
            planned_cherry_pick_operation_count: 2,
            planned_executor: RestackExecutorPlan::TempWorktreeCherryPick { operation_count: 2 },
            would_fetch_origin_when_executed: true,
            would_create_backup_tag: true,
            would_create_temp_worktree: true,
            would_reset_current_branch: true,
            would_refresh_stack_metadata: true,
            not_validated: vec![
                "remote freshness".to_string(),
                "cherry-pick conflicts".to_string(),
            ],
        }
    }

    #[test]
    fn restack_preview_json_uses_preview_envelope() {
        let output = preview(sample_preview_data());
        let json = serde_json::to_value(&output).unwrap();

        assert_eq!(output.result, RestackPreviewResult::Preview);
        assert_eq!(json["command"], "restack");
        assert_eq!(json["result"], "preview");
        assert_eq!(
            json["data"]["dropped_groups"][0]["stable_handle"],
            "pr:alpha"
        );
        assert_eq!(
            json["data"]["remaining_groups"][0]["stable_handle"],
            "pr:beta"
        );
        assert_eq!(
            json["data"]["planned_executor"]["kind"],
            "temp_worktree_cherry_pick"
        );
        assert_eq!(json["data"]["planned_executor"]["operation_count"], 2);
        assert_eq!(json["data"]["would_create_backup_tag"], true);
    }

    #[test]
    fn restack_preview_human_renderer_names_plan_and_warnings() {
        let rendered = render_human_preview("Restack preview", &sample_preview_data());

        assert!(rendered.contains("Restack preview:"));
        assert!(rendered.contains("target base: origin/main @ base123"));
        assert!(rendered.contains("drop from local stack: pr:alpha (2 commit(s))"));
        assert!(
            rendered.contains("replay onto target base: pr:beta (1 commit(s), 1 ignored after)")
        );
        assert!(rendered.contains("planned cherry-pick operations: 2"));
        assert!(rendered.contains("planned executor: temp-worktree cherry-pick replay"));
        assert!(
            rendered.contains("not validated by preview: remote freshness; cherry-pick conflicts")
        );
    }

    #[test]
    fn restack_preview_human_renderer_names_native_rebase_executor() {
        let data = RestackPreviewData {
            planned_executor: RestackExecutorPlan::NativeRebase {
                upstream_exclusive: "alpha123".to_string(),
                commit_count: 3,
            },
            would_create_temp_worktree: false,
            ..sample_preview_data()
        };

        let rendered = render_human_preview("Restack preview", &data);

        assert!(
            rendered.contains("planned executor: native git rebase of 3 commit(s) after alpha123")
        );
        assert!(rendered.contains("skip temp restack worktree on the planned executor"));
        assert!(!rendered.contains("create temp restack worktree"));
    }

    #[test]
    fn restack_execution_human_renderer_uses_already_refreshed_language() {
        let data = RestackPreviewData {
            base_ref_was_refreshed: true,
            would_fetch_origin_when_executed: false,
            not_validated: vec!["cherry-pick conflicts".to_string(), "tests".to_string()],
            ..sample_preview_data()
        };

        let rendered = render_human_preview("Restack plan", &data);

        assert!(rendered.contains("target base: origin/main @ base123 (refreshed before planning)"));
        assert!(rendered.contains("execution plan: use refreshed base ref"));
        assert!(rendered.contains("not validated yet: cherry-pick conflicts; tests"));
        assert!(!rendered.contains("not validated by preview"));
    }
}
