//! Output contract for read-only adopt-prefix previews.

use serde::Serialize;

use crate::json_output::{JsonCommand, JSON_OUTPUT_SCHEMA_VERSION};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdoptPrefixPreviewResult {
    Preview,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdoptPrefixPreviewOutput {
    pub schema_version: u32,
    pub command: JsonCommand,
    pub result: AdoptPrefixPreviewResult,
    pub data: AdoptPrefixPreviewData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdoptPrefixPreviewGroup {
    pub stable_handle: String,
    pub commit_count: usize,
    pub ignored_after_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdoptPrefixPreviewData {
    pub candidate_head: String,
    pub candidate_groups: Vec<AdoptPrefixPreviewGroup>,
    pub owning_stack_id: String,
    pub owning_stack_branch: String,
    pub old_stack_head: String,
    pub merge_base: String,
    pub replaced_old_boundary: String,
    pub replay_suffix_groups: Vec<AdoptPrefixPreviewGroup>,
    pub replay_operation_count: usize,
    pub publishable_before: Vec<String>,
    pub publishable_after: Vec<String>,
    pub would_create_backup_tag: bool,
    pub would_create_temp_worktree: bool,
    pub would_move_stack_branch: bool,
    pub would_refresh_stack_metadata: bool,
    pub not_validated: Vec<String>,
}

pub fn preview(data: AdoptPrefixPreviewData) -> AdoptPrefixPreviewOutput {
    AdoptPrefixPreviewOutput {
        schema_version: JSON_OUTPUT_SCHEMA_VERSION,
        command: JsonCommand::AdoptPrefix,
        result: AdoptPrefixPreviewResult::Preview,
        data,
    }
}

impl AdoptPrefixPreviewOutput {
    pub fn exit_code(&self) -> i32 {
        crate::json_output::EXIT_SUCCESS
    }

    pub fn render_human(&self) -> String {
        render_human_preview(&self.data)
    }
}

fn render_groups(groups: &[AdoptPrefixPreviewGroup]) -> String {
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

fn render_selectors(selectors: &[String]) -> String {
    if selectors.is_empty() {
        "<none>".to_string()
    } else {
        selectors.join("; ")
    }
}

pub fn render_human_preview(data: &AdoptPrefixPreviewData) -> String {
    format!(
        "Adopt-prefix preview:\n  candidate HEAD: {}\n  candidate prefix: {}\n  owning stack: {} on {} @ {}\n  shared merge base: {}\n  replace old raw boundary through: {}\n  replay suffix: {}\n  planned cherry-pick operations: {}\n  publishable before: {}\n  publishable after: {}\n  execution would: {}; {}; {}; {}\n  not validated by preview: {}",
        data.candidate_head,
        render_groups(&data.candidate_groups),
        data.owning_stack_id,
        data.owning_stack_branch,
        data.old_stack_head,
        data.merge_base,
        data.replaced_old_boundary,
        render_groups(&data.replay_suffix_groups),
        data.replay_operation_count,
        render_selectors(&data.publishable_before),
        render_selectors(&data.publishable_after),
        if data.would_create_backup_tag {
            "create backup tag"
        } else {
            "skip backup tag"
        },
        if data.would_create_temp_worktree {
            "create temp adopt-prefix worktree"
        } else {
            "skip temp adopt-prefix worktree"
        },
        if data.would_move_stack_branch {
            "move owning stack branch"
        } else {
            "leave owning stack branch unchanged"
        },
        if data.would_refresh_stack_metadata {
            "refresh stack metadata"
        } else {
            "leave stack metadata untouched"
        },
        data.not_validated.join("; ")
    )
}

#[cfg(test)]
mod tests {
    use super::{
        preview, render_human_preview, AdoptPrefixPreviewData, AdoptPrefixPreviewGroup,
        AdoptPrefixPreviewResult,
    };

    fn sample_preview_data() -> AdoptPrefixPreviewData {
        AdoptPrefixPreviewData {
            candidate_head: "candidate123".to_string(),
            candidate_groups: vec![AdoptPrefixPreviewGroup {
                stable_handle: "pr:alpha".to_string(),
                commit_count: 2,
                ignored_after_count: 0,
            }],
            owning_stack_id: "stack-1".to_string(),
            owning_stack_branch: "stack".to_string(),
            old_stack_head: "stack123".to_string(),
            merge_base: "base123".to_string(),
            replaced_old_boundary: "alpha-old".to_string(),
            replay_suffix_groups: vec![AdoptPrefixPreviewGroup {
                stable_handle: "pr:beta".to_string(),
                commit_count: 1,
                ignored_after_count: 1,
            }],
            replay_operation_count: 2,
            publishable_before: vec!["pr:alpha".to_string(), "pr:beta".to_string()],
            publishable_after: vec!["pr:alpha".to_string(), "pr:beta".to_string()],
            would_create_backup_tag: true,
            would_create_temp_worktree: true,
            would_move_stack_branch: true,
            would_refresh_stack_metadata: true,
            not_validated: vec!["cherry-pick conflicts".to_string()],
        }
    }

    #[test]
    fn adopt_prefix_preview_json_uses_preview_envelope() {
        let output = preview(sample_preview_data());
        let json = serde_json::to_value(&output).unwrap();

        assert_eq!(output.result, AdoptPrefixPreviewResult::Preview);
        assert_eq!(json["command"], "adopt-prefix");
        assert_eq!(json["result"], "preview");
        assert_eq!(
            json["data"]["candidate_groups"][0]["stable_handle"],
            "pr:alpha"
        );
        assert_eq!(
            json["data"]["replay_suffix_groups"][0]["stable_handle"],
            "pr:beta"
        );
    }

    #[test]
    fn adopt_prefix_preview_human_renderer_names_plan_and_guards() {
        let rendered = render_human_preview(&sample_preview_data());

        assert!(rendered.contains("Adopt-prefix preview:"));
        assert!(rendered.contains("candidate prefix: pr:alpha (2 commit(s))"));
        assert!(rendered.contains("owning stack: stack-1 on stack @ stack123"));
        assert!(rendered.contains("publishable before: pr:alpha; pr:beta"));
        assert!(rendered.contains("not validated by preview: cherry-pick conflicts"));
    }
}
