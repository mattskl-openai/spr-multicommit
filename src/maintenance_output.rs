use serde::Serialize;

use crate::config::{LocalPrBranchSyncPolicy, PrDescriptionMode};
use crate::json_output::JsonCommand;
use crate::summary_output::SummaryOutput;
use crate::update_output::UpdateSummaryData;

pub type MaintenanceOutput = SummaryOutput<MaintenancePayload>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MaintenancePayload {
    Prep {
        #[serde(flatten)]
        data: Box<PrepSummaryData>,
    },
    RelinkPrs {
        #[serde(flatten)]
        data: Box<RelinkPrsSummaryData>,
    },
    Cleanup {
        #[serde(flatten)]
        data: Box<CleanupSummaryData>,
    },
    LocalPrBranchSync {
        #[serde(flatten)]
        data: Box<LocalPrBranchSyncSummaryData>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrepSummaryData {
    pub repo: PrepRepoContext,
    pub options: PrepOptions,
    pub selection: ResolvedPrepSelection,
    pub selected_groups: Vec<PreparedGroupData>,
    pub rewritten_head_sha: Option<String>,
    pub replayed_commit_count: usize,
    pub skipped_replay_commit_count: usize,
    pub next_child: Option<PrepNextChildData>,
    pub update: Option<UpdateSummaryData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrepRepoContext {
    pub base: String,
    pub prefix: String,
    pub ignore_tag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrepOptions {
    pub dry_run: bool,
    pub pr_description_mode: PrDescriptionMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResolvedPrepSelection {
    All,
    Until {
        selector: String,
        last_local_pr_number: usize,
    },
    Exact {
        selector: String,
        local_pr_number: usize,
    },
    From {
        selector: String,
        first_local_pr_number: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparedGroupAction {
    Squashed,
    PreservedSingleCommit,
    SkippedEmpty,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PreparedGroupData {
    pub local_pr_number: usize,
    pub stable_handle: String,
    pub source_commit_count: usize,
    pub action: PreparedGroupAction,
    pub target_sha: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrepNextChildAction {
    WouldAppendWarning,
    WarningAppended,
    SkippedStackOnly,
    MissingOpenPr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrepNextChildData {
    pub local_pr_number: usize,
    pub stable_handle: String,
    pub head_branch: String,
    pub remote_pr_number: Option<u64>,
    pub action: PrepNextChildAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RelinkPrsSummaryData {
    pub repo: MaintenanceRepoContext,
    pub options: MaintenanceOptions,
    pub expected_chain: Vec<RelinkExpectedBaseData>,
    pub decisions: Vec<RelinkPrDecisionData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MaintenanceRepoContext {
    pub base: String,
    pub prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MaintenanceOptions {
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RelinkExpectedBaseData {
    pub local_pr_number: usize,
    pub stable_handle: String,
    pub head_branch: String,
    pub expected_base_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelinkPrAction {
    AlreadyCorrect,
    Edited,
    DryRunEdit,
    MissingOpenPr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RelinkPrDecisionData {
    pub local_pr_number: usize,
    pub stable_handle: String,
    pub head_branch: String,
    pub expected_base_ref: String,
    pub current_base_ref: Option<String>,
    pub remote_pr_number: Option<u64>,
    pub action: RelinkPrAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CleanupSummaryData {
    pub repo: CleanupRepoContext,
    pub options: MaintenanceOptions,
    pub remote_candidates: Vec<String>,
    pub open_pr_heads: Vec<String>,
    pub decisions: Vec<CleanupDecisionData>,
    pub delete_batch: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalPrBranchSyncSummaryData {
    pub repo: LocalPrBranchSyncRepoContext,
    pub policy: LocalPrBranchSyncPolicy,
    pub local_pr_branch_actions: Vec<crate::local_pr_branches::LocalPrBranchAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalPrBranchSyncRepoContext {
    pub base: String,
    pub prefix: String,
    pub ignore_tag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CleanupRepoContext {
    pub prefix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupAction {
    Delete,
    DryRunDelete,
    SkipOpenPr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CleanupDecisionData {
    pub branch: String,
    pub action: CleanupAction,
}

pub fn prep_summary(data: PrepSummaryData) -> MaintenanceOutput {
    SummaryOutput::new(
        JsonCommand::Prep,
        MaintenancePayload::Prep {
            data: Box::new(data),
        },
    )
}

pub fn relink_prs_summary(data: RelinkPrsSummaryData) -> MaintenanceOutput {
    SummaryOutput::new(
        JsonCommand::RelinkPrs,
        MaintenancePayload::RelinkPrs {
            data: Box::new(data),
        },
    )
}

pub fn cleanup_summary(data: CleanupSummaryData) -> MaintenanceOutput {
    SummaryOutput::new(
        JsonCommand::Cleanup,
        MaintenancePayload::Cleanup {
            data: Box::new(data),
        },
    )
}

pub fn local_pr_branch_sync_summary(data: LocalPrBranchSyncSummaryData) -> MaintenanceOutput {
    SummaryOutput::new(
        JsonCommand::SyncLocalBranches,
        MaintenancePayload::LocalPrBranchSync {
            data: Box::new(data),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{
        cleanup_summary, prep_summary, relink_prs_summary, CleanupAction, CleanupDecisionData,
        CleanupRepoContext, CleanupSummaryData, MaintenanceOptions, MaintenancePayload,
        PrepOptions, PrepRepoContext, PrepSummaryData, PreparedGroupAction, PreparedGroupData,
        RelinkExpectedBaseData, RelinkPrAction, RelinkPrDecisionData, RelinkPrsSummaryData,
        ResolvedPrepSelection,
    };
    use crate::config::PrDescriptionMode;
    use crate::json_output::JsonCommand;
    use crate::summary_output::SummaryResult;

    #[test]
    fn prep_summary_uses_summary_envelope() {
        let output = prep_summary(PrepSummaryData {
            repo: PrepRepoContext {
                base: "main".to_string(),
                prefix: "dank-spr/".to_string(),
                ignore_tag: "ignore".to_string(),
            },
            options: PrepOptions {
                dry_run: true,
                pr_description_mode: PrDescriptionMode::Overwrite,
            },
            selection: ResolvedPrepSelection::All,
            selected_groups: vec![PreparedGroupData {
                local_pr_number: 1,
                stable_handle: "pr:alpha".to_string(),
                source_commit_count: 2,
                action: PreparedGroupAction::Squashed,
                target_sha: Some("abc123".to_string()),
            }],
            rewritten_head_sha: Some("abc123".to_string()),
            replayed_commit_count: 3,
            skipped_replay_commit_count: 0,
            next_child: None,
            update: None,
        });

        assert_eq!(output.command, JsonCommand::Prep);
        assert_eq!(output.result, SummaryResult::Summary);
        let json = serde_json::to_value(&output).unwrap();
        assert_eq!(json["result"], "summary");
        assert_eq!(json["data"]["kind"], "prep");
    }

    #[test]
    fn relink_prs_summary_uses_summary_envelope() {
        let output = relink_prs_summary(RelinkPrsSummaryData {
            repo: super::MaintenanceRepoContext {
                base: "main".to_string(),
                prefix: "dank-spr/".to_string(),
            },
            options: MaintenanceOptions { dry_run: true },
            expected_chain: vec![RelinkExpectedBaseData {
                local_pr_number: 1,
                stable_handle: "pr:alpha".to_string(),
                head_branch: "dank-spr/alpha".to_string(),
                expected_base_ref: "main".to_string(),
            }],
            decisions: vec![RelinkPrDecisionData {
                local_pr_number: 1,
                stable_handle: "pr:alpha".to_string(),
                head_branch: "dank-spr/alpha".to_string(),
                expected_base_ref: "main".to_string(),
                current_base_ref: Some("main".to_string()),
                remote_pr_number: Some(17),
                action: RelinkPrAction::AlreadyCorrect,
            }],
        });

        assert_eq!(output.command, JsonCommand::RelinkPrs);
        let json = serde_json::to_value(&output).unwrap();
        assert_eq!(json["result"], "summary");
        assert_eq!(json["data"]["kind"], "relink_prs");
    }

    #[test]
    fn cleanup_summary_uses_summary_envelope() {
        let output = cleanup_summary(CleanupSummaryData {
            repo: CleanupRepoContext {
                prefix: "dank-spr/".to_string(),
            },
            options: MaintenanceOptions { dry_run: true },
            remote_candidates: vec!["dank-spr/alpha".to_string()],
            open_pr_heads: vec!["dank-spr/alpha".to_string()],
            decisions: vec![CleanupDecisionData {
                branch: "dank-spr/alpha".to_string(),
                action: CleanupAction::SkipOpenPr,
            }],
            delete_batch: Vec::new(),
        });

        assert_eq!(output.command, JsonCommand::Cleanup);
        let json = serde_json::to_value(&output).unwrap();
        assert_eq!(json["result"], "summary");
        assert_eq!(json["data"]["kind"], "cleanup");
    }

    #[test]
    fn prep_summary_preserves_payload_shape() {
        let output = prep_summary(PrepSummaryData {
            repo: PrepRepoContext {
                base: "main".to_string(),
                prefix: "dank-spr/".to_string(),
                ignore_tag: "ignore".to_string(),
            },
            options: PrepOptions {
                dry_run: false,
                pr_description_mode: PrDescriptionMode::Overwrite,
            },
            selection: ResolvedPrepSelection::All,
            selected_groups: Vec::new(),
            rewritten_head_sha: None,
            replayed_commit_count: 0,
            skipped_replay_commit_count: 0,
            next_child: None,
            update: None,
        });

        assert_eq!(
            output.data,
            MaintenancePayload::Prep {
                data: Box::new(PrepSummaryData {
                    repo: PrepRepoContext {
                        base: "main".to_string(),
                        prefix: "dank-spr/".to_string(),
                        ignore_tag: "ignore".to_string(),
                    },
                    options: PrepOptions {
                        dry_run: false,
                        pr_description_mode: PrDescriptionMode::Overwrite,
                    },
                    selection: ResolvedPrepSelection::All,
                    selected_groups: Vec::new(),
                    rewritten_head_sha: None,
                    replayed_commit_count: 0,
                    skipped_replay_commit_count: 0,
                    next_child: None,
                    update: None,
                }),
            }
        );
    }
}
