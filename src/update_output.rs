use serde::Serialize;

use crate::config::PrDescriptionMode;
use crate::json_command::{JsonCommand, EXIT_SUCCESS};

const UPDATE_OUTPUT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpdateOutput {
    pub schema_version: u32,
    pub command: JsonCommand,
    #[serde(flatten)]
    pub payload: UpdatePayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum UpdatePayload {
    Summary { data: UpdateSummaryData },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpdateSummaryData {
    pub repo: UpdateRepoContext,
    pub options: UpdateOptions,
    pub extent: ResolvedUpdateLimit,
    pub warnings: Vec<String>,
    pub skipped_groups: Vec<SkippedUpdateGroupData>,
    pub groups: Vec<UpdateGroupData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpdateRepoContext {
    pub base: String,
    pub from: String,
    pub prefix: String,
    pub ignore_tag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpdateOptions {
    pub dry_run: bool,
    pub no_pr: bool,
    pub pr_description_mode: PrDescriptionMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResolvedUpdateLimit {
    All,
    ByPr { count: usize },
    ByCommits { count: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateExecutionData {
    pub warnings: Vec<String>,
    pub skipped_groups: Vec<SkippedUpdateGroupData>,
    pub groups: Vec<UpdateGroupData>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdatePushAction {
    Unchanged,
    CreateBranch,
    FastForwardBranch,
    ForcePushBranch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdatePrAction {
    NotRequested,
    Created,
    Existing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateEditAction {
    NotRequested,
    Unchanged,
    Updated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateSkippedReason {
    IgnoredBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkippedUpdateGroupData {
    pub stable_handle: String,
    pub reason: UpdateSkippedReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpdateGroupData {
    pub local_pr_number: usize,
    pub stable_handle: String,
    pub head_branch: String,
    pub base_ref: String,
    pub title: String,
    pub target_sha: String,
    pub push_action: UpdatePushAction,
    pub pr_action: UpdatePrAction,
    pub base_ref_action: UpdateEditAction,
    pub description_action: UpdateEditAction,
    pub remote_pr_number: Option<u64>,
    pub remote_pr_url: Option<String>,
}

impl UpdateSummaryData {
    pub fn from_execution(
        repo: UpdateRepoContext,
        options: UpdateOptions,
        extent: ResolvedUpdateLimit,
        execution: UpdateExecutionData,
    ) -> Self {
        Self {
            repo,
            options,
            extent,
            warnings: execution.warnings,
            skipped_groups: execution.skipped_groups,
            groups: execution.groups,
        }
    }
}

impl UpdateOutput {
    pub fn summary(data: UpdateSummaryData) -> Self {
        Self {
            schema_version: UPDATE_OUTPUT_SCHEMA_VERSION,
            command: JsonCommand::Update,
            payload: UpdatePayload::Summary { data },
        }
    }

    pub fn exit_code(&self) -> i32 {
        EXIT_SUCCESS
    }
}
