//! Machine-readable output for read-only discovery commands.

use serde::Serialize;

use crate::json_output::JsonCommand;
use crate::summary_output::SummaryOutput;

pub type ReadOnlyOutput = SummaryOutput<ReadOnlyPayload>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReadOnlyPayload {
    PrList {
        #[serde(flatten)]
        data: crate::commands::PrListData,
    },
    CommitList {
        #[serde(flatten)]
        data: crate::commands::CommitListData,
    },
}

pub fn pr_list(command: JsonCommand, data: crate::commands::PrListData) -> ReadOnlyOutput {
    SummaryOutput::new(command, ReadOnlyPayload::PrList { data })
}

pub fn commit_list(command: JsonCommand, data: crate::commands::CommitListData) -> ReadOnlyOutput {
    SummaryOutput::new(command, ReadOnlyPayload::CommitList { data })
}

#[cfg(test)]
mod tests {
    use super::{commit_list, pr_list, ReadOnlyPayload};
    use crate::commands::{
        CommitEntryData, CommitGroupData, CommitListData, PrGroupData, PrListData,
        RemotePrMetadata, RemotePrState,
    };
    use crate::github::{PrCiReviewStatus, PrCiState, PrReviewDecision, PrState};
    use crate::json_output::JsonCommand;
    use crate::summary_output::{SummaryOutput, SummaryResult};

    #[test]
    fn pr_list_output_uses_summary_envelope() {
        let output = pr_list(
            JsonCommand::ListPr,
            PrListData {
                groups: vec![PrGroupData {
                    local_pr_number: 1,
                    stable_handle: "pr:alpha".to_string(),
                    head_branch: "dank-spr/alpha".to_string(),
                    first_commit_sha: "aaaaaaaa1".to_string(),
                    commit_count: 2,
                    first_subject: "feat: alpha".to_string(),
                    remote: RemotePrMetadata {
                        state: RemotePrState::RemoteWithCiReview {
                            pr_number: 17,
                            url: "https://github.com/o/r/pull/17".to_string(),
                            base_branch: "main".to_string(),
                            state: PrState::Open,
                            ci_review_status: PrCiReviewStatus {
                                ci_state: PrCiState::Success,
                                review_decision: PrReviewDecision::Approved,
                            },
                        },
                    },
                }],
            },
        );

        assert_eq!(output.command, JsonCommand::ListPr);
        assert_eq!(output.result, SummaryResult::Summary);
        let json = serde_json::to_value(&output).unwrap();
        assert_eq!(json["result"], "summary");
        assert_eq!(json["data"]["kind"], "pr_list");
        assert_eq!(json["data"]["groups"][0]["stable_handle"], "pr:alpha");
    }

    #[test]
    fn commit_list_output_uses_summary_envelope() {
        let output = commit_list(
            JsonCommand::ListCommit,
            CommitListData {
                groups: vec![CommitGroupData {
                    local_pr_number: 1,
                    stable_handle: "pr:alpha".to_string(),
                    head_branch: "dank-spr/alpha".to_string(),
                    remote: RemotePrMetadata {
                        state: RemotePrState::NoRemote,
                    },
                    commits: vec![CommitEntryData {
                        global_commit_index: 1,
                        sha: "aaaaaaaa1".to_string(),
                        subject: "feat: alpha".to_string(),
                    }],
                }],
            },
        );

        assert_eq!(output.command, JsonCommand::ListCommit);
        assert_eq!(
            output,
            SummaryOutput::new(
                JsonCommand::ListCommit,
                ReadOnlyPayload::CommitList {
                    data: CommitListData {
                        groups: vec![CommitGroupData {
                            local_pr_number: 1,
                            stable_handle: "pr:alpha".to_string(),
                            head_branch: "dank-spr/alpha".to_string(),
                            remote: RemotePrMetadata {
                                state: RemotePrState::NoRemote,
                            },
                            commits: vec![CommitEntryData {
                                global_commit_index: 1,
                                sha: "aaaaaaaa1".to_string(),
                                subject: "feat: alpha".to_string(),
                            }],
                        }],
                    },
                },
            )
        );
    }
}
