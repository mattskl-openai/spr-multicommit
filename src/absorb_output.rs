//! Output contract for read-only absorb branch queries.

use serde::Serialize;

use crate::json_output::{JsonCommand, JSON_OUTPUT_SCHEMA_VERSION};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AbsorbQueryResult {
    Query,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AbsorbChangedBranchesOutput {
    pub schema_version: u32,
    pub command: JsonCommand,
    pub result: AbsorbQueryResult,
    pub data: AbsorbChangedBranchesData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AbsorbChangedBranchesData {
    pub changed_branches: Vec<String>,
}

pub fn changed_branches(changed_branches: Vec<String>) -> AbsorbChangedBranchesOutput {
    AbsorbChangedBranchesOutput {
        schema_version: JSON_OUTPUT_SCHEMA_VERSION,
        command: JsonCommand::Absorb,
        result: AbsorbQueryResult::Query,
        data: AbsorbChangedBranchesData { changed_branches },
    }
}

impl AbsorbChangedBranchesOutput {
    pub fn exit_code(&self) -> i32 {
        crate::json_output::EXIT_SUCCESS
    }

    pub fn render_human(&self) -> String {
        if self.data.changed_branches.is_empty() {
            "No branches would change.".to_string()
        } else {
            self.data.changed_branches.join("\n")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{changed_branches, AbsorbQueryResult};

    #[test]
    fn changed_branch_output_uses_query_envelope() {
        let output = changed_branches(vec!["stack".to_string(), "dank-spr/alpha".to_string()]);

        assert_eq!(output.result, AbsorbQueryResult::Query);
        let json = serde_json::to_value(&output).unwrap();
        assert_eq!(json["command"], "absorb");
        assert_eq!(json["result"], "query");
        assert_eq!(json["data"]["changed_branches"][0], "stack");
    }
}
