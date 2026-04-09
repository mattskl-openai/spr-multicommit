use serde::Serialize;

use crate::json_output::{JsonCommand, JSON_OUTPUT_SCHEMA_VERSION};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryResult {
    Summary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SummaryOutput<T> {
    pub schema_version: u32,
    pub command: JsonCommand,
    pub result: SummaryResult,
    pub data: T,
}

impl<T> SummaryOutput<T> {
    pub fn new(command: JsonCommand, data: T) -> Self {
        Self {
            schema_version: JSON_OUTPUT_SCHEMA_VERSION,
            command,
            result: SummaryResult::Summary,
            data,
        }
    }

    pub fn exit_code(&self) -> i32 {
        crate::json_output::EXIT_SUCCESS
    }
}
