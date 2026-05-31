use crate::json_output::JsonCommand;
use crate::summary_output::SummaryOutput;
use crate::validation::ValidationSummaryData;

pub type ValidationOutput = SummaryOutput<ValidationSummaryData>;

pub fn summary(data: ValidationSummaryData) -> ValidationOutput {
    SummaryOutput::new(JsonCommand::Validate, data)
}
