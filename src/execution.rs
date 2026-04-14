//! Execution policy for commands that can change repository or GitHub state.
//!
//! CLI parsing converts command-local `--dry-run` flags into this type at the
//! boundary. Command implementations use it to decide whether state-changing IO
//! should be applied or only reported.

/// Whether a state-changing command should apply changes or report them only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    /// Execute state-changing operations.
    #[default]
    Apply,
    /// Report state-changing operations without applying them.
    DryRun,
}
