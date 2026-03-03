//! Shared parsing and validation for PR-group labels.
//!
//! Labels are the immutable payload in `pr:<label>` commit markers and in
//! stable selector inputs. They must start with an ASCII letter and may then
//! use ASCII letters, digits, `.`, `_`, or `-`.

use regex::Regex;
use std::sync::OnceLock;

const VALID_MARKER_PATTERN: &str = r"(?i)\bpr:([A-Za-z][A-Za-z0-9._\-]*)\b";
const CANDIDATE_MARKER_PATTERN: &str = r"(?i)\bpr:([A-Za-z0-9._\-]+)\b";

static VALID_MARKER_REGEX: OnceLock<Regex> = OnceLock::new();
static CANDIDATE_MARKER_REGEX: OnceLock<Regex> = OnceLock::new();

/// A validation failure for a PR-group label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelValidationError {
    MustStartWithLetter,
    InvalidCharacters,
}

impl std::fmt::Display for LabelValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MustStartWithLetter => write!(f, "must start with an ASCII letter"),
            Self::InvalidCharacters => write!(
                f,
                "must use only ASCII letters, digits, `.`, `_`, or `-` after the first letter"
            ),
        }
    }
}

impl std::error::Error for LabelValidationError {}

/// Returns the regex for valid `pr:<label>` markers in commit messages.
pub fn valid_marker_regex() -> &'static Regex {
    VALID_MARKER_REGEX.get_or_init(|| {
        Regex::new(VALID_MARKER_PATTERN).expect("valid PR label regex should compile")
    })
}

/// Returns the regex used to find candidate `pr:<tag>` markers before validation.
pub fn candidate_marker_regex() -> &'static Regex {
    CANDIDATE_MARKER_REGEX.get_or_init(|| {
        Regex::new(CANDIDATE_MARKER_PATTERN).expect("candidate PR label regex should compile")
    })
}

/// Validates one PR-group label against the shared commit-marker and selector grammar.
pub fn validate_label(label: &str) -> std::result::Result<(), LabelValidationError> {
    let mut chars = label.chars();
    if let Some(first) = chars.next() {
        if !first.is_ascii_alphabetic() {
            Err(LabelValidationError::MustStartWithLetter)
        } else if chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')) {
            Ok(())
        } else {
            Err(LabelValidationError::InvalidCharacters)
        }
    } else {
        Err(LabelValidationError::MustStartWithLetter)
    }
}
