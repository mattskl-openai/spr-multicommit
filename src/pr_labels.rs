//! Shared parsing and validation for PR-group labels.
//!
//! Labels are the immutable payload in `pr:<label>` commit markers and in
//! stable selector inputs. They must start with an ASCII letter and may then
//! use ASCII letters, digits, `.`, `_`, or `-`.

use regex::{Captures, Regex};
use std::sync::OnceLock;

const VALID_MARKER_PATTERN: &str = r"(?i)(^|[^A-Za-z0-9_])pr:([A-Za-z][A-Za-z0-9._\-]*)($|\s)";
const CANDIDATE_MARKER_PATTERN: &str = r"(?i)(^|[^A-Za-z0-9_])pr:(\S*)";

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
fn valid_marker_regex() -> &'static Regex {
    VALID_MARKER_REGEX.get_or_init(|| {
        Regex::new(VALID_MARKER_PATTERN).expect("valid PR label regex should compile")
    })
}

/// Returns the regex used to find candidate `pr:<tag>` markers before validation.
fn candidate_marker_regex() -> &'static Regex {
    CANDIDATE_MARKER_REGEX.get_or_init(|| {
        Regex::new(CANDIDATE_MARKER_PATTERN).expect("candidate PR label regex should compile")
    })
}

fn marker_label<'a>(capture: &'a Captures<'a>) -> &'a str {
    capture
        .get(2)
        .expect("marker regex should capture the label token")
        .as_str()
}

/// Returns every `pr:<token>` candidate from `text` using the shared marker tokenizer.
pub fn candidate_marker_labels(text: &str) -> Vec<String> {
    candidate_marker_regex()
        .captures_iter(text)
        .map(|capture| marker_label(&capture).to_string())
        .collect()
}

/// Returns the first valid label found in `text`, if any.
pub fn first_valid_marker_label(text: &str) -> Option<String> {
    valid_marker_regex()
        .captures(text)
        .map(|capture| marker_label(&capture).to_string())
}

/// Reports whether `text` contains any `pr:<token>` candidate.
pub fn contains_candidate_marker(text: &str) -> bool {
    candidate_marker_regex().is_match(text)
}

/// Removes valid `pr:<label>` markers from `text` without partially stripping malformed tokens.
pub fn strip_valid_markers(text: &str) -> String {
    valid_marker_regex()
        .replace_all(text, |capture: &Captures<'_>| {
            let prefix = capture.get(1).map_or("", |value| value.as_str());
            let suffix = capture.get(3).map_or("", |value| value.as_str());
            format!("{prefix}{suffix}")
        })
        .to_string()
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

#[cfg(test)]
mod tests {
    use super::{
        candidate_marker_labels, first_valid_marker_label, strip_valid_markers, validate_label,
        LabelValidationError,
    };

    #[test]
    fn candidate_markers_capture_full_tokens_until_whitespace() {
        assert_eq!(
            candidate_marker_labels("feat: alpha pr:alpha!oops more\npr:beta-\npr:"),
            vec![
                "alpha!oops".to_string(),
                "beta-".to_string(),
                "".to_string(),
            ]
        );
    }

    #[test]
    fn first_valid_marker_label_accepts_trailing_dash_and_dot() {
        assert_eq!(
            first_valid_marker_label("feat: alpha pr:alpha-"),
            Some("alpha-".to_string())
        );
        assert_eq!(
            first_valid_marker_label("feat: beta pr:beta."),
            Some("beta.".to_string())
        );
    }

    #[test]
    fn strip_valid_markers_does_not_partially_strip_invalid_tokens() {
        assert_eq!(strip_valid_markers("feat: alpha pr:alpha-"), "feat: alpha ");
        assert_eq!(
            strip_valid_markers("feat: alpha pr:alpha!oops"),
            "feat: alpha pr:alpha!oops"
        );
    }

    #[test]
    fn validate_label_rejects_empty_string() {
        assert_eq!(
            validate_label("").unwrap_err(),
            LabelValidationError::MustStartWithLetter
        );
    }
}
