//! Shared parsing and validation for PR-group labels.
//!
//! Labels are the immutable payload in `pr:<label>` commit markers and in
//! stable selector inputs. They must start with an ASCII letter and may then
//! use ASCII letters, digits, `.`, `_`, or `-`.

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
    use super::{validate_label, LabelValidationError};

    #[test]
    fn validate_label_rejects_empty_string() {
        assert_eq!(
            validate_label("").unwrap_err(),
            LabelValidationError::MustStartWithLetter
        );
    }
}
