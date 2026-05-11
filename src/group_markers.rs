//! Shared parsing and rendering for PR-group seed markers.
//!
//! A seed commit identifies exactly one group either with `pr:<label>` or with
//! `branch:<branch-name>`. `pr:` labels keep the historical compact grammar,
//! while `branch:` names are validated through Git because real refname rules
//! are wider than the label grammar.

use anyhow::{bail, Result};
use regex::{Captures, Regex};
use std::sync::OnceLock;

const CANDIDATE_MARKER_PATTERN: &str = r"(?i)(^|[^A-Za-z0-9_])(pr|branch):(\S*)";

static CANDIDATE_MARKER_REGEX: OnceLock<Regex> = OnceLock::new();

/// The exact one-of marker stored on a PR-group seed commit.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupMarker {
    PrLabel(String),
    BranchName(String),
}

impl GroupMarker {
    pub fn explicit_selector_text(&self) -> String {
        match self {
            Self::PrLabel(label) => format!("pr:{label}"),
            Self::BranchName(branch_name) => format!("branch:{branch_name}"),
        }
    }

    pub fn bare_selector_text(&self) -> &str {
        match self {
            Self::PrLabel(label) => label,
            Self::BranchName(branch_name) => branch_name,
        }
    }

    pub fn concrete_branch_name(&self, prefix: &str) -> String {
        match self {
            Self::PrLabel(label) => format!("{prefix}{label}"),
            Self::BranchName(branch_name) => branch_name.clone(),
        }
    }

    pub fn is_ignore_pr_label(&self, ignore_tag: &str) -> bool {
        matches!(self, Self::PrLabel(label) if label == ignore_tag)
    }
}

impl std::fmt::Display for GroupMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.explicit_selector_text())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateGroupMarker {
    pub kind: CandidateGroupMarkerKind,
    pub payload: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateGroupMarkerKind {
    Pr,
    Branch,
}

impl CandidateGroupMarker {
    pub fn display_text(&self) -> String {
        match self.kind {
            CandidateGroupMarkerKind::Pr => format!("pr:{}", self.payload),
            CandidateGroupMarkerKind::Branch => format!("branch:{}", self.payload),
        }
    }

    pub fn validate(self) -> Result<GroupMarker> {
        match self.kind {
            CandidateGroupMarkerKind::Pr => {
                if let Err(err) = crate::pr_labels::validate_label(&self.payload) {
                    bail!("invalid PR tag `pr:{}`: {err}", self.payload);
                }
                Ok(GroupMarker::PrLabel(self.payload))
            }
            CandidateGroupMarkerKind::Branch => {
                crate::git::validate_branch_name(&self.payload)?;
                Ok(GroupMarker::BranchName(self.payload))
            }
        }
    }
}

fn candidate_marker_regex() -> &'static Regex {
    CANDIDATE_MARKER_REGEX.get_or_init(|| {
        Regex::new(CANDIDATE_MARKER_PATTERN).expect("candidate group marker regex should compile")
    })
}

fn marker_kind(capture: &Captures<'_>) -> CandidateGroupMarkerKind {
    let raw = capture
        .get(2)
        .expect("marker regex should capture marker kind")
        .as_str();
    if raw.eq_ignore_ascii_case("pr") {
        CandidateGroupMarkerKind::Pr
    } else {
        CandidateGroupMarkerKind::Branch
    }
}

fn marker_payload<'a>(capture: &'a Captures<'a>) -> &'a str {
    capture
        .get(3)
        .expect("marker regex should capture marker payload")
        .as_str()
}

/// Returns every `pr:` or `branch:` token candidate from `text`.
pub fn candidate_group_markers(text: &str) -> Vec<CandidateGroupMarker> {
    candidate_marker_regex()
        .captures_iter(text)
        .map(|capture| CandidateGroupMarker {
            kind: marker_kind(&capture),
            payload: marker_payload(&capture).to_string(),
        })
        .collect()
}

/// Returns the first valid group marker found in `text`, if any.
pub fn first_valid_group_marker(text: &str) -> Option<GroupMarker> {
    candidate_group_markers(text)
        .into_iter()
        .find_map(|candidate| candidate.validate().ok())
}

/// Removes valid group markers from `text` without partially stripping malformed tokens.
pub fn strip_valid_group_markers(text: &str) -> String {
    candidate_marker_regex()
        .replace_all(text, |capture: &Captures<'_>| {
            let candidate = CandidateGroupMarker {
                kind: marker_kind(capture),
                payload: marker_payload(capture).to_string(),
            };
            if candidate.validate().is_ok() {
                let prefix = capture.get(1).map_or("", |value| value.as_str());
                prefix.to_string()
            } else {
                capture
                    .get(0)
                    .expect("whole marker match should exist")
                    .as_str()
                    .to_string()
            }
        })
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        candidate_group_markers, first_valid_group_marker, strip_valid_group_markers,
        CandidateGroupMarkerKind, GroupMarker,
    };

    #[test]
    fn candidate_markers_capture_pr_and_branch_tokens() {
        let markers = candidate_group_markers(
            "feat: alpha pr:alpha\nfeat: login branch:feature/login branch:",
        );

        assert_eq!(markers.len(), 3);
        assert_eq!(markers[0].kind, CandidateGroupMarkerKind::Pr);
        assert_eq!(markers[0].payload, "alpha");
        assert_eq!(markers[1].kind, CandidateGroupMarkerKind::Branch);
        assert_eq!(markers[1].payload, "feature/login");
        assert_eq!(markers[2].kind, CandidateGroupMarkerKind::Branch);
        assert_eq!(markers[2].payload, "");
    }

    #[test]
    fn first_valid_group_marker_accepts_real_branch_names() {
        assert_eq!(
            first_valid_group_marker("feat: login branch:feature/login"),
            Some(GroupMarker::BranchName("feature/login".to_string()))
        );
    }

    #[test]
    fn strip_valid_group_markers_removes_both_valid_forms() {
        assert_eq!(
            strip_valid_group_markers("feat: login branch:feature/login"),
            "feat: login "
        );
        assert_eq!(
            strip_valid_group_markers("feat: alpha pr:alpha"),
            "feat: alpha "
        );
    }

    #[test]
    fn strip_valid_group_markers_preserves_invalid_branch_tokens() {
        assert_eq!(
            strip_valid_group_markers("feat: bad branch:bad..name"),
            "feat: bad branch:bad..name"
        );
    }
}
