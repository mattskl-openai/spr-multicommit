//! Parse `pr:<tag>` commit markers into PR groups and attach ignore blocks.
//!
//! The parser treats `pr:ignore` (or a configured ignore tag) as a local-only block:
//! ignored commits are preserved in local history, but they are not part of any PR
//! grouping and are attached to the preceding group for rewrite operations.

use crate::git::git_ro;
use anyhow::{bail, Result};
use regex::Regex;
use tracing::warn;

/// A PR group derived from `pr:<tag>` markers in commit messages.
///
/// Groups are ordered oldest→newest, and each group owns the commits that will
/// become its branch tip. Ignore blocks are preserved and attached to the group
/// they follow so rewrite operations can keep local-only work.
#[derive(Debug, Default, Clone)]
pub struct Group {
    /// The tag value from `pr:<tag>`.
    pub tag: String,
    /// Subjects for commits in this group, oldest→newest.
    pub subjects: Vec<String>,
    /// Commit SHAs for the group, oldest→newest.
    pub commits: Vec<String>, // SHAs oldest→newest
    /// Full commit message for the first commit in the group.
    pub first_message: Option<String>,
    /// Commits that follow this group in an ignore block (pr:ignore_tag .. next pr:<tag>).
    pub ignored_after: Vec<String>,
}

impl Group {
    pub fn pr_title(&self) -> Result<String> {
        if let Some(s) = self.subjects.first() {
            let re = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
            let t = re.replace_all(s, "").trim().to_string();
            if !t.is_empty() {
                return Ok(t);
            }
        }
        Ok(self.tag.clone())
    }
    pub fn squash_commit_message(&self) -> Result<String> {
        if let Some(full) = &self.first_message {
            // Validate the first commit contains the expected pr:<tag> marker
            let re = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
            if let Some(cap) = re.captures(full) {
                let found = cap.get(1).unwrap().as_str();
                if !found.eq_ignore_ascii_case(&self.tag) {
                    bail!(
                        "First commit tag mismatch for group `{}`: expected `pr:{}`, found `pr:{}`",
                        self.tag,
                        self.tag,
                        found
                    );
                }
            } else {
                bail!(
                    "First commit is missing required `pr:{}` tag for group `{}`.",
                    self.tag,
                    self.tag
                );
            }
            return Ok(full.trim_end().to_string());
        }
        bail!("First commit message missing for group `{}`", self.tag)
    }
    pub fn pr_body(&self) -> Result<String> {
        // Use only the body (drop the subject/title line); remove pr:<tag> markers
        let base_body = if let Some(full) = &self.first_message {
            let mut it = full.lines();
            let _ = it.next();
            it.collect::<Vec<_>>().join("\n")
        } else {
            String::new()
        };
        let re = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
        let cleaned = re
            .replace_all(&base_body, "")
            .to_string()
            .trim()
            .to_string();
        let sep = if cleaned.is_empty() { "" } else { "\n\n" };
        Ok(format!(
            "{}{}<!-- spr-stack:start -->\n(placeholder; will be filled by spr)\n<!-- spr-stack:end -->",
            cleaned, sep,
        ))
    }

    /// Body derived from the first commit message (without the title line) and with pr:<tag> markers removed.
    /// Does not include any stack markers. Trimmed.
    pub fn pr_body_base(&self) -> Result<String> {
        let base_body = if let Some(full) = &self.first_message {
            let mut it = full.lines();
            let _ = it.next();
            it.collect::<Vec<_>>().join("\n")
        } else {
            String::new()
        };
        let re = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
        Ok(re
            .replace_all(&base_body, "")
            .to_string()
            .trim()
            .to_string())
    }
}

/// Parse a reversed git log stream into PR groups, honoring an ignore tag.
///
/// The input must be the raw output of `git log --format=%H%x00%B%x1e --reverse <range>`.
/// Commits with a single `pr:<tag>` marker start a new group, and untagged commits
/// are appended to the current group once one exists.
///
/// If a commit's tag matches `ignore_tag` (case-sensitive), the current group is
/// finalized and the parser enters ignore mode; commits are skipped until the next
/// non-ignore `pr:<tag>` marker is seen.
///
/// # Errors
///
/// Returns an error if any commit message contains more than one `pr:<tag>` marker.
pub fn parse_groups(raw: &str, ignore_tag: &str) -> Result<Vec<Group>> {
    let (_leading_ignored, groups) = parse_groups_with_ignored(raw, ignore_tag)?;
    Ok(groups)
}

/// Parse a reversed git log stream into PR groups while retaining ignored commits.
///
/// Ignored commits are attached to the preceding group as `ignored_after`. If an ignore
/// block appears before the first group, those commits are returned separately as
/// `leading_ignored`.
///
/// # Errors
///
/// Returns an error if any commit message contains more than one `pr:<tag>` marker.
pub fn parse_groups_with_ignored(
    raw: &str,
    ignore_tag: &str,
) -> Result<(Vec<String>, Vec<Group>)> {
    let re = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
    let mut groups: Vec<Group> = vec![];
    let mut current: Option<Group> = None;
    let mut ignoring = false;
    let mut ignored_block: Vec<String> = vec![];
    let mut leading_ignored: Vec<String> = vec![];

    let flush_current = |current: &mut Option<Group>, groups: &mut Vec<Group>| {
        if let Some(g) = current.take() {
            if !g.commits.is_empty() {
                groups.push(g);
            }
        }
    };

    let flush_ignored =
        |ignored_block: &mut Vec<String>, groups: &mut Vec<Group>, leading: &mut Vec<String>| {
            if ignored_block.is_empty() {
                return;
            }
            if let Some(last) = groups.last_mut() {
                last.ignored_after.append(ignored_block);
            } else {
                leading.append(ignored_block);
            }
        };

    for chunk in raw.split('\u{001e}') {
        let chunk = chunk.trim_end_matches('\n');
        if chunk.trim().is_empty() {
            continue;
        }
        let mut parts = chunk.splitn(2, '\0');
        let sha = parts.next().unwrap_or_default().trim().to_string();
        let message = parts.next().unwrap_or_default().to_string();
        let subj = message.lines().next().unwrap_or_default().to_string();

        let tag_matches = re.captures_iter(&message).count();
        if tag_matches > 1 {
            bail!("Multiple pr:<tag> markers found in commit {sha}");
        }

        if tag_matches == 1 {
            let cap = re.captures(&message).unwrap();
            let tag = cap.get(1).unwrap().as_str().to_string();
            if tag == ignore_tag {
                flush_current(&mut current, &mut groups);
                ignoring = true;
                ignored_block.push(sha);
                continue;
            }
            if ignoring {
                ignoring = false;
                flush_ignored(&mut ignored_block, &mut groups, &mut leading_ignored);
            }
            flush_current(&mut current, &mut groups);
            current = Some(Group {
                tag,
                subjects: vec![subj.clone()],
                commits: vec![sha],
                first_message: Some(message.clone()),
                ignored_after: Vec::new(),
            });
        } else if ignoring {
            ignored_block.push(sha);
        } else if let Some(g) = current.as_mut() {
            g.subjects.push(subj);
            g.commits.push(sha);
        } else {
            warn!("Untagged commit before first pr:<tag>; ignored");
        }
    }
    flush_current(&mut current, &mut groups);
    if ignoring {
        flush_ignored(&mut ignored_block, &mut groups, &mut leading_ignored);
    }
    Ok((leading_ignored, groups))
}

/// Derive PR groups from `merge-base(base, to)..to` in oldest→newest order.
///
/// Returns the computed merge base alongside the parsed groups, using `ignore_tag`
/// to skip ignored blocks during parsing.
pub fn derive_groups_between(
    base: &str,
    to: &str,
    ignore_tag: &str,
) -> Result<(String, Vec<Group>)> {
    let merge_base = git_ro(["merge-base", base, to].as_slice())?
        .trim()
        .to_string();
    let lines = git_ro(
        [
            "log",
            "--format=%H%x00%B%x1e",
            "--reverse",
            &format!("{merge_base}..{to}"),
        ]
        .as_slice(),
    )?;
    let groups = parse_groups(&lines, ignore_tag)?;
    Ok((merge_base, groups))
}

/// Convenience: derive PR groups from merge-base(base, HEAD)..HEAD.
pub fn derive_local_groups(base: &str, ignore_tag: &str) -> Result<(String, Vec<Group>)> {
    derive_groups_between(base, "HEAD", ignore_tag)
}

/// Derive PR groups and leading ignored commits from `merge-base(base, to)..to`.
///
/// Leading ignored commits come from an ignore block that appears before the first
/// `pr:<tag>` marker and are preserved ahead of the stack during rewrite operations.
///
/// # Errors
///
/// Returns errors from git commands or group parsing.
pub fn derive_groups_between_with_ignored(
    base: &str,
    to: &str,
    ignore_tag: &str,
) -> Result<(String, Vec<String>, Vec<Group>)> {
    let merge_base = git_ro(["merge-base", base, to].as_slice())?
        .trim()
        .to_string();
    let lines = git_ro(
        [
            "log",
            "--format=%H%x00%B%x1e",
            "--reverse",
            &format!("{merge_base}..{to}"),
        ]
        .as_slice(),
    )?;
    let (leading_ignored, groups) = parse_groups_with_ignored(&lines, ignore_tag)?;
    Ok((merge_base, leading_ignored, groups))
}

/// Convenience: derive PR groups and leading ignored commits from merge-base(base, HEAD)..HEAD.
///
/// # Errors
///
/// Returns errors from git commands or group parsing.
pub fn derive_local_groups_with_ignored(
    base: &str,
    ignore_tag: &str,
) -> Result<(String, Vec<String>, Vec<Group>)> {
    derive_groups_between_with_ignored(base, "HEAD", ignore_tag)
}

#[cfg(test)]
mod tests {
    use super::{parse_groups, parse_groups_with_ignored};

    fn make_log(entries: &[(&str, &str)]) -> String {
        let mut out = String::new();
        for (sha, msg) in entries {
            out.push_str(sha);
            out.push('\0');
            out.push_str(msg);
            out.push('\u{001e}');
        }
        out
    }

    #[test]
    fn parse_groups_custom_ignore_tag() {
        let raw = make_log(&[
            ("a1", "feat: alpha start pr:alpha"),
            ("a2", "feat: alpha follow-up"),
            ("i1", "feat: ignore group pr:ignore"),
            ("i2", "feat: ignore follow-up"),
            ("s1", "chore: block start pr:skip"),
            ("s2", "wip: skipped work"),
            ("b1", "feat: beta start pr:beta"),
        ]);
        let groups = parse_groups(&raw, "skip").expect("parse_groups ok");
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].tag, "alpha");
        assert_eq!(groups[0].commits, vec!["a1", "a2"]);
        assert_eq!(groups[1].tag, "ignore");
        assert_eq!(groups[1].commits, vec!["i1", "i2"]);
        assert_eq!(groups[2].tag, "beta");
        assert_eq!(groups[2].commits, vec!["b1"]);
    }

    #[test]
    fn parse_groups_with_ignored_attaches_to_previous() {
        let raw = make_log(&[
            ("a1", "feat: alpha start pr:alpha"),
            ("a2", "feat: alpha follow-up"),
            ("i1", "chore: experiments pr:ignore"),
            ("i2", "wip: local spike"),
            ("b1", "feat: beta start pr:beta"),
            ("b2", "feat: beta follow-up"),
        ]);
        let (leading, groups) =
            parse_groups_with_ignored(&raw, "ignore").expect("parse_groups_with_ignored ok");
        assert!(leading.is_empty());
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].tag, "alpha");
        assert_eq!(groups[0].commits, vec!["a1", "a2"]);
        assert_eq!(groups[0].ignored_after, vec!["i1", "i2"]);
        assert_eq!(groups[1].tag, "beta");
        assert_eq!(groups[1].commits, vec!["b1", "b2"]);
        assert!(groups[1].ignored_after.is_empty());

        // Superset check: the wrapper should drop ignored commits but preserve grouping.
        let groups_via_wrapper = parse_groups(&raw, "ignore").expect("parse_groups ok");
        assert_eq!(groups_via_wrapper.len(), 2);
        assert_eq!(groups_via_wrapper[0].tag, "alpha");
        assert_eq!(groups_via_wrapper[0].commits, vec!["a1", "a2"]);
        assert_eq!(groups_via_wrapper[1].tag, "beta");
        assert_eq!(groups_via_wrapper[1].commits, vec!["b1", "b2"]);
    }

    #[test]
    fn parse_groups_with_ignored_leading_block() {
        let raw = make_log(&[
            ("i1", "chore: experiments pr:ignore"),
            ("i2", "wip: local spike"),
            ("a1", "feat: alpha start pr:alpha"),
        ]);
        let (leading, groups) =
            parse_groups_with_ignored(&raw, "ignore").expect("parse_groups_with_ignored ok");
        assert_eq!(leading, vec!["i1", "i2"]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].tag, "alpha");
        assert!(groups[0].ignored_after.is_empty());
    }

    #[test]
    fn parse_groups_ignore_tag_is_case_sensitive() {
        let raw = make_log(&[
            ("a1", "feat: alpha start pr:alpha"),
            ("i1", "chore: uppercase tag pr:IGNORE"),
            ("b1", "feat: beta start pr:beta"),
        ]);
        let groups = parse_groups(&raw, "ignore").expect("parse_groups ok");
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].tag, "alpha");
        assert_eq!(groups[1].tag, "IGNORE");
        assert_eq!(groups[2].tag, "beta");
    }
}
