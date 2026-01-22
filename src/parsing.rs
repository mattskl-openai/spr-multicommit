use crate::git::git_ro;
use anyhow::{bail, Result};
use regex::Regex;
use tracing::warn;

#[derive(Debug, Default, Clone)]
pub struct Group {
    pub tag: String,
    pub subjects: Vec<String>,
    pub commits: Vec<String>, // SHAs oldest→newest
    pub first_message: Option<String>,
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

/// Parse commit stream from `git log --format=%H%x00%B%x1e --reverse <range>`
pub fn parse_groups(raw: &str) -> Result<Vec<Group>> {
    let re = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
    let mut groups: Vec<Group> = vec![];
    let mut current: Option<Group> = None;

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
            if let Some(g) = current.take() {
                if !g.commits.is_empty() {
                    groups.push(g);
                }
            }
            let tag = cap.get(1).unwrap().as_str().to_string();
            current = Some(Group {
                tag,
                subjects: vec![subj.clone()],
                commits: vec![sha],
                first_message: Some(message.clone()),
            });
        } else if let Some(g) = current.as_mut() {
            g.subjects.push(subj);
            g.commits.push(sha);
        } else {
            warn!("Untagged commit before first pr:<tag>; ignored");
        }
    }
    if let Some(g) = current.take() {
        if !g.commits.is_empty() {
            groups.push(g);
        }
    }
    Ok(groups)
}

/// Derive PR groups from local commits between merge-base(base, to)..to (oldest→newest).
/// Returns (merge_base, groups).
pub fn derive_groups_between(base: &str, to: &str) -> Result<(String, Vec<Group>)> {
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
    let groups = parse_groups(&lines)?;
    Ok((merge_base, groups))
}

/// Convenience: derive PR groups from local commits between merge-base(base, HEAD)..HEAD.
pub fn derive_local_groups(base: &str) -> Result<(String, Vec<Group>)> {
    derive_groups_between(base, "HEAD")
}
