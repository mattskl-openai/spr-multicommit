use anyhow::{anyhow, Result};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{info, warn};

use crate::git::{gh_ro, gh_rw, git_ro, sanitize_gh_base_ref};

#[derive(Debug, Deserialize, Clone)]
pub struct PrInfo {
    pub number: u64,
    pub head: String,
    pub base: String,
}

#[derive(Debug, Clone)]
pub struct PrRef {
    pub number: u64,
    pub head: String,
    pub base: String,
}

#[derive(Clone)]
pub struct PrBodyInfo {
    pub id: String,
    pub body: String,
}

pub fn fetch_pr_bodies_graphql(numbers: &Vec<u64>) -> Result<HashMap<u64, PrBodyInfo>> {
    let mut out = HashMap::new();
    if numbers.is_empty() {
        return Ok(out);
    }
    let (owner, name) = get_repo_owner_name()?;
    let mut q =
        String::from("query($owner:String!,$name:String!){ repository(owner:$owner,name:$name){ ");
    for (i, n) in numbers.iter().enumerate() {
        q.push_str(&format!(
            "pr{}: pullRequest(number: {}) {{ id body }} ",
            i, n
        ));
    }
    q.push_str("} }");
    let json = gh_ro(
        [
            "api",
            "graphql",
            "-f",
            &format!("query={}", q),
            "-F",
            &format!("owner={}", owner),
            "-F",
            &format!("name={}", name),
        ]
        .as_slice(),
    )?;
    let v: serde_json::Value = serde_json::from_str(&json)?;
    let repo = &v["data"]["repository"];
    for (i, n) in numbers.iter().enumerate() {
        let key = format!("pr{}", i);
        let id = repo[&key]["id"].as_str().unwrap_or("").to_string();
        let body = repo[&key]["body"].as_str().unwrap_or("").to_string();
        out.insert(*n, PrBodyInfo { id, body });
    }
    Ok(out)
}

pub fn get_repo_owner_name() -> Result<(String, String)> {
    let url = git_ro(["config", "--get", "remote.origin.url"].as_slice())?
        .trim()
        .to_string();
    if let Some(idx) = url.find("://") {
        let rest = &url[idx + 3..];
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() >= 3 {
            let owner = parts[1].to_string();
            let mut name = parts[2].to_string();
            if let Some(s) = name.strip_suffix(".git") {
                name = s.to_string();
            }
            return Ok((owner, name));
        }
    } else if let Some(pos) = url.find(":") {
        // git@github.com:owner/name.git
        let rest = &url[pos + 1..];
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() >= 2 {
            let owner = parts[0].to_string();
            let mut name = parts[1].to_string();
            if let Some(s) = name.strip_suffix(".git") {
                name = s.to_string();
            }
            return Ok((owner, name));
        }
    }
    anyhow::bail!("Unable to parse remote.origin.url: {}", url)
}

pub fn graphql_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

pub fn list_spr_prs(prefix: &str) -> Result<Vec<PrInfo>> {
    let json = gh_ro(
        [
            "pr",
            "list",
            "--state",
            "open",
            "--limit",
            "200",
            "--json",
            "number,headRefName,baseRefName",
        ]
        .as_slice(),
    )?;
    #[derive(Deserialize)]
    struct Raw {
        number: u64,
        #[serde(rename = "headRefName")]
        headRefName: String,
        #[serde(rename = "baseRefName")]
        baseRefName: String,
    }
    let raws: Vec<Raw> = serde_json::from_str(&json)?;
    let mut out = vec![];
    for r in raws {
        if r.headRefName.starts_with(prefix) {
            out.push(PrInfo {
                number: r.number,
                head: r.headRefName,
                base: r.baseRefName,
            });
        }
    }
    if out.is_empty() {
        warn!("No open PRs with head starting with `{}` found.", prefix);
    }
    Ok(out)
}

pub fn upsert_pr(branch: &str, parent: &str, title: &str, body: &str, dry: bool) -> Result<u64> {
    if dry {
        if std::env::var_os("SPR_DRY_ASSUME_EXISTING").is_some() {
            let n = fake_pr_number(branch);
            gh_rw(
                dry,
                [
                    "pr",
                    "edit",
                    &format!("#{}", n),
                    "--title",
                    title,
                    "--base",
                    &sanitize_gh_base_ref(parent),
                    "--body",
                    body,
                ]
                .as_slice(),
            )?;
            return Ok(n);
        } else {
            // In dry-run default, pretend PR does not exist and show create command
            gh_rw(
                dry,
                [
                    "pr",
                    "create",
                    "--head",
                    branch,
                    "--base",
                    &sanitize_gh_base_ref(parent),
                    "--title",
                    title,
                    "--body",
                    body,
                ]
                .as_slice(),
            )?;
            return Ok(0);
        }
    }
    // Check for existing open PR by head
    let json = gh_ro(
        [
            "pr", "list", "--state", "open", "--head", branch, "--limit", "1", "--json", "number",
        ]
        .as_slice(),
    )?;
    #[derive(Deserialize)]
    struct V {
        number: u64,
    }
    let existing: Vec<V> = serde_json::from_str(&json)?;
    if let Some(v) = existing.get(0) {
        // Defer edits (title/body) to final update pass; just return the number
        return Ok(v.number);
    }

    // Create new PR
    gh_rw(
        dry,
        [
            "pr",
            "create",
            "--head",
            branch,
            "--base",
            &sanitize_gh_base_ref(parent),
            "--title",
            title,
            "--body",
            body,
        ]
        .as_slice(),
    )?;

    // Fetch created PR number
    let json2 = gh_ro(
        [
            "pr", "list", "--state", "open", "--head", branch, "--limit", "1", "--json", "number",
        ]
        .as_slice(),
    )?;
    let created: Vec<V> = serde_json::from_str(&json2)?;
    let num = created
        .get(0)
        .map(|v| v.number)
        .ok_or_else(|| anyhow!("Failed to determine PR number for {}", branch))?;
    Ok(num)
}

pub fn upsert_pr_cached(
    branch: &str,
    parent: &str,
    title: &str,
    body: &str,
    dry: bool,
    prs_by_head: &mut HashMap<String, u64>,
) -> Result<u64> {
    if let Some(&num) = prs_by_head.get(branch) {
        // Defer edits to the final pass
        return Ok(num);
    }
    gh_rw(
        dry,
        [
            "pr", "create", "--head", branch, "--base", parent, "--title", title, "--body", body,
        ]
        .as_slice(),
    )?;
    let json = gh_ro(
        [
            "pr", "list", "--state", "open", "--head", branch, "--limit", "1", "--json", "number",
        ]
        .as_slice(),
    )?;
    #[derive(Deserialize)]
    struct V {
        number: u64,
    }
    let arr: Vec<V> = serde_json::from_str(&json)?;
    let num = arr
        .get(0)
        .map(|v| v.number)
        .ok_or_else(|| anyhow!("Failed to determine PR number for {}", branch))?;
    prs_by_head.insert(branch.to_string(), num);
    Ok(num)
}

pub fn update_stack_bodies(stack: &Vec<PrRef>, dry: bool) -> Result<()> {
    if stack.is_empty() {
        return Ok(());
    }

    let numbers: Vec<u64> = stack.iter().map(|p| p.number).collect();
    let numbers_rev: Vec<u64> = numbers.iter().cloned().rev().collect();
    let bodies_by_number = fetch_pr_bodies_graphql(&numbers)?;
    let mut to_update: Vec<(u64, String, String)> = vec![]; // (number, id, new_body)

    for (_idx, pr) in stack.iter().enumerate() {
        let mut body = bodies_by_number
            .get(&pr.number)
            .map(|x| x.body.clone())
            .unwrap_or_default();

        let start = "<!-- spr-stack:start -->";
        let end = "<!-- spr-stack:end -->";
        let re = Regex::new(&format!(
            r"(?s){}.*?{}",
            regex::escape(start),
            regex::escape(end)
        ))?;
        body = re.replace(&body, "").trim().to_string();

        let em_space = "\u{2003}"; // U+2003 EM SPACE for indentation
        let mut lines = String::new();
        for n in &numbers_rev {
            let marker = if *n == pr.number { "➡" } else { em_space };
            lines.push_str(&format!("- {} #{}\n", marker, n));
        }
        let block = format!(
            "\n\n{}\n**Stack**:\n{}\n\n⚠️ *Part of a stack created by [spr-multicommit](https://github.com/mattskl-openai/spr-multicommit). Do not merge manually using the UI - doing so may have unexpected results.*\n{}\n",
            start,
            lines.trim_end(),
            end,
        );
        let new_body = if body.is_empty() {
            block.clone()
        } else {
            format!("{}\n\n{}", body, block)
        };

        if new_body.trim() == body.trim() {
            info!("PR #{} body unchanged; skipping edit", pr.number);
        } else {
            let id = bodies_by_number
                .get(&pr.number)
                .map(|x| x.id.clone())
                .unwrap_or_default();
            if !id.is_empty() {
                to_update.push((pr.number, id, new_body));
            }
        }
    }
    if !to_update.is_empty() {
        let mut m = String::from("mutation {");
        for (i, (_num, id, body)) in to_update.iter().enumerate() {
            m.push_str(&format!("m{}: updatePullRequest(input:{{pullRequestId:\"{}\", body:\"{}\"}}){{ clientMutationId }} ", i, id, graphql_escape(body)));
        }
        m.push_str("}");
        gh_rw(
            dry,
            ["api", "graphql", "-f", &format!("query={}", m)].as_slice(),
        )?;
        for (num, _, _) in to_update {
            info!("Updated stack visual in PR #{}", num);
        }
    }
    Ok(())
}

pub fn fake_pr_number(head: &str) -> u64 {
    let mut sum: u64 = 0;
    for b in head.bytes() {
        sum = sum.wrapping_add(b as u64);
    }
    1000 + (sum % 900_000)
}

/// Append a warning line to a specific PR body (idempotent). Returns Ok(()) whether updated or skipped.
pub fn append_warning_to_pr(number: u64, warning: &str, dry: bool) -> Result<()> {
    let bodies = fetch_pr_bodies_graphql(&vec![number])?;
    if let Some(info) = bodies.get(&number) {
        let body = info.body.clone();
        if body.contains(warning) {
            info!("Warning already present in PR #{}; skipping", number);
            return Ok(());
        }
        let new_body = if body.trim().is_empty() { warning.to_string() } else { format!("{}\n\n{}", warning, body) };
        let mut m = String::from("mutation {");
        m.push_str(&format!(
            "u: updatePullRequest(input:{{pullRequestId:\"{}\", body:\"{}\"}}){{ clientMutationId }} ",
            info.id,
            graphql_escape(&new_body)
        ));
        m.push_str("}");
        gh_rw(dry, ["api", "graphql", "-f", &format!("query={}", m)].as_slice())?;
        info!("Appended warning to PR #{}", number);
    }
    Ok(())
}
