//! Shared desired-vs-observed PR base-chain reconciliation.

use anyhow::{bail, Result};
use std::collections::HashMap;

use crate::branch_names::{
    canonical_branch_conflict_key, group_branch_identities, CanonicalBranchConflictKey,
};
use crate::commands::common;
use crate::git::sanitize_gh_base_ref;
use crate::github::{list_open_prs_for_heads, PrInfo};
use crate::parsing::Group;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredPrBase {
    pub local_pr_number: usize,
    pub stable_handle: String,
    pub head_branch: String,
    pub expected_base_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedOpenPrBase {
    pub remote_pr_number: u64,
    pub head_branch: String,
    pub current_base_ref: String,
}

#[derive(Debug, Clone, Default)]
pub struct ObservedPrBaseChain {
    by_head: HashMap<CanonicalBranchConflictKey, ObservedOpenPrBase>,
}

impl ObservedPrBaseChain {
    pub fn from_open_prs(prs: Vec<PrInfo>) -> Self {
        let by_head = prs
            .into_iter()
            .map(|pr| {
                (
                    canonical_branch_conflict_key(&pr.head),
                    ObservedOpenPrBase {
                        remote_pr_number: pr.number,
                        head_branch: pr.head,
                        current_base_ref: pr.base,
                    },
                )
            })
            .collect();
        Self { by_head }
    }

    pub fn observe_for_heads(heads: &[String]) -> Result<Self> {
        Ok(Self::from_open_prs(list_open_prs_for_heads(heads)?))
    }

    pub fn pr_numbers_by_head(&self) -> HashMap<CanonicalBranchConflictKey, u64> {
        self.by_head
            .iter()
            .map(|(head, pr)| (head.clone(), pr.remote_pr_number))
            .collect()
    }

    fn get_for_head(&self, head: &str) -> Option<&ObservedOpenPrBase> {
        self.by_head.get(&canonical_branch_conflict_key(head))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseReconciliationAction {
    AlreadyCorrect,
    NeedsEdit,
    MissingOpenPr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseReconciliationDecision {
    pub desired: DesiredPrBase,
    pub current_base_ref: Option<String>,
    pub remote_pr_number: Option<u64>,
    pub action: BaseReconciliationAction,
}

pub fn build_desired_pr_base_chain(
    base: &str,
    groups: &[Group],
    prefix: &str,
) -> Result<Vec<DesiredPrBase>> {
    let branch_identities = group_branch_identities(groups, prefix)?;
    let expected_by_head: HashMap<String, String> =
        common::build_head_base_chain(base, groups, prefix)?
            .into_iter()
            .collect();

    groups
        .iter()
        .zip(branch_identities)
        .enumerate()
        .map(|(group_idx, (group, identity))| {
            let expected_base_ref = expected_by_head
                .get(&identity.exact)
                .cloned()
                .unwrap_or_else(|| base.to_string());
            Ok(DesiredPrBase {
                local_pr_number: group_idx + 1,
                stable_handle: common::group_selector_text(group),
                head_branch: identity.exact,
                expected_base_ref,
            })
        })
        .collect()
}

pub fn plan_base_reconciliation(
    desired_chain: &[DesiredPrBase],
    observed_chain: &ObservedPrBaseChain,
) -> Vec<BaseReconciliationDecision> {
    desired_chain
        .iter()
        .map(|desired| {
            if let Some(observed) = observed_chain.get_for_head(&desired.head_branch) {
                let action = if sanitize_gh_base_ref(&observed.current_base_ref)
                    == sanitize_gh_base_ref(&desired.expected_base_ref)
                {
                    BaseReconciliationAction::AlreadyCorrect
                } else {
                    BaseReconciliationAction::NeedsEdit
                };
                BaseReconciliationDecision {
                    desired: desired.clone(),
                    current_base_ref: Some(observed.current_base_ref.clone()),
                    remote_pr_number: Some(observed.remote_pr_number),
                    action,
                }
            } else {
                BaseReconciliationDecision {
                    desired: desired.clone(),
                    current_base_ref: None,
                    remote_pr_number: None,
                    action: BaseReconciliationAction::MissingOpenPr,
                }
            }
        })
        .collect()
}

pub fn verify_base_edits_converged(
    edited_head_branches: &[String],
    decisions: &[BaseReconciliationDecision],
) -> Result<()> {
    let edited_heads = edited_head_branches
        .iter()
        .map(|head| canonical_branch_conflict_key(head))
        .collect::<Vec<_>>();
    let pending = decisions
        .iter()
        .filter(|decision| {
            edited_heads.contains(&canonical_branch_conflict_key(
                &decision.desired.head_branch,
            )) && decision.action != BaseReconciliationAction::AlreadyCorrect
        })
        .map(|decision| {
            format!(
                "{}: {} -> {}",
                decision.desired.head_branch,
                decision.current_base_ref.as_deref().unwrap_or("<missing>"),
                decision.desired.expected_base_ref
            )
        })
        .collect::<Vec<_>>();
    if pending.is_empty() {
        Ok(())
    } else {
        bail!(
            "GitHub PR base chain did not converge after update: {}",
            pending.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_desired_pr_base_chain, plan_base_reconciliation, verify_base_edits_converged,
        BaseReconciliationAction, ObservedPrBaseChain,
    };
    use crate::github::PrInfo;
    use crate::parsing::Group;

    fn groups(tags: &[&str]) -> Vec<Group> {
        tags.iter()
            .map(|tag| Group {
                marker: crate::group_markers::GroupMarker::PrLabel(tag.to_string()),
                subjects: vec![format!("feat: {tag}")],
                commits: vec![format!("{tag}1")],
                first_message: Some(format!("feat: {tag} pr:{tag}")),
                ignored_after: Vec::new(),
            })
            .collect()
    }

    fn pr(number: u64, head: &str, base: &str) -> PrInfo {
        PrInfo {
            number,
            head: head.to_string(),
            base: base.to_string(),
        }
    }

    #[test]
    fn desired_chain_follows_local_group_order() {
        let desired =
            build_desired_pr_base_chain("main", &groups(&["alpha", "beta", "gamma"]), "spr/")
                .unwrap();

        assert_eq!(
            desired
                .iter()
                .map(|row| (row.head_branch.as_str(), row.expected_base_ref.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("spr/alpha", "main"),
                ("spr/beta", "spr/alpha"),
                ("spr/gamma", "spr/beta"),
            ]
        );
    }

    #[test]
    fn planner_leaves_a_correct_chain_untouched() {
        let desired =
            build_desired_pr_base_chain("main", &groups(&["alpha", "beta", "gamma"]), "spr/")
                .unwrap();
        let observed = ObservedPrBaseChain::from_open_prs(vec![
            pr(1, "spr/alpha", "main"),
            pr(2, "spr/beta", "spr/alpha"),
            pr(3, "spr/gamma", "spr/beta"),
        ]);

        assert_eq!(
            plan_base_reconciliation(&desired, &observed)
                .iter()
                .map(|decision| decision.action)
                .collect::<Vec<_>>(),
            vec![
                BaseReconciliationAction::AlreadyCorrect,
                BaseReconciliationAction::AlreadyCorrect,
                BaseReconciliationAction::AlreadyCorrect,
            ]
        );
    }

    #[test]
    fn planner_only_retargets_the_post_land_bottom_pr() {
        let desired =
            build_desired_pr_base_chain("main", &groups(&["beta", "gamma"]), "spr/").unwrap();
        let observed = ObservedPrBaseChain::from_open_prs(vec![
            pr(2, "spr/beta", "spr/alpha"),
            pr(3, "spr/gamma", "spr/beta"),
        ]);

        assert_eq!(
            plan_base_reconciliation(&desired, &observed)
                .iter()
                .map(|decision| decision.action)
                .collect::<Vec<_>>(),
            vec![
                BaseReconciliationAction::NeedsEdit,
                BaseReconciliationAction::AlreadyCorrect,
            ]
        );
    }

    #[test]
    fn planner_keeps_missing_open_prs_explicit() {
        let desired =
            build_desired_pr_base_chain("main", &groups(&["alpha", "beta"]), "spr/").unwrap();
        let observed = ObservedPrBaseChain::from_open_prs(vec![pr(1, "spr/alpha", "main")]);

        assert_eq!(
            plan_base_reconciliation(&desired, &observed)
                .iter()
                .map(|decision| decision.action)
                .collect::<Vec<_>>(),
            vec![
                BaseReconciliationAction::AlreadyCorrect,
                BaseReconciliationAction::MissingOpenPr,
            ]
        );
    }

    #[test]
    fn verification_ignores_missing_prs_that_were_not_edited() {
        let desired =
            build_desired_pr_base_chain("main", &groups(&["alpha", "beta"]), "spr/").unwrap();
        let observed = ObservedPrBaseChain::from_open_prs(vec![pr(1, "spr/alpha", "main")]);
        let decisions = plan_base_reconciliation(&desired, &observed);

        verify_base_edits_converged(&["spr/alpha".to_string()], &decisions).unwrap();
    }

    #[test]
    fn verification_rejects_edited_prs_that_stay_pending() {
        let desired =
            build_desired_pr_base_chain("main", &groups(&["alpha", "beta"]), "spr/").unwrap();
        let observed = ObservedPrBaseChain::from_open_prs(vec![pr(1, "spr/alpha", "other")]);
        let decisions = plan_base_reconciliation(&desired, &observed);

        let err = verify_base_edits_converged(&["spr/alpha".to_string()], &decisions).unwrap_err();

        assert_eq!(
            err.to_string(),
            "GitHub PR base chain did not converge after update: spr/alpha: other -> main"
        );
    }

    #[test]
    fn verification_rejects_edited_prs_that_disappear() {
        let desired = build_desired_pr_base_chain("main", &groups(&["alpha"]), "spr/").unwrap();
        let observed = ObservedPrBaseChain::default();
        let decisions = plan_base_reconciliation(&desired, &observed);

        let err = verify_base_edits_converged(&["spr/alpha".to_string()], &decisions).unwrap_err();

        assert_eq!(
            err.to_string(),
            "GitHub PR base chain did not converge after update: spr/alpha: <missing> -> main"
        );
    }
}
