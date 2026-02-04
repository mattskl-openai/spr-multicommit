# Agent Expectations

If an agent modifies code in this repository, it must complete the following before ending its turn:

1. Run `cargo fmt`.
2. Run `cargo clippy` and resolve all reported issues.
3. Run `cargo test`.
4. Fix any test failures introduced by the agent's changes.

Handling pre-existing test failures:

- If tests were already failing before the agent started, the agent should call that out explicitly.
- In that case, the agent must ask the user whether those pre-existing failures should be fixed now or left as-is.
- The "must fix" requirement applies to newly introduced failures, not historical ones.
