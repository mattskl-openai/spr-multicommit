use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

fn run_spr(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_spr"))
        .args(args)
        .output()
        .unwrap()
}

fn stdout_json(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn init_detached_update_repo() -> (TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    fs::create_dir(&repo).unwrap();
    git(&repo, ["init", "-b", "main"].as_slice());
    git(
        &repo,
        ["config", "user.email", "spr@example.com"].as_slice(),
    );
    git(&repo, ["config", "user.name", "SPR Tests"].as_slice());
    fs::write(repo.join("README.md"), "init\n").unwrap();
    git(&repo, ["add", "README.md"].as_slice());
    git(&repo, ["commit", "-m", "init"].as_slice());
    let origin = dir.path().join("origin.git");
    git(
        &repo,
        ["init", "--bare", origin.to_str().unwrap()].as_slice(),
    );
    git(
        &repo,
        ["remote", "add", "origin", origin.to_str().unwrap()].as_slice(),
    );
    git(&repo, ["push", "-u", "origin", "main"].as_slice());
    git(&repo, ["checkout", "-b", "stack"].as_slice());
    fs::write(repo.join("alpha.txt"), "alpha\n").unwrap();
    git(&repo, ["add", "alpha.txt"].as_slice());
    git(
        &repo,
        ["commit", "-m", "feat: alpha start pr:alpha"].as_slice(),
    );
    git(&repo, ["checkout", "--detach", "HEAD"].as_slice());
    (dir, repo)
}

fn run_detached_update(repo: &Path, home: &Path, json: bool) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_spr"));
    command
        .env("HOME", home)
        .args(["--cd", repo.to_str().unwrap(), "--base", "main"])
        .args(["--prefix", "test-spr/", "update", "--no-pr"]);
    if json {
        command.arg("--json");
    }
    command.output().unwrap()
}

fn assert_alpha_remote_exists(repo: &Path) {
    assert!(!git(
        repo,
        ["ls-remote", "--heads", "origin", "test-spr/alpha"].as_slice(),
    )
    .trim()
    .is_empty());
}

#[test]
fn json_help_outputs_structured_root_help() {
    let output = run_spr(&["--json", "--help"]);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let json = stdout_json(&output);
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["command"], "help");
    assert_eq!(json["result"], "help");
    assert_eq!(json["data"]["command_path"], serde_json::json!(["spr"]));
    assert!(json["data"]["options"]
        .as_array()
        .unwrap()
        .iter()
        .any(|option| option["long"] == "json" && option["global"] == true));
    assert!(!json["data"]["rendered_text"].as_str().unwrap().is_empty());
}

#[test]
fn json_help_outputs_structured_nested_help() {
    for args in [
        ["--json", "help", "list", "commit"],
        ["list", "commit", "--help", "--json"],
    ] {
        let output = run_spr(&args);

        assert!(output.status.success(), "args failed: {args:?}");
        assert!(
            output.stderr.is_empty(),
            "stderr for {args:?} was not empty"
        );
        let json = stdout_json(&output);
        assert_eq!(json["command"], "help");
        assert_eq!(json["result"], "help");
        assert_eq!(
            json["data"]["command_path"],
            serde_json::json!(["spr", "list", "commit"])
        );
        assert!(!json["data"]["options"].as_array().unwrap().is_empty());
        assert!(!json["data"]["rendered_text"].as_str().unwrap().is_empty());
    }
}

#[test]
fn json_version_outputs_package_metadata() {
    for args in [["--json", "--version"], ["--version", "--json"]] {
        let output = run_spr(&args);

        assert!(output.status.success(), "args failed: {args:?}");
        assert!(
            output.stderr.is_empty(),
            "stderr for {args:?} was not empty"
        );
        let json = stdout_json(&output);
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["command"], "version");
        assert_eq!(json["result"], "version");
        assert_eq!(json["data"]["name"], "spr");
        assert_eq!(json["data"]["version"], env!("CARGO_PKG_VERSION"));
    }
}

#[test]
fn json_parse_error_outputs_error_object() {
    let output = run_spr(&["--json", "definitely-not-a-command"]);

    assert!(!output.status.success());
    assert!(output.stderr.is_empty());
    let json = stdout_json(&output);
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["command"], "cli");
    assert_eq!(json["result"], "error");
    assert_eq!(json["error_kind"], "invalid_arguments");
    assert!(json["message"]
        .as_str()
        .unwrap()
        .contains("definitely-not-a-command"));
}

#[test]
fn detached_update_human_warns_after_pushing_branch() {
    let (dir, repo) = init_detached_update_repo();

    let output = run_detached_update(&repo, dir.path(), false);

    assert!(output.status.success());
    assert_alpha_remote_exists(&repo);
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("Skipping stack metadata refresh because HEAD is detached"));
}

#[test]
fn detached_update_json_reports_warning_after_pushing_branch() {
    let (dir, repo) = init_detached_update_repo();

    let output = run_detached_update(&repo, dir.path(), true);

    assert!(output.status.success());
    assert_alpha_remote_exists(&repo);
    let json = stdout_json(&output);
    assert!(json["data"]["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|warning| warning
            .as_str()
            .unwrap()
            .contains("Skipping stack metadata refresh because HEAD is detached")));
}
