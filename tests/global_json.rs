use serde_json::Value;
use std::process::Command;

fn run_spr(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_spr"))
        .args(args)
        .output()
        .unwrap()
}

fn stdout_json(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap()
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
