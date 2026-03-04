use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard};

static PROCESS_CWD_LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn lock_cwd() -> MutexGuard<'static, ()> {
    PROCESS_CWD_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) struct DirGuard {
    original: PathBuf,
}

impl DirGuard {
    pub(crate) fn change_to(path: &Path) -> Self {
        let original = env::current_dir().expect("current dir available");
        env::set_current_dir(path).expect("set current dir to temp repo");
        Self { original }
    }
}

impl Drop for DirGuard {
    fn drop(&mut self) {
        env::set_current_dir(&self.original).expect("restore original current dir");
    }
}

pub(crate) fn git(repo: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {:?} failed\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

pub(crate) fn write_file(repo: &Path, file: &str, contents: &str) {
    fs::write(repo.join(file), contents).expect("write file");
}

pub(crate) fn commit_file(repo: &Path, file: &str, contents: &str, message: &str) -> String {
    write_file(repo, file, contents);
    git(repo, ["add", file].as_slice());
    git(repo, ["commit", "-m", message].as_slice());
    git(repo, ["rev-parse", "HEAD"].as_slice())
        .trim()
        .to_string()
}

pub(crate) fn log_subjects(repo: &Path, count: usize) -> Vec<String> {
    git(
        repo,
        ["log", "--format=%s", &format!("-{}", count)].as_slice(),
    )
    .lines()
    .map(|line| line.to_string())
    .collect()
}
