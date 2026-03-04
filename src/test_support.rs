use std::env;
use std::path::{Path, PathBuf};
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
