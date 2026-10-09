//! Where a result was produced: source revision, toolchain, OS, and hardware.
//! Every probe is best effort; a field that cannot be read says so instead of
//! failing the run, because a result without metadata must never be quoted.

use std::path::Path;
use std::process::Command;

use serde::{Deserialize, Serialize};

/// The machine and source tree a benchmark ran on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Environment {
    /// Short commit SHA of the workspace, or `unknown` outside a git checkout.
    pub git_sha: String,
    /// Whether the workspace had uncommitted changes; `None` when unknown.
    pub git_dirty: Option<bool>,
    /// `rustc --version` output.
    pub rustc: String,
    /// The OS family, e.g. `macos` or `linux`.
    pub os: String,
    /// `uname -sr` output, or `unknown`.
    pub kernel: String,
    /// The CPU architecture, e.g. `aarch64`.
    pub arch: String,
    /// The CPU model string, or `unknown`.
    pub cpu_model: String,
    /// Logical cores available to the process.
    pub cpu_cores: usize,
}

impl Environment {
    /// Probes the current machine. `root` is the workspace root for git.
    #[must_use]
    pub fn collect(root: &Path) -> Environment {
        Environment {
            git_sha: git_output(root, &["rev-parse", "--short", "HEAD"])
                .unwrap_or_else(|| "unknown".to_owned()),
            git_dirty: git_output(root, &["status", "--porcelain"]).map(|s| !s.is_empty()),
            rustc: command_output("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_owned()),
            os: std::env::consts::OS.to_owned(),
            kernel: command_output("uname", &["-sr"]).unwrap_or_else(|| "unknown".to_owned()),
            arch: std::env::consts::ARCH.to_owned(),
            cpu_model: cpu_model(),
            cpu_cores: std::thread::available_parallelism().map_or(0, usize::from),
        }
    }
}

fn git_output(root: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    (!text.is_empty()).then_some(text)
}

/// `sysctl` on macOS (it can refuse inside sandboxes, hence the fallback),
/// `/proc/cpuinfo` on Linux, otherwise `unknown`.
fn cpu_model() -> String {
    if let Some(brand) = command_output("sysctl", &["-n", "machdep.cpu.brand_string"]) {
        return brand;
    }
    if let Ok(info) = std::fs::read_to_string("/proc/cpuinfo")
        && let Some(model) = parse_cpuinfo_model(&info)
    {
        return model;
    }
    "unknown".to_owned()
}

/// The first `model name` (x86) or `Hardware`/`Model` (ARM) line.
#[must_use]
pub fn parse_cpuinfo_model(info: &str) -> Option<String> {
    info.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        let key = key.trim();
        (key == "model name" || key == "Hardware" || key == "Model")
            .then(|| value.trim().to_owned())
            .filter(|v| !v.is_empty())
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn cpuinfo_model_is_read_from_x86_and_arm_layouts() {
        let x86 = "processor\t: 0\nmodel name\t: Intel(R) Xeon(R) CPU\nflags\t: fpu\n";
        assert_eq!(
            parse_cpuinfo_model(x86).as_deref(),
            Some("Intel(R) Xeon(R) CPU")
        );
        let arm = "processor : 0\nModel\t: Raspberry Pi 5\n";
        assert_eq!(parse_cpuinfo_model(arm).as_deref(), Some("Raspberry Pi 5"));
        assert_eq!(parse_cpuinfo_model("processor : 0\n"), None);
    }

    #[test]
    fn collect_always_fills_the_cheap_fields() {
        let env = Environment::collect(Path::new("/nonexistent-thadam-bench-root"));
        assert_eq!(env.git_sha, "unknown");
        assert_eq!(env.git_dirty, None);
        assert!(env.cpu_cores >= 1);
        assert!(!env.os.is_empty());
    }
}
