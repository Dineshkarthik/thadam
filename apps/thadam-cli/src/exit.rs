//! Process exit codes (ARCHITECTURE.md section 11).

use std::process::ExitCode;

/// How a command ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exit {
    /// The command succeeded.
    Ok,
    /// A runtime failure.
    RuntimeFailure,
    /// The configuration is invalid, or the command line was.
    InvalidConfig,
    /// The data directory is locked or has an incompatible format.
    #[allow(dead_code)]
    DataDir,
    /// Interrupted by a signal.
    #[allow(dead_code)]
    Interrupted,
}

impl Exit {
    /// The numeric code.
    pub const fn code(self) -> u8 {
        match self {
            Exit::Ok => 0,
            Exit::RuntimeFailure => 1,
            Exit::InvalidConfig => 2,
            Exit::DataDir => 3,
            Exit::Interrupted => 130,
        }
    }
}

impl From<Exit> for ExitCode {
    fn from(exit: Exit) -> Self {
        ExitCode::from(exit.code())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_match_the_architecture() {
        assert_eq!(Exit::Ok.code(), 0);
        assert_eq!(Exit::RuntimeFailure.code(), 1);
        assert_eq!(Exit::InvalidConfig.code(), 2);
        assert_eq!(Exit::DataDir.code(), 3);
        assert_eq!(Exit::Interrupted.code(), 130);
    }
}
