//! Process exit codes (ARCHITECTURE.md section 11) and how errors map to them.

use thadam_core::{ErrorDomain, ThadamError, codes};

/// How a pipeline run ended, as a process exit code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ExitStatus {
    /// Clean shutdown, every component stopped normally.
    Ok,
    /// A component failed or the runtime hit an unrecoverable error.
    RuntimeFailure,
    /// The configuration is invalid (including connector configuration).
    InvalidConfig,
    /// The data directory is locked by another process or is incompatible.
    DataDir,
    /// The shutdown deadline passed or a second signal arrived.
    Interrupted,
}

impl ExitStatus {
    /// The numeric process exit code.
    pub const fn code(self) -> u8 {
        match self {
            ExitStatus::Ok => 0,
            ExitStatus::RuntimeFailure => 1,
            ExitStatus::InvalidConfig => 2,
            ExitStatus::DataDir => 3,
            ExitStatus::Interrupted => 130,
        }
    }

    /// The exit status for an error that stopped a run before or while it
    /// started.
    pub fn for_error(err: &ThadamError) -> ExitStatus {
        match err.code {
            codes::BUFFER_INCOMPATIBLE | codes::CONFIG_INCOMPATIBLE => ExitStatus::DataDir,
            _ if err.domain == ErrorDomain::Config => ExitStatus::InvalidConfig,
            _ => ExitStatus::RuntimeFailure,
        }
    }

    /// The exit status for the errors [`Pipeline::build`](crate::Pipeline::build)
    /// reported: the most specific one wins (data directory, then config).
    pub fn for_errors(errs: &[ThadamError]) -> ExitStatus {
        let all: Vec<ExitStatus> = errs.iter().map(ExitStatus::for_error).collect();
        [ExitStatus::DataDir, ExitStatus::InvalidConfig]
            .into_iter()
            .find(|s| all.contains(s))
            .unwrap_or(ExitStatus::RuntimeFailure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_match_the_architecture() {
        let all = [
            (ExitStatus::Ok, 0),
            (ExitStatus::RuntimeFailure, 1),
            (ExitStatus::InvalidConfig, 2),
            (ExitStatus::DataDir, 3),
            (ExitStatus::Interrupted, 130),
        ];
        for (status, code) in all {
            assert_eq!(status.code(), code);
        }
    }

    #[test]
    fn errors_map_to_exit_codes() {
        let locked = ThadamError::fatal(ErrorDomain::Buffer, codes::BUFFER_INCOMPATIBLE, "locked");
        assert_eq!(ExitStatus::for_error(&locked), ExitStatus::DataDir);
        let bad = ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, "bad");
        assert_eq!(ExitStatus::for_error(&bad), ExitStatus::InvalidConfig);
        let io = ThadamError::fatal(ErrorDomain::Buffer, codes::BUFFER_IO, "eio");
        assert_eq!(ExitStatus::for_error(&io), ExitStatus::RuntimeFailure);

        assert_eq!(
            ExitStatus::for_errors(&[io.clone(), bad.clone()]),
            ExitStatus::InvalidConfig
        );
        assert_eq!(ExitStatus::for_errors(&[bad, locked]), ExitStatus::DataDir);
        assert_eq!(ExitStatus::for_errors(&[io]), ExitStatus::RuntimeFailure);
    }
}
