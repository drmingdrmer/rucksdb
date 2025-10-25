use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Code {
    Ok,
    NotFound,
    Corruption,
    NotSupported,
    InvalidArgument,
    IOError,
    MergeInProgress,
    Incomplete,
    ShutdownInProgress,
    TimedOut,
    Aborted,
    Busy,
    Expired,
    TryAgain,
}

#[derive(Debug, Clone)]
pub struct Status {
    code: Code,
    message: Option<String>,
}

impl Status {
    pub fn ok() -> Self {
        Status {
            code: Code::Ok,
            message: None,
        }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Status {
            code: Code::NotFound,
            message: Some(msg.into()),
        }
    }

    pub fn corruption(msg: impl Into<String>) -> Self {
        Status {
            code: Code::Corruption,
            message: Some(msg.into()),
        }
    }

    pub fn not_supported(msg: impl Into<String>) -> Self {
        Status {
            code: Code::NotSupported,
            message: Some(msg.into()),
        }
    }

    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Status {
            code: Code::InvalidArgument,
            message: Some(msg.into()),
        }
    }

    pub fn io_error(msg: impl Into<String>) -> Self {
        Status {
            code: Code::IOError,
            message: Some(msg.into()),
        }
    }

    pub fn busy(msg: impl Into<String>) -> Self {
        Status {
            code: Code::Busy,
            message: Some(msg.into()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.code == Code::Ok
    }

    pub fn is_not_found(&self) -> bool {
        self.code == Code::NotFound
    }

    pub fn is_corruption(&self) -> bool {
        self.code == Code::Corruption
    }

    pub fn is_io_error(&self) -> bool {
        self.code == Code::IOError
    }

    pub fn code(&self) -> &Code {
        &self.code
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(msg) => write!(f, "{:?}: {}", self.code, msg),
            None => write!(f, "{:?}", self.code),
        }
    }
}

impl std::error::Error for Status {}

impl From<std::io::Error> for Status {
    fn from(err: std::io::Error) -> Self {
        Status::io_error(err.to_string())
    }
}

impl From<serde_json::Error> for Status {
    fn from(err: serde_json::Error) -> Self {
        Status::corruption(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Status>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_ok() {
        let status = Status::ok();
        assert!(status.is_ok());
        assert_eq!(status.code(), &Code::Ok);
    }

    #[test]
    fn test_status_not_found() {
        let status = Status::not_found("key not found");
        assert!(status.is_not_found());
        assert_eq!(status.message(), Some("key not found"));
    }

    #[test]
    fn test_status_display() {
        let status = Status::io_error("disk full");
        assert_eq!(status.to_string(), "IOError: disk full");
    }
}
