use thiserror::Error;

use super::session::SessionError;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Invalid argument provided to client")]
    InvalidArg,

    #[error("Invalid response from server")]
    InvalidResponse,

    #[error(transparent)]
    Session(#[from] SessionError),
}
