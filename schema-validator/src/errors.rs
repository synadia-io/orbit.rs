use std::fmt::{Debug, Display};

pub type GenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The error type for the NATS client, generic by the kind of error.
#[derive(Debug)]
pub struct Error<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    pub(crate) kind: Kind,
    pub(crate) source: Option<GenericError>,
}

impl<Kind> Error<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    pub(crate) fn new(kind: Kind) -> Self {
        Self { kind, source: None }
    }

    pub(crate) fn with_source<S>(kind: Kind, source: S) -> Self
    where
        S: Into<GenericError>,
    {
        Self {
            kind,
            source: Some(source.into()),
        }
    }

    // In some cases the kind doesn't implement `Copy` trait
    pub fn kind(&self) -> Kind {
        self.kind.clone()
    }
}

impl<Kind> Display for Error<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(err) = &self.source {
            write!(f, "{}: {}", self.kind, err)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

impl<Kind> std::error::Error for Error<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|boxed| boxed.as_ref() as &(dyn std::error::Error + 'static))
    }
}

impl<Kind> From<Kind> for Error<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    fn from(kind: Kind) -> Self {
        Self { kind, source: None }
    }
}
