use serde::Deserialize;
use std::fmt;
use std::path::PathBuf;
use std::{fs, io, option};

#[derive(Debug, Default, Deserialize, PartialEq)]
pub struct Settings {
    pub description: String,
    pub url: String,
    pub socks_proxy: String,
    pub token: String,
    pub user: String,
    pub password: String,
    pub creds: String,
    pub nkey: String,
    pub cert: String,
    pub key: String,
    pub ca: String,
    pub nsc: String,
    pub jetstream_domain: String,
    pub jetstream_api_prefix: String,
    pub jetstream_event_prefix: String,
    pub inbox_prefix: String,
    pub user_jwt: String,
    pub color_scheme: String,
    #[serde(default)]
    pub tls_first: bool,
    pub windows_cert_store: String,
    pub windows_cert_match_by: String,
    pub windows_cert_match: String,
    pub windows_ca_certs_match: Option<Vec<String>>,
}

impl Settings {
    /// Reads `Settings` from provided `filename`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if `filename` does not exist or the could not be read.
    pub fn read_from_file(filename: PathBuf) -> Result<Self, Error> {
        let ctx_file =
            fs::File::open(filename).map_err(|e| Error::with_source(ErrorKind::IoError, e))?;
        serde_json::from_reader(ctx_file).map_err(|e| Error::with_source(ErrorKind::SerdeError, e))
    }
}

pub struct Context {
    name: String,
    settings: Settings,
}

pub type Error = async_nats::error::Error<ErrorKind>;

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorKind {
    ContextNameFileNotFound,
    ContextFileNotFound,
    URLNotFound,
    IoError,
    SerdeError,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ContextNameFileNotFound => write!(f, "context.txt  not found"),
            Self::ContextFileNotFound => write!(f, "context file not found"),
            Self::URLNotFound => write!(f, "url not found in context settings"),
            Self::IoError => write!(f, "io error"),
            Self::SerdeError => write!(f, "deserialization error"),
        }
    }
}

impl Context {
    /// Loads the selected NATS CLI Context.
    /// The selected Context name is read from `$HOME/.config/nats/context.txt`.
    /// Context Settings is read from `$HOME/.config/nats/context/{name}.json`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn connect() -> Result<(), Box<dyn std::error::Error>> {
    ///     let ctx = nats_context::Context::new()?;
    ///     let settings = ctx.settings();
    ///     let nc = ctx.options().await?.connect(&settings.url).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if context.txt file could not be loaded or the `Settings` for the selected
    /// context could not be loaded.
    pub fn new() -> Result<Self, Error> {
        let ctx_name_path =
            context_name_path().ok_or(Error::new(ErrorKind::ContextNameFileNotFound))?;
        let ctx_name = fs::read_to_string(ctx_name_path)
            .map_err(|e| Error::with_source(ErrorKind::IoError, e))?;
        Context::new_with_name(ctx_name.trim())
    }

    /// Loads a Context by name.
    /// Context Settings is read from `$HOME/.config/nats/context/{name}.json`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if context file does not exist or a `Context` could not be parsed.
    pub fn new_with_name(name: &str) -> Result<Self, Error> {
        let ctx_path = context_path(name).ok_or(Error::new(ErrorKind::ContextFileNotFound))?;
        let settings = Settings::read_from_file(ctx_path)?;
        Ok(Context {
            name: name.to_string(),
            settings,
        })
    }

    /// Constructs a `ConnectOptions` from parsed `Context` `Settings`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if `Settings.creds` is provided and the credential file could not be
    /// loaded.
    pub async fn options(&self) -> io::Result<async_nats::ConnectOptions> {
        let mut options = async_nats::ConnectOptions::new().name(&self.name);
        if !self.settings.token.is_empty() {
            options = options.token(self.settings.token.clone());
        }
        if !self.settings.user.is_empty() && !self.settings.password.is_empty() {
            options = options
                .user_and_password(self.settings.user.clone(), self.settings.password.clone());
        }
        if !self.settings.creds.is_empty() {
            options = options.credentials_file(&self.settings.creds).await?;
        }
        if !self.settings.nkey.is_empty() {
            options = options.nkey(self.settings.nkey.clone());
        }
        if !self.settings.inbox_prefix.is_empty() {
            options = options.custom_inbox_prefix(self.settings.inbox_prefix.clone());
        }
        if self.settings.tls_first {
            options = options.tls_first();
        }
        Ok(options)
    }

    /// Returns the parsed `Context` `Settings`.
    pub fn settings(&self) -> &Settings {
        &self.settings
    }
}

impl async_nats::ToServerAddrs for Context {
    type Iter = option::IntoIter<async_nats::ServerAddr>;

    /// Parses the `Context` URLs as `ServerAddrs`.
    fn to_server_addrs(&self) -> io::Result<Self::Iter> {
        self.settings
            .url
            .parse::<async_nats::ServerAddr>()
            .map(|addr| Some(addr).into_iter())
    }
}

/// Returns the root nats config directory
/// `$HOME/.config/nats` as a path.
/// If $HOME can't be resolved, None is returned.
fn nats_config_dir() -> Option<std::path::PathBuf> {
    let home_dir = std::env::home_dir()?;
    Some(home_dir.join(".config").join("nats"))
}

/// Returns `$HOME/.config/nats/context.txt` as a path.
/// If $HOME can't be resolved, None is returned.
/// `context.txt` contains the selected NATS CLI context name.
fn context_name_path() -> Option<std::path::PathBuf> {
    nats_config_dir()?.join("context.txt").into()
}

/// Returns `$HOME/.config/nats/context/{name}.json` as a path.
/// If $HOME can't be resolved, None is returned.
/// The JSON document contains NATS CLI context Settings.
fn context_path(name: &str) -> Option<std::path::PathBuf> {
    nats_config_dir()?
        .join("context")
        .join(format!("{name}.json"))
        .into()
}
