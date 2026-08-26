use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

pub const DEFAULT_DIR: &str = "catalog";
pub const HANDSHAKE_VERSION: u32 = 1;
pub const PROTOCOL_VERSION: u32 = 1;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

pub fn dir() -> PathBuf {
    std::env::var_os("STRATA_CATALOG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DIR))
}

pub fn binary(dir: &Path, backend: &str) -> Option<PathBuf> {
    let path = dir.join(format!("strata-{backend}"));
    path.is_file().then_some(path)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub handshake: u32,
    pub protocol: u32,
    pub network: String,
    pub address: String,
}

impl Handshake {
    pub fn new(network: impl Into<String>, address: impl Into<String>) -> Self {
        Handshake {
            handshake: HANDSHAKE_VERSION,
            protocol: PROTOCOL_VERSION,
            network: network.into(),
            address: address.into(),
        }
    }

    pub fn parse(line: &str) -> Result<Self> {
        let parts: Vec<&str> = line.trim().split('|').collect();
        let [handshake, protocol, network, address] = parts.as_slice() else {
            bail!("expected `<handshake>|<protocol>|<network>|<address>`");
        };
        let handshake: u32 = handshake
            .parse()
            .with_context(|| format!("handshake version `{handshake}`"))?;
        if handshake != HANDSHAKE_VERSION {
            bail!("unsupported handshake version `{handshake}`, expected {HANDSHAKE_VERSION}");
        }
        let protocol: u32 = protocol
            .parse()
            .with_context(|| format!("protocol version `{protocol}`"))?;
        if protocol != PROTOCOL_VERSION {
            bail!("provider speaks protocol `{protocol}`, this host speaks {PROTOCOL_VERSION}");
        }
        Ok(Handshake {
            handshake,
            protocol,
            network: network.to_string(),
            address: address.to_string(),
        })
    }

    pub fn endpoint(&self) -> Result<String> {
        match self.network.as_str() {
            "tcp" => Ok(format!("http://{}", self.address)),
            other => bail!("unsupported provider network `{other}`"),
        }
    }
}

impl fmt::Display for Handshake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.handshake, self.protocol, self.network, self.address
        )
    }
}

pub struct Process {
    _child: Child,
}

pub async fn spawn(binary: &Path) -> Result<(Process, Handshake)> {
    let mut child = Command::new(binary)
        .arg("--addr")
        .arg("127.0.0.1:0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawning provider `{}`", binary.display()))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("provider `{}` has no stdout", binary.display()))?;
    let mut lines = BufReader::new(stdout).lines();

    let announced = tokio::time::timeout(STARTUP_TIMEOUT, lines.next_line())
        .await
        .map_err(|_| {
            anyhow!(
                "provider `{}` did not announce itself within {STARTUP_TIMEOUT:?}",
                binary.display()
            )
        })?
        .with_context(|| format!("reading from provider `{}`", binary.display()))?
        .ok_or_else(|| {
            anyhow!(
                "provider `{}` exited without announcing itself",
                binary.display()
            )
        })?;

    let handshake = Handshake::parse(&announced)
        .with_context(|| format!("provider `{}` announced `{announced}`", binary.display()))?;
    Ok((Process { _child: child }, handshake))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_well_formed_handshake() {
        let line = Handshake::new("tcp", "127.0.0.1:41234").to_string();
        let parsed = Handshake::parse(&line).unwrap();
        assert_eq!(parsed.endpoint().unwrap(), "http://127.0.0.1:41234");
    }

    #[test]
    fn rejects_a_version_mismatch() {
        let stale = format!("{HANDSHAKE_VERSION}|{}|tcp|127.0.0.1:1", PROTOCOL_VERSION + 1);
        let error = Handshake::parse(&stale).unwrap_err().to_string();
        assert!(error.contains("this host speaks"), "got: {error}");
    }

    #[test]
    fn rejects_a_malformed_line() {
        assert!(Handshake::parse("hello").is_err());
        assert!(
            Handshake::parse("1|1|carrier-pigeon|somewhere")
                .unwrap()
                .endpoint()
                .is_err()
        );
    }
}
