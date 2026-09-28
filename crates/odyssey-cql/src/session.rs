use std::collections::HashMap;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::NodeAddr;
use scylla::cluster::metadata::Peer;
use scylla::errors::TranslationError;
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::policies::host_filter::HostFilter;

use odyssey_core::{AddressTranslation, SourceConfig, TargetConfig};

use crate::CqlError;

/// Thin wrapper so call sites do not depend on the driver type directly.
pub struct CqlSession {
    inner: Arc<Session>,
}

impl CqlSession {
    pub fn inner(&self) -> &Session {
        &self.inner
    }

    /// Shared session handle for crates that need `Arc<Session>` (CDC reader).
    pub fn shared(&self) -> Arc<Session> {
        Arc::clone(&self.inner)
    }
}

/// Connect to the migration source cluster.
pub async fn connect_source(config: &SourceConfig) -> Result<CqlSession, CqlError> {
    connect(
        &config.contact_points,
        config.username.as_deref(),
        config.password.as_deref(),
        config.datacenter.as_deref(),
        config.contact_points_only,
        &config.address_translations,
    )
    .await
}

/// Connect to the migration target cluster.
pub async fn connect_target(config: &TargetConfig) -> Result<CqlSession, CqlError> {
    connect(
        &config.contact_points,
        config.username.as_deref(),
        config.password.as_deref(),
        config.datacenter.as_deref(),
        config.contact_points_only,
        &config.address_translations,
    )
    .await
}

async fn connect(
    contact_points: &[String],
    username: Option<&str>,
    password: Option<&str>,
    datacenter: Option<&str>,
    contact_points_only: bool,
    address_translations: &[AddressTranslation],
) -> Result<CqlSession, CqlError> {
    if contact_points.is_empty() {
        return Err(CqlError::Invalid(
            "contact_points must not be empty".into(),
        ));
    }

    let tunnel_mode = contact_points_only || !address_translations.is_empty();
    let contact_ports = resolve_contact_ports(contact_points)?;

    let mut builder = SessionBuilder::new().known_nodes(contact_points);

    // SSH LocalForward only exposes the mapped native port. Shard-aware ports break tunnels.
    if tunnel_mode {
        builder = builder.disallow_shard_aware_port(true);
        tracing::info!("disallow_shard_aware_port=true (tunnel / contact_points_only)");
    }

    if !tunnel_mode {
        if let Some(dc) = datacenter.filter(|d| !d.is_empty()) {
            builder = builder.prefer_datacenter(dc.to_string());
        }
    } else {
        builder = builder.prefer_no_datacenter();
    }

    match (username, password) {
        (Some(user), Some(pass)) => {
            builder = builder.user(user, pass);
        }
        (None, None) => {}
        _ => {
            return Err(CqlError::Invalid(
                "username and password must both be set, or both omitted".into(),
            ));
        }
    }

    if !address_translations.is_empty() {
        let translator =
            TunnelAddressTranslator::from_config(address_translations, &contact_ports)?;
        tracing::info!(
            rules = address_translations.len(),
            "installing tunnel address translator"
        );
        builder = builder.address_translator(Arc::new(translator));
    } else if contact_points_only {
        // Docker Desktop / single published port: peers advertise container IPs
        // that the host cannot dial. Pin every peer to the known contact point(s).
        let pin = PinToContactsTranslator::from_contacts(contact_points)?;
        tracing::info!(
            contacts = pin.contacts.len(),
            "contact_points_only: pinning peer addresses to contact points"
        );
        builder = builder.address_translator(Arc::new(pin));
    }

    if contact_points_only && !address_translations.is_empty() {
        let filter = TunnelHostFilter::new(contact_points, address_translations, &contact_ports)?;
        builder = builder.host_filter(Arc::new(filter));
        tracing::info!(
            contact_points = ?contact_points,
            "contact_points_only: TunnelHostFilter active"
        );
    }

    tracing::info!(
        contact_points = ?contact_points,
        authenticated = username.is_some(),
        contact_points_only,
        address_translations = address_translations.len(),
        datacenter = ?datacenter,
        "opening CQL session"
    );

    let session = builder.build().await.map_err(|err| {
        tracing::error!(error = %err, "CQL session failed to open");
        CqlError::Connect(err.to_string())
    })?;

    tracing::info!("CQL session established");
    Ok(CqlSession {
        inner: Arc::new(session),
    })
}

fn resolve_contact_ports(contact_points: &[String]) -> Result<HashSet<u16>, CqlError> {
    let mut ports = HashSet::new();
    for cp in contact_points {
        for addr in cp
            .to_socket_addrs()
            .map_err(|err| CqlError::Invalid(format!("resolve `{cp}`: {err}")))?
        {
            ports.insert(addr.port());
        }
    }
    Ok(ports)
}

/// Keep contact points and peers that map to our tunnel (by IP + contact port).
struct TunnelHostFilter {
    /// Exact socket addrs (contact points + translation targets).
    allowed_addrs: HashSet<SocketAddr>,
    /// Broadcast IPs we accept on any contact-point port (driver copies tunnel port).
    allowed_ips: HashSet<IpAddr>,
    contact_ports: HashSet<u16>,
}

impl TunnelHostFilter {
    fn new(
        contact_points: &[String],
        translations: &[AddressTranslation],
        contact_ports: &HashSet<u16>,
    ) -> Result<Self, CqlError> {
        let mut allowed_addrs = HashSet::new();
        let mut allowed_ips = HashSet::new();

        for cp in contact_points {
            for addr in cp
                .to_socket_addrs()
                .map_err(|err| CqlError::Invalid(format!("resolve `{cp}`: {err}")))?
            {
                allowed_addrs.insert(addr);
            }
        }

        for rule in translations {
            let from = parse_socket(&rule.from, "address_translations.from")?;
            let to = parse_socket(&rule.to, "address_translations.to")?;
            allowed_addrs.insert(from);
            allowed_addrs.insert(to);
            allowed_ips.insert(from.ip());

            // Driver may rewrite peer ports to match the local contact-point port.
            for port in contact_ports {
                allowed_addrs.insert(SocketAddr::new(from.ip(), *port));
            }
        }

        Ok(Self {
            allowed_addrs,
            allowed_ips,
            contact_ports: contact_ports.clone(),
        })
    }

    fn accepts_translatable(&self, addr: SocketAddr) -> bool {
        if self.allowed_addrs.contains(&addr) {
            return true;
        }
        self.allowed_ips.contains(&addr.ip()) && self.contact_ports.contains(&addr.port())
    }
}

impl HostFilter for TunnelHostFilter {
    fn accept(&self, peer: &Peer) -> bool {
        match peer.address {
            NodeAddr::Untranslatable(_) => true,
            NodeAddr::Translatable(addr) => self.accepts_translatable(addr),
            _ => false,
        }
    }
}

/// Maps every discovered peer to a configured contact point (Docker / single-port).
struct PinToContactsTranslator {
    contacts: Vec<SocketAddr>,
}

impl PinToContactsTranslator {
    fn from_contacts(contact_points: &[String]) -> Result<Self, CqlError> {
        let mut contacts = Vec::new();
        for cp in contact_points {
            for addr in cp
                .to_socket_addrs()
                .map_err(|err| CqlError::Invalid(format!("resolve `{cp}`: {err}")))?
            {
                contacts.push(addr);
            }
        }
        if contacts.is_empty() {
            return Err(CqlError::Invalid(
                "contact_points_only could not resolve any contact socket".into(),
            ));
        }
        Ok(Self { contacts })
    }
}

#[async_trait]
impl AddressTranslator for PinToContactsTranslator {
    async fn translate_address(
        &self,
        _untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        Ok(self.contacts[0])
    }
}

/// Maps broadcast IPs from translation rules to the tunnel endpoint.
/// Matches any port on those IPs (driver assigns the contact-point port).
struct TunnelAddressTranslator {
    by_ip: HashMap<IpAddr, SocketAddr>,
}

impl TunnelAddressTranslator {
    fn from_config(
        rules: &[AddressTranslation],
        contact_ports: &HashSet<u16>,
    ) -> Result<Self, CqlError> {
        let mut by_ip = HashMap::new();
        for rule in rules {
            let from = parse_socket(&rule.from, "address_translations.from")?;
            let to = parse_socket(&rule.to, "address_translations.to")?;
            by_ip.insert(from.ip(), to);
            for port in contact_ports {
                by_ip.insert(from.ip(), SocketAddr::new(to.ip(), *port));
            }
        }
        Ok(Self { by_ip })
    }
}

#[async_trait]
impl AddressTranslator for TunnelAddressTranslator {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        let addr = untranslated_peer.untranslated_address();
        Ok(self
            .by_ip
            .get(&addr.ip())
            .copied()
            .unwrap_or(addr))
    }
}

fn parse_socket(raw: &str, field: &str) -> Result<SocketAddr, CqlError> {
    SocketAddr::from_str(raw).map_err(|err| CqlError::Invalid(format!("{field} `{raw}`: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_filter_accepts_broadcast_ip_on_tunnel_port() {
        let mut ports = HashSet::new();
        ports.insert(19_042);
        let filter = TunnelHostFilter::new(
            &["127.0.0.1:19042".to_string()],
            &[AddressTranslation {
                from: "10.108.0.86:9042".to_string(),
                to: "127.0.0.1:19042".to_string(),
            }],
            &ports,
        )
        .unwrap();

        let peer_addr: SocketAddr = "10.108.0.86:19042".parse().unwrap();
        assert!(filter.accepts_translatable(peer_addr));
    }

    #[test]
    fn pin_translator_resolves_localhost_contact() {
        let pin = PinToContactsTranslator::from_contacts(&["127.0.0.1:9042".to_string()]).unwrap();
        assert_eq!(pin.contacts[0].port(), 9042);
    }
}
