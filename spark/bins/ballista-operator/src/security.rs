//! Per-cluster security material, the counterpart of Spark's `spark.authenticate`
//! secret and `spark.ssl.*` keystores: a Secret `<cluster>-security` holding a
//! CA certificate, the cluster certificate and key it signed, the web UI / REST
//! token and the Spark Connect token. Every component mounts it; the CA key is
//! discarded once the certificate is signed, so nobody can mint new members.
//! Rotate by deleting the Secret (the operator creates a new one) and the pods.

use std::collections::BTreeMap;

use chrono::{Datelike, Utc};
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::ByteString;
use kube::api::{Api, PostParams};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, date_time_ymd,
};

use crate::Error;
use crate::crd::BallistaClusterSpec;
use crate::resources as r;

pub const CA_KEY: &str = "ca.crt";
pub const CERT_KEY: &str = "tls.crt";
pub const KEY_KEY: &str = "tls.key";
pub const UI_TOKEN_KEY: &str = "ui-token";
pub const CONNECT_TOKEN_KEY: &str = "connect-token";
/// Name every cluster certificate carries; peers are verified against it
/// because executors are reached by pod IP.
pub const INTERNAL_NAME: &str = "ballista-internal";
const VALIDITY_YEARS: i32 = 10;

/// The Secret a cluster's pods mount, or `None` with security off.
pub fn secret_name(cluster: &str, spec: &BallistaClusterSpec) -> Option<String> {
    spec.security.enabled.then(|| {
        spec.security
            .secret_name
            .clone()
            .unwrap_or_else(|| format!("{cluster}-security"))
    })
}

/// Names a client may use for the cluster's services.
fn service_names(cluster: &str, ns: &str) -> Vec<String> {
    let mut names = vec![
        INTERNAL_NAME.to_string(),
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ];
    for svc in [r::scheduler_name(cluster), r::connect_name(cluster), r::history_name(cluster)] {
        names.push(format!("{svc}.{ns}.svc.cluster.local"));
        names.push(format!("{svc}.{ns}.svc"));
        names.push(format!("{svc}.{ns}"));
        names.push(svc);
    }
    names
}

fn cert_error(e: rcgen::Error) -> Error {
    Error::Invalid(format!("generating cluster certificates: {e}"))
}

/// A random token: 32 bytes, hex.
fn token() -> String {
    rand::random::<[u8; 32]>()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// PEM CA certificate, certificate and key for `cluster`.
pub fn generate_pki(cluster: &str, ns: &str) -> Result<(String, String, String), Error> {
    let now = Utc::now();
    let not_before = date_time_ymd(now.year(), now.month() as u8, now.day() as u8);
    let not_after = date_time_ymd(now.year() + VALIDITY_YEARS, now.month() as u8, 1);

    let ca_key = KeyPair::generate().map_err(cert_error)?;
    let mut ca = CertificateParams::new(Vec::<String>::new()).map_err(cert_error)?;
    ca.distinguished_name
        .push(DnType::CommonName, format!("Ballista cluster CA {ns}/{cluster}"));
    ca.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca.not_before = not_before;
    ca.not_after = not_after;
    let ca_pem = ca.self_signed(&ca_key).map_err(cert_error)?.pem();
    let issuer = Issuer::new(ca, ca_key);

    let key = KeyPair::generate().map_err(cert_error)?;
    let mut leaf = CertificateParams::new(service_names(cluster, ns)).map_err(cert_error)?;
    leaf.distinguished_name
        .push(DnType::CommonName, format!("{cluster}.{ns}"));
    leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    // One certificate for both ends of every connection (mutual TLS).
    leaf.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    leaf.not_before = not_before;
    leaf.not_after = not_after;
    let cert = leaf.signed_by(&key, &issuer).map_err(cert_error)?;
    Ok((ca_pem, cert.pem(), key.serialize_pem()))
}

/// A new Secret for `cluster`, owned by it.
pub fn generate(cluster: &str, ns: &str, owner: &OwnerReference) -> Result<Secret, Error> {
    let (ca, cert, key) = generate_pki(cluster, ns)?;
    let data = BTreeMap::from([
        (CA_KEY.to_string(), ByteString(ca.into_bytes())),
        (CERT_KEY.to_string(), ByteString(cert.into_bytes())),
        (KEY_KEY.to_string(), ByteString(key.into_bytes())),
        (UI_TOKEN_KEY.to_string(), ByteString(token().into_bytes())),
        (CONNECT_TOKEN_KEY.to_string(), ByteString(token().into_bytes())),
    ]);
    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(format!("{cluster}-security")),
            namespace: Some(ns.to_string()),
            labels: Some(r::labels(cluster, "security")),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        type_: Some("Opaque".to_string()),
        data: Some(data),
        ..Default::default()
    })
}

/// Makes sure the cluster's Secret exists (creating it unless the spec names
/// one) and returns the UI token the operator presents to the scheduler.
/// `None` with security off.
pub async fn ensure(
    secrets: &Api<Secret>,
    cluster: &str,
    ns: &str,
    spec: &BallistaClusterSpec,
    owner: &OwnerReference,
) -> Result<Option<String>, Error> {
    let Some(name) = secret_name(cluster, spec) else {
        return Ok(None);
    };
    let secret = match secrets.get_opt(&name).await? {
        Some(s) => s,
        None if spec.security.secret_name.is_some() => {
            return Err(Error::Invalid(format!(
                "security.secretName {name} does not exist"
            )));
        }
        None => {
            log::info!("{ns}/{cluster}: creating Secret {name} (cluster CA, certificate, tokens)");
            let secret = generate(cluster, ns, owner)?;
            match secrets.create(&PostParams::default(), &secret).await {
                Ok(s) => s,
                // Created meanwhile by another reconcile.
                Err(kube::Error::Api(e)) if e.code == 409 => secrets.get(&name).await?,
                Err(e) => return Err(e.into()),
            }
        }
    };
    let data = secret.data.unwrap_or_default();
    for key in [CA_KEY, CERT_KEY, KEY_KEY, UI_TOKEN_KEY, CONNECT_TOKEN_KEY] {
        if !data.contains_key(key) {
            return Err(Error::Invalid(format!("Secret {name} has no {key}")));
        }
    }
    let token = data
        .get(UI_TOKEN_KEY)
        .map(|b| String::from_utf8_lossy(&b.0).trim().to_string())
        .unwrap_or_default();
    Ok(Some(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certificate_names_cover_every_service() {
        let names = service_names("c1", "ns");
        for n in [
            INTERNAL_NAME,
            "localhost",
            "c1-scheduler",
            "c1-scheduler.ns.svc",
            "c1-connect.ns.svc.cluster.local",
            "c1-history.ns",
        ] {
            assert!(names.iter().any(|x| x == n), "missing {n}");
        }
    }

    #[test]
    fn generates_pem_and_distinct_tokens() -> Result<(), Error> {
        let (ca, cert, key) = generate_pki("c1", "ns")?;
        assert!(ca.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(cert.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(key.starts_with("-----BEGIN PRIVATE KEY-----"));
        let (a, b) = (token(), token());
        assert_eq!(a.len(), 64);
        assert_ne!(a, b);
        Ok(())
    }
}
