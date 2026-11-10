//! Ballista Kubernetes operator.
//!
//!   ballista-operator crd    # print the CustomResourceDefinitions (YAML)
//!   ballista-operator run    # reconcile BallistaCluster and BallistaApplication
//!
//! Runs in the cluster with its service account, or from a laptop with the
//! current kubeconfig (it reaches schedulers through the API server's service proxy).

mod application;
mod autoscale;
mod cluster;
mod crd;
mod resources;
mod security;

use std::sync::Arc;

use clap::{Parser, Subcommand};
use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::{Controller, watcher};
use kube::{Api, Client, CustomResourceExt};

use crate::crd::{BallistaApplication, BallistaCluster};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kubernetes API: {0}")]
    Kube(#[from] kube::Error),
    #[error("serialization: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Invalid(String),
}

pub struct Ctx {
    pub client: Client,
    /// Image for driver containers when an application names none.
    pub driver_image: String,
}

#[derive(Parser)]
#[command(
    version,
    about = "Kubernetes operator for Ballista clusters and applications"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the CustomResourceDefinitions as YAML.
    Crd,
    /// Run the controllers.
    Run {
        /// Watch only this namespace (default: all namespaces).
        #[arg(long, env = "WATCH_NAMESPACE")]
        namespace: Option<String>,
        /// Default driver image (Python with pyspark-client and /opt/ballista/run.py).
        #[arg(
            long,
            env = "BALLISTA_DRIVER_IMAGE",
            default_value = "localhost/ballista-pyspark:dev"
        )]
        driver_image: String,
    },
}

fn api<K>(client: &Client, ns: &Option<String>) -> Api<K>
where
    K: kube::Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    K::DynamicType: Default,
{
    match ns {
        Some(ns) => Api::namespaced(client.clone(), ns),
        None => Api::all(client.clone()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info,kube_runtime=warn"),
    )
    .init();
    match Args::parse().command {
        Command::Crd => {
            print!(
                "{}---\n{}",
                serde_yaml::to_string(&BallistaCluster::crd())?,
                serde_yaml::to_string(&BallistaApplication::crd())?
            );
            Ok(())
        }
        Command::Run {
            namespace,
            driver_image,
        } => {
            // Both rustls backends are linked (via the workspace); pick one explicitly.
            // Err means a provider is already installed, which is fine.
            let _ = rustls::crypto::ring::default_provider().install_default();
            let client = Client::try_default().await?;
            log::info!(
                "ballista-operator {} watching {}",
                env!("CARGO_PKG_VERSION"),
                namespace.as_deref().unwrap_or("all namespaces")
            );
            let ctx = Arc::new(Ctx {
                client: client.clone(),
                driver_image,
            });
            let owned = || {
                watcher::Config::default().labels("app.kubernetes.io/managed-by=ballista-operator")
            };
            let clusters = Controller::new(
                api::<BallistaCluster>(&client, &namespace),
                watcher::Config::default(),
            )
            .owns(api::<Deployment>(&client, &namespace), owned())
            .owns(api::<Pod>(&client, &namespace), owned())
            .shutdown_on_signal()
            .run(cluster::reconcile, cluster::error_policy, ctx.clone())
            .for_each(|r| async move {
                if let Err(e) = r {
                    log::debug!("cluster controller: {e}");
                }
            });
            let apps = Controller::new(
                api::<BallistaApplication>(&client, &namespace),
                watcher::Config::default(),
            )
            .owns(api::<Pod>(&client, &namespace), owned())
            .owns(
                api::<BallistaCluster>(&client, &namespace),
                watcher::Config::default(),
            )
            .shutdown_on_signal()
            .run(application::reconcile, application::error_policy, ctx)
            .for_each(|r| async move {
                if let Err(e) = r {
                    log::debug!("application controller: {e}");
                }
            });
            tokio::join!(clusters, apps);
            Ok(())
        }
    }
}
