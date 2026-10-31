use std::future::Future;
use std::sync::Arc;

use sail_common::actor::ActorSystem;
use sail_common::config::{AppConfig, GRPC_MAX_MESSAGE_LENGTH_DEFAULT};
use sail_common::runtime::RuntimeHandle;
use sail_common::server::ServerBuilder;
pub use sail_session::session_factory::{
    SessionJobRunner, SessionJobRunnerFactory, SessionJobRunnerInfo,
};
use sail_session::session_manager::SessionManager;
pub use sail_session::session_manager::{ServerSessionFactoryFn, SessionManagerOptions};
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

use crate::server::SparkConnectServer;
use crate::session_manager::{
    create_spark_session_factory, create_spark_session_manager_with_factories,
    create_spark_session_manager_with_factory,
};
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

/// The meat of the gRPC server.
pub async fn serve<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    serve_with_session_factory(
        listener,
        signal,
        config,
        runtime,
        create_spark_session_factory,
    )
    .await
}

/// The same as [`serve`], with the session factory chosen by the caller.
pub async fn serve_with_session_factory<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    session_factory_fn: ServerSessionFactoryFn,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let mut system = ActorSystem::new();
    let session_manager =
        create_spark_session_manager_with_factory(config, runtime, &mut system, session_factory_fn)
            .await?;
    serve_session_manager(listener, signal, session_manager, system).await
}

/// The same as [`serve_with_session_factory`], with the job runner factory
/// also chosen by the caller. Ballista uses this to execute Spark plans.
pub async fn serve_with_factories<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    session_factory_fn: ServerSessionFactoryFn,
    job_runner_factory: Box<dyn SessionJobRunnerFactory>,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let mut system = ActorSystem::new();
    let session_manager = create_spark_session_manager_with_factories(
        config,
        runtime,
        &mut system,
        session_factory_fn,
        job_runner_factory,
    )
    .await?;
    serve_session_manager(listener, signal, session_manager, system).await
}

async fn serve_session_manager<F>(
    listener: TcpListener,
    signal: F,
    session_manager: SessionManager,
    mut system: ActorSystem,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let result = {
        let server = SparkConnectServer::new(session_manager.clone());
        let service = SparkConnectServiceServer::new(server)
            // The original Spark Connect server seems to have configuration for inbound (decoding) message size only.
            // .max_encoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);
        ServerBuilder::new("sail_spark_connect", Default::default())
            .add_service(service, Some(crate::spark::connect::FILE_DESCRIPTOR_SET))
            .await
            .serve(listener, signal)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    };
    session_manager.shutdown().await?;
    system.join().await;
    result.map_err(Into::into)
}
