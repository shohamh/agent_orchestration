//! `agent-worker run`: the execution service on 127.0.0.1, the relay connector and any extra components, in one process.
//! Nothing listens on the network: the service binds to loopback and the connector only makes outbound calls.
use crate::components::{supervise, Supervisor};
use crate::config::{self, port_of, Home};
use crate::connector::Connector;
use crate::service::{self, App, Config};
use std::sync::Arc;

pub async fn run(home: Home) -> Result<(), String> {
    let config = home.config()?;
    let name = config::name_of(&config);
    let uuid = config::relay_id(&config);
    let server = config["server"].as_str().ok_or("worker.json has no server")?.to_string();
    let token = home.token();
    if token.is_empty() {
        return Err("The token file is empty".into());
    }
    let port = port_of(&config);
    let pidfile = home.file("worker.pid");
    std::fs::write(&pidfile, std::process::id().to_string()).map_err(|e| e.to_string())?; // `agent-worker stop` and status find the runner here
    let app = App::new(Config { id: name.clone(), uuid: uuid.clone(), slots: config["slots"].as_u64().unwrap_or(1), token: token.clone(), root: home.file("runtime"), version: config::self_version() });
    service::spawn_background(app.clone());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.map_err(|e| format!("cannot listen on 127.0.0.1:{port}: {e}"))?;
    crate::log!("runner", "worker {name} ({uuid}) {} on 127.0.0.1:{port}, relaying through {server}", config::self_version());
    let server_task = tokio::spawn(async move { axum::serve(listener, service::router(app)).await });
    let connector = Connector::new(&server, &uuid, &token, port, Home(home.0.clone()));
    let connector_task = tokio::spawn(connector.run());
    let supervisor = Arc::new(tokio::sync::Mutex::new(Supervisor::new(Home(home.0.clone()))));
    let supervisor_task = tokio::spawn(supervise(supervisor.clone()));

    wait_for_stop().await;
    crate::log!("runner", "stopping");
    supervisor_task.abort();
    supervisor.lock().await.stop_all().await;
    connector_task.abort();
    server_task.abort();
    if std::fs::read_to_string(&pidfile).map(|t| t.trim() == std::process::id().to_string()).unwrap_or(false) {
        let _ = std::fs::remove_file(&pidfile);
    }
    Ok(())
}

async fn wait_for_stop() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let (mut term, mut int) = (signal(SignalKind::terminate()).unwrap(), signal(SignalKind::interrupt()).unwrap());
        tokio::select! { _ = term.recv() => {}, _ = int.recv() => {} }
    }
    #[cfg(windows)]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
