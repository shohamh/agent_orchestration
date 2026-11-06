//! agent-worker: the execution worker for the agent orchestration fleet, in one static binary.
mod artifacts;
mod capabilities;
mod components;
mod config;
mod connector;
mod ctl;
mod gate;
mod install;
mod job;
mod procutil;
mod program;
mod runner;
mod service;
mod svc;
mod sysmetrics;
mod template;
#[macro_use]
mod util;

use clap::{Parser, Subcommand};
use config::Home;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "agent-worker", version, about = "Manage this PC's execution worker (installed by the Fleet tab's one-line command)")]
struct Cli {
    /// The install folder (default: the folder this program is in)
    #[arg(long, global = true, hide = true)]
    home: Option<PathBuf>,
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the worker: the execution service, the relay connector and any components (started by the startup entry)
    Run {
        #[arg(long, hide = true)]
        log: Option<PathBuf>,
    },
    /// What is installed, running and connected
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Start the worker, or one component
    Start { name: Option<String> },
    /// Stop the worker, or one component
    Stop { name: Option<String> },
    /// Restart the worker, or one component
    Restart { name: Option<String> },
    /// Fetch the server's current worker, wait for running jobs, swap it in and restart (rolls back on failure)
    Update {
        #[arg(long)]
        force: bool,
        #[arg(long, default_value_t = ctl::DRAIN_TIMEOUT, help = "Seconds to wait for running jobs")]
        drain_timeout: u64,
        #[arg(long, hide = true)]
        background: bool,
        #[arg(long, hide = true)]
        from_file: Option<String>,
        /// With --code: this program is the new worker, downloaded through an update link; install it and report back
        #[arg(long, requires = "code")]
        server: Option<String>,
        #[arg(long, requires = "server")]
        code: Option<String>,
    },
    /// Remove this worker from the PC and from the fleet
    Uninstall {
        #[arg(long)]
        yes: bool,
        #[arg(long, help = "Leave the PC registered in the fleet")]
        keep_registration: bool,
    },
    /// Start the worker with the PC, before anyone logs in (Windows: run once in an Administrator shell)
    Boot {
        /// enable, disable or status
        #[arg(default_value = "status")]
        action: String,
    },
    /// The end of the worker's log
    Logs {
        #[arg(short = 'n', long, default_value_t = 50)]
        lines: usize,
    },
    /// The installed worker version
    Version,
    /// Extra processes the worker keeps running (any engine worker): list | add [--cwd DIR] [--env K=V] NAME -- COMMAND... | remove NAME
    Component {
        action: String,
        name: Option<String>,
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        env: Vec<String>,
        #[arg(last = true)]
        argv: Vec<String>,
    },
    /// Used by the one-line install command
    #[command(hide = true)]
    Install {
        /// The install id from the Fleet tab's command (a uuid); everything else comes from the server
        uuid: String,
        #[arg(long, help = "The server this PC reaches (the address in the Fleet tab's command)")]
        server: String,
        #[arg(long, default_value_t = 8096, help = "The worker's local port (it listens on 127.0.0.1 only)")]
        port: u16,
        #[arg(long, help = "Install the files but no startup entry, and do not register")]
        no_service: bool,
    },
    /// Run only the execution service (tests and PCs the coordinator reaches directly)
    #[command(hide = true)]
    Serve {
        #[arg(long, default_value = "local")]
        id: String,
        #[arg(long)]
        uuid: Option<String>,
        #[arg(long, default_value_t = 8096)]
        port: u16,
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        #[arg(long, default_value_t = 4)]
        slots: u64,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        root: PathBuf,
    },
}

#[cfg(windows)]
fn hide_console() {
    // The startup entry launches this console program for an interactive session: drop its window at once.
    unsafe { windows_sys::Win32::System::Console::FreeConsole() };
}

fn finish(result: Result<(), String>) -> ! {
    match result {
        Ok(()) => std::process::exit(0),
        Err(message) => {
            if !message.is_empty() {
                eprintln!("error: {message}");
            }
            std::process::exit(1)
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let installing = matches!(cli.command, Cmd::Install { .. });
    // `install` runs from a temporary download, so its folder is the default install folder, not the program's own.
    let home = if installing { Home(cli.home.clone().unwrap_or_else(install::default_home)) } else { Home::resolve(cli.home) };
    match cli.command {
        Cmd::Run { log } => {
            #[cfg(windows)]
            hide_console();
            util::init_log(Some(&log.unwrap_or_else(|| home.file("worker.log"))));
            // A previous update leaves the old binary behind (Windows cannot delete a running program).
            let _ = std::fs::remove_file(home.file(&format!("{}.old", Home::exe_name())));
            finish(runner::run(home).await)
        }
        Cmd::Status { json } => match ctl::status(&home, json).await {
            Ok(code) => std::process::exit(code),
            Err(e) => finish(Err(e)),
        },
        Cmd::Start { name } => finish(ctl::start(&home, name.as_deref())),
        Cmd::Stop { name } => finish(ctl::stop(&home, name.as_deref())),
        Cmd::Restart { name } => finish(ctl::restart(&home, name.as_deref())),
        Cmd::Update { force, drain_timeout, background: _, from_file, server, code } => finish(ctl::update(&home, force, drain_timeout, from_file, server.zip(code)).await),
        Cmd::Uninstall { yes, keep_registration } => finish(ctl::uninstall(&home, yes, keep_registration).await),
        Cmd::Boot { action } => finish(ctl::boot(&home, &action)),
        Cmd::Logs { lines } => finish(ctl::logs(&home, lines)),
        Cmd::Version => {
            println!("{}", config::self_version());
            std::process::exit(0)
        }
        Cmd::Component { action, name, cwd, env, argv } => finish(ctl::component(&home, &action, name.as_deref(), cwd, env, argv)),
        Cmd::Install { uuid, server, port, no_service } => finish(install::install(home, install::Options { uuid, server, port, no_service }).await),
        Cmd::Serve { id, uuid, port, bind, slots, token, root } => {
            let token = token.or_else(|| std::env::var("FLOW_WORKER_TOKEN").ok()).unwrap_or_default();
            let loopback = ["127.0.0.1", "localhost", "::1"].contains(&bind.as_str());
            if !loopback && token.is_empty() {
                eprintln!("a token is required for a network execution service");
                std::process::exit(2);
            }
            let app = service::App::new(service::Config { id, uuid: uuid.unwrap_or_else(|| uuid::Uuid::new_v4().hyphenated().to_string()), slots, token, root, version: config::self_version() });
            service::spawn_background(app.clone());
            let listener = tokio::net::TcpListener::bind((bind.as_str(), port)).await.expect("bind");
            log!("service", "listening on {}:{}", bind, port);
            axum::serve(listener, service::router(app)).await.unwrap();
        }
    }
}
