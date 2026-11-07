//! agent-worker: the execution worker for the agent orchestration fleet, in one static binary.
mod acp;
mod artifacts;
mod capabilities;
mod components;
mod config;
mod confluence;
mod connector;
mod ctl;
mod environment;
mod gate;
mod install;
mod job;
mod procutil;
mod program;
mod runner;
mod schema;
mod service;
mod session;
mod svc;
mod sysmetrics;
mod template;
#[macro_use]
mod util;

use clap::{Parser, Subcommand};
use config::Home;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "agent-worker",
    version,
    about = "Manage this PC's execution worker (installed by the Fleet tab's one-line command)"
)]
struct Cli {
    /// The install folder (default: this program's install, then the marked default install)
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
        #[arg(long, hide = true)]
        as_user: Option<String>,
    },
    /// Restart the worker when it crashes or is killed: on, off or status
    Autorestart {
        #[arg(default_value = "status")]
        action: String,
        #[arg(long, hide = true)]
        as_user: Option<String>,
    },
    /// The end of the worker's log
    Logs {
        #[arg(short = 'n', long, default_value_t = 50)]
        lines: usize,
    },
    /// The installed worker version
    Version,
    /// `install`: the coding-agent adapters this worker can drive (needs Node.js). `status`: what this PC can run.
    Agents {
        action: String,
        /// The npm registry to install from (an offline mirror, for example)
        #[arg(long)]
        registry: Option<String>,
    },
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
        #[arg(
            long,
            help = "The server this PC reaches (the address in the Fleet tab's command)"
        )]
        server: String,
        #[arg(
            long,
            default_value_t = 8096,
            help = "The worker's local port (it listens on 127.0.0.1 only)"
        )]
        port: u16,
        #[arg(
            long,
            help = "Install the files but no startup entry, and do not register"
        )]
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

fn command_home(
    explicit: Option<PathBuf>,
    configured: Option<PathBuf>,
    default_home: PathBuf,
    executable_home: Home,
) -> Home {
    if let Some(home) = explicit.or(configured) {
        return Home(home);
    }
    if executable_home.file("installed.json").is_file()
        && executable_home.file("worker.json").is_file()
    {
        return executable_home;
    }
    if default_home.join("installed.json").is_file() && default_home.join("worker.json").is_file() {
        Home(default_home)
    } else {
        executable_home
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let installing = matches!(&cli.command, Cmd::Install { .. });
    let home = if installing {
        Home(cli.home.clone().unwrap_or_else(install::default_home))
    } else {
        command_home(
            cli.home.clone(),
            std::env::var_os("AGENT_WORKER_HOME").map(PathBuf::from),
            install::default_home(),
            Home::resolve(None),
        )
    };
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
        Cmd::Update {
            force,
            drain_timeout,
            background: _,
            from_file,
            server,
            code,
        } => finish(ctl::update(&home, force, drain_timeout, from_file, server.zip(code)).await),
        Cmd::Uninstall {
            yes,
            keep_registration,
        } => finish(ctl::uninstall(&home, yes, keep_registration).await),
        Cmd::Boot { action, as_user } => finish(ctl::boot(&home, &action, as_user)),
        Cmd::Autorestart { action, as_user } => finish(ctl::autorestart(&home, &action, as_user)),
        Cmd::Logs { lines } => finish(ctl::logs(&home, lines)),
        Cmd::Version => {
            println!("{}", config::self_version());
            std::process::exit(0)
        }
        Cmd::Agents { action, registry } => {
            let adapters = home.file("adapters");
            acp::set_adapter_home(adapters.clone());
            let outcome = match action.as_str() {
                "install" => acp::install_adapters(&adapters, registry.as_deref()),
                "status" => Ok(()),
                other => Err(format!("agents takes install or status, not {other}")),
            };
            if outcome.is_ok() {
                for agent in capabilities::AGENTS {
                    println!(
                        "{agent}: {}",
                        capabilities::probe_agent(agent)["status"]
                            .as_str()
                            .unwrap_or("unknown")
                    );
                }
            }
            finish(outcome)
        }
        Cmd::Component {
            action,
            name,
            cwd,
            env,
            argv,
        } => finish(ctl::component(
            &home,
            &action,
            name.as_deref(),
            cwd,
            env,
            argv,
        )),
        Cmd::Install {
            uuid,
            server,
            port,
            no_service,
        } => finish(
            install::install(
                home,
                install::Options {
                    uuid,
                    server,
                    port,
                    no_service,
                },
            )
            .await,
        ),
        Cmd::Serve {
            id,
            uuid,
            port,
            bind,
            slots,
            token,
            root,
        } => {
            let token = token
                .or_else(|| std::env::var("FLOW_WORKER_TOKEN").ok())
                .unwrap_or_default();
            let loopback = ["127.0.0.1", "localhost", "::1"].contains(&bind.as_str());
            if !loopback && token.is_empty() {
                eprintln!("a token is required for a network execution service");
                std::process::exit(2);
            }
            let app = service::App::new(service::Config {
                id,
                uuid: uuid.unwrap_or_else(|| uuid::Uuid::new_v4().hyphenated().to_string()),
                slots,
                token,
                root,
                version: config::self_version(),
            });
            service::spawn_background(app.clone());
            let listener = tokio::net::TcpListener::bind((bind.as_str(), port))
                .await
                .expect("bind");
            log!("service", "listening on {}:{}", bind, port);
            axum::serve(listener, service::router(app)).await.unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_uses_the_marked_default_install_from_an_external_cli_folder() {
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join("installed");
        let command = dir.path().join("temporary-cli");
        std::fs::create_dir_all(&default).unwrap();
        std::fs::write(default.join("installed.json"), "{}").unwrap();
        std::fs::write(default.join("worker.json"), "{}").unwrap();
        assert_eq!(
            command_home(None, None, default.clone(), Home(command)).0,
            default
        );
    }

    #[test]
    fn command_honors_explicit_and_environment_home_over_default() {
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join("installed");
        let explicit = dir.path().join("explicit");
        let configured = dir.path().join("configured");
        std::fs::create_dir_all(&default).unwrap();
        std::fs::write(default.join("installed.json"), "{}").unwrap();
        assert_eq!(
            command_home(
                Some(explicit.clone()),
                None,
                default.clone(),
                Home(dir.path().join("cli")),
            )
            .0,
            explicit
        );
        assert_eq!(
            command_home(
                None,
                Some(configured.clone()),
                default,
                Home(dir.path().join("cli")),
            )
            .0,
            configured
        );
    }

    #[test]
    fn command_falls_back_to_the_executable_folder_without_a_default_install() {
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join("not-installed");
        let executable = dir.path().join("cli");
        assert_eq!(
            command_home(None, None, default, Home(executable.clone())).0,
            executable
        );
    }
    #[test]
    fn command_prefers_a_marked_custom_install_over_the_marked_default() {
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join("default");
        let custom = dir.path().join("custom");
        for home in [&default, &custom] {
            std::fs::create_dir_all(home).unwrap();
            std::fs::write(home.join("installed.json"), "{}").unwrap();
            std::fs::write(home.join("worker.json"), "{}").unwrap();
        }
        assert_eq!(
            command_home(None, None, default, Home(custom.clone())).0,
            custom
        );
    }
}
