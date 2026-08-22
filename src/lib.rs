//! k7s Tauri application entry point (library crate).
//!
//! The frontend talks to Kubernetes exclusively through the Tauri commands
//! registered here; it never speaks to the API server directly. Live data is
//! pushed back to the webview via Tauri events (see the `kube` module).

// Re-export core modules from k7s-core instead of duplicating them
pub use k7s_core::{ai, core, error, kube};

// Web and MCP servers are now in the k7s-server crate.
// Re-export for backward compatibility so existing bin entry points work.
#[cfg(feature = "web")]
pub use k7s_server::web;

#[cfg(any(feature = "mcp", feature = "web"))]
pub use k7s_server::mcp;

pub use error::{AppError, AppResult};

// Re-export k7s-deps for downstream consumers
pub use k7s_deps;

use core::CoreState;
use kube::ClientManager;
use std::sync::Arc;
// Brings `.manage()` into scope for the App in the setup hook.
use tauri::Manager;

/// Build and run the Tauri application.
///
/// Kept in the library crate so integration tests can construct pieces of it
/// without spawning a real window.
pub fn run() {
    // Structured logs to stderr; level controlled by RUST_LOG (defaults to info).
    k7s_deps::tracing_subscriber::fmt()
        .with_env_filter(
            k7s_deps::tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| k7s_deps::tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Install the default crypto provider for rustls before any TLS connections.
    let _ = k7s_deps::rustls::crypto::ring::default_provider().install_default();

    tauri::Builder::default()
        // The shell plugin backs the capability that lets us open external URLs
        // (e.g. links in the UI) in the user's default browser.
        .plugin(tauri_plugin_shell::init())
        // The dialog plugin backs the native file picker for "Import kubeconfig".
        .plugin(tauri_plugin_dialog::init())
        // Remembers the window's size, position and monitor across launches (B22),
        // saving on exit and restoring on show. There's nothing to gate for demo
        // mode: that runs as a plain browser page with no Tauri backend at all, so
        // this code isn't in the build to begin with.
        .plugin(tauri_plugin_window_state::Builder::default().build())
        .setup(|app| {
            // The ClientManager owns the active client and all connection-scoped
            // tasks. It takes an `EventSink` (not a Tauri `AppHandle`) so the
            // same manager can serve the standalone web shell in the future —
            // TauriEventSink here, WebEventSink over there.
            let sink = core::events::tauri_sink(app.handle().clone());
            let manager = Arc::new(ClientManager::new(sink));
            // Where `prefs.json` (and any future persistent state) lives. The
            // web shell uses a XDG-style fallback — see `web/state.rs`.
            let data_dir = app
                .path()
                .app_config_dir()
                .map_err(|e| format!("no config dir: {e}"))?;
            let state = CoreState::new(manager, data_dir);
            app.manage(state);
            // The AI assistant runtime holds in-flight run bookkeeping
            // (approvals + cancellation). It's cheap and self-contained.
            app.manage(Arc::new(k7s_commands::commands::ai::AiRuntime::new()));
            save_window_state_on_sigterm(app.handle().clone());
            Ok(())
        })
        .invoke_handler(k7s_commands::register_commands!())
        .run(tauri::generate_context!())
        .expect("error while running k7s application");
}

/// Save window geometry when the process is asked to terminate (B22).
///
/// The window-state plugin saves when the app quits *through Tauri* — Cmd+Q, or
/// closing the window. It never sees a SIGTERM, which is exactly how `dev/run.sh`
/// stops the app, so without this the geometry would never survive a development
/// session: B22 would be dead in the workflow B24 standardised.
///
/// Unix-only, which is every platform this ships on today; elsewhere the
/// plugin's own save-on-quit is the whole story.
#[cfg(unix)]
fn save_window_state_on_sigterm(app: tauri::AppHandle) {
    use tauri_plugin_window_state::{AppHandleExt, StateFlags};

    tauri::async_runtime::spawn(async move {
        let Ok(mut term) = k7s_deps::tokio::signal::unix::signal(
            k7s_deps::tokio::signal::unix::SignalKind::terminate(),
        ) else {
            // Nothing to do if the handler can't be installed; the app still exits
            // on SIGTERM, just without remembering where it was.
            return;
        };
        term.recv().await;
        if let Err(e) = app.save_window_state(StateFlags::all()) {
            k7s_deps::tracing::warn!("could not save window state on SIGTERM: {e}");
        }
        // Exit through Tauri so the rest of its shutdown still runs.
        app.exit(0);
    });
}

#[cfg(not(unix))]
fn save_window_state_on_sigterm(_app: tauri::AppHandle) {}
