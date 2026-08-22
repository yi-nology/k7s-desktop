//! Live verification of node-exporter sampling (B27) against a real cluster,
//! through the same find → forward → scrape → parse → rate path the app uses:
//!
//!   KUBECONFIG=/path/to/kubeconfig cargo run --example nodestats_check
//!
//! Takes several real samples of each Ready node and prints them as the plots
//! would draw them, so the numbers can be sanity-checked against the machine.

use k7s_deps::k8s_openapi::api::core::v1::Node;
use k7s_deps::kube::api::{Api, ListParams};
use k7s_deps::kube::{Client, ResourceExt};
use k7s_deps::tokio::sync::{mpsc, oneshot};
use k7s_lib::kube::observability::exporter::{self, Sampler};
use k7s_lib::kube::observability::nodestats;
use std::time::Duration;

#[k7s_deps::tokio::main]
async fn main() -> k7s_deps::anyhow::Result<()> {
    let client = Client::try_default().await?;

    for node in Api::<Node>::all(client.clone())
        .list(&ListParams::default())
        .await?
        .items
    {
        let name = node.name_any();
        let ready = node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
            .unwrap_or(false);
        if !ready {
            println!("\n=== {name}: NotReady, skipping ===");
            continue;
        }
        println!("\n=== {name} ===");

        let (ns, pod) = match nodestats::find_exporter(client.clone(), &name).await {
            Ok(v) => v,
            Err(e) => {
                println!("  {e}");
                continue;
            }
        };
        println!("  exporter: {ns}/{pod}");

        let (ready_tx, ready_rx) = oneshot::channel();
        let (err_tx, _err_rx) = mpsc::channel::<String>(8);
        let pf = k7s_deps::tokio::spawn(k7s_lib::kube::portforward::run_port_forward(
            client.clone(),
            ns,
            pod,
            9100,
            0,
            ready_tx,
            err_tx,
        ));
        let port = ready_rx.await?.map_err(k7s_deps::anyhow::Error::msg)?;

        let http = k7s_deps::reqwest::Client::new();
        let url = format!("http://127.0.0.1:{port}/metrics");
        let mut sampler = Sampler::default();

        // Four scrapes two seconds apart: the first is only a baseline, so this
        // yields three real samples.
        for i in 0..4 {
            let text = http
                .get(&url)
                .timeout(Duration::from_secs(10))
                .send()
                .await?
                .text()
                .await?;
            if i == 0 {
                println!("  scrape: {} bytes", text.len());
            }
            let raw = exporter::parse(&text, k7s_deps::chrono::Utc::now().timestamp_millis());
            match sampler.push(raw) {
                None => println!("  sample 0: (baseline only — counters need two scrapes)"),
                Some(s) => println!(
                    "  cpu {:>5.1}%   mem {:>5.1}% ({:.1}/{:.1} GiB)   rx {:>8}/s  tx {:>8}/s   load {:.2} {:.2} {:.2}",
                    s.cpu_percent,
                    100.0 * s.mem_used_bytes / s.mem_total_bytes.max(1.0),
                    s.mem_used_bytes / 1e9,
                    s.mem_total_bytes / 1e9,
                    human(s.net_rx_bps),
                    human(s.net_tx_bps),
                    s.load1,
                    s.load5,
                    s.load15,
                ),
            }
            k7s_deps::tokio::time::sleep(Duration::from_secs(2)).await;
        }

        // Filesystems come from the last sample; they barely move.
        let text = http
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await?
            .text()
            .await?;
        let raw = exporter::parse(&text, k7s_deps::chrono::Utc::now().timestamp_millis());
        if let Some(s) = sampler.push(raw) {
            println!("  filesystems:");
            for fs in &s.filesystems {
                println!(
                    "    {:<24} {:>5.1}%  ({:.0}/{:.0} GiB)",
                    fs.mountpoint,
                    100.0 * fs.used_bytes / fs.size_bytes.max(1.0),
                    fs.used_bytes / 1e9,
                    fs.size_bytes / 1e9
                );
            }
            // Sanity: the numbers must be plottable, not NaN/absurd.
            assert!((0.0..=100.0).contains(&s.cpu_percent), "cpu% out of range");
            assert!(s.mem_total_bytes > 0.0, "no memory reported");
            assert!(s.net_rx_bps >= 0.0 && s.net_tx_bps >= 0.0, "negative rate");
        }

        pf.abort();
    }

    println!("\nNode stats OK.");
    Ok(())
}

/// Bytes as a short human string.
fn human(bps: f64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut v = bps;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    format!("{v:.1} {}", UNITS[u])
}
