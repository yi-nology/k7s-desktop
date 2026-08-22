//! Live verification of the Prometheus history backfill (B38):
//!
//!   KUBECONFIG=/path/to/kubeconfig cargo run --example promql_check
//!
//! Read-only. Discovers the cluster's Prometheus, backfills a node's charts from
//! it, and checks the samples are actually plottable — the point of the feature
//! is a chart that opens populated, so empty-but-successful is a failure here.

use k7s_deps::k8s_openapi::api::core::v1::Node;
use k7s_deps::kube::api::{Api, ListParams};
use k7s_deps::kube::{Client, ResourceExt};
use k7s_lib::kube::observability::promql;

#[k7s_deps::tokio::main]
async fn main() -> k7s_deps::anyhow::Result<()> {
    let client = Client::try_default().await?;

    let Some(svc) = promql::discover(&client).await else {
        println!("no Prometheus found — the app falls back to B27's live scraper.");
        return Ok(());
    };
    println!("discovered: {}/{}:{}", svc.namespace, svc.name, svc.port);

    // Backfill whichever node is actually Ready — a node that's been down for
    // days has no recent series, and would look like a bug in this code.
    let nodes: Api<Node> = Api::all(client.clone());
    let ready = nodes
        .list(&ListParams::default())
        .await?
        .items
        .into_iter()
        .find(|n| {
            n.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        })
        .expect("a Ready node");
    let name = ready.name_any();

    let now = k7s_deps::chrono::Utc::now().timestamp();
    let samples = promql::node_history(&client, &svc, &name, now, 3600, 30).await?;
    println!(
        "node {name}: {} backfilled sample(s) over the last hour\n",
        samples.len()
    );

    for s in samples.iter().rev().take(5).rev() {
        println!(
            "  ts={} cpu={:>5.1}%  mem={:>5.1}GiB/{:<5.1}GiB  rx={:>8.0}B/s tx={:>8.0}B/s  load={:.2}",
            s.ts,
            s.cpu_percent,
            s.mem_used_bytes / 1024.0 / 1024.0 / 1024.0,
            s.mem_total_bytes / 1024.0 / 1024.0 / 1024.0,
            s.net_rx_bps,
            s.net_tx_bps,
            s.load1,
        );
    }

    assert!(
        !samples.is_empty(),
        "a Prometheus with node metrics must yield history"
    );
    // Timestamps ascending and distinct — the charts plot these as an x axis.
    assert!(
        samples.windows(2).all(|w| w[0].ts < w[1].ts),
        "samples must be strictly ordered by time"
    );
    // The whole point is a *populated* chart, so the series must carry real
    // values rather than a row of structurally-present zeroes.
    let with_mem = samples.iter().filter(|s| s.mem_total_bytes > 0.0).count();
    let with_cpu = samples.iter().filter(|s| s.cpu_percent > 0.0).count();
    println!(
        "\n{with_mem}/{} carry memory, {with_cpu}/{} carry cpu",
        samples.len(),
        samples.len()
    );
    assert!(with_mem > 0, "memory series must have landed");
    assert!(with_cpu > 0, "cpu series must have landed");
    // Filesystems are a *current* reading; backfilled points must not carry them.
    assert!(
        samples.iter().all(|s| s.filesystems.is_empty()),
        "history must not backfill filesystems — the UI shows those as current"
    );

    println!("\nPrometheus backfill OK.");
    Ok(())
}
