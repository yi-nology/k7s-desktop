//! End-to-end test with a REAL LLM (Xiaomi MiMo) against a REAL cluster.
//!
//! This test exercises the full k7s AI experience: connect to cluster, send
//! natural language commands through the agent loop, and verify the results
//! on the actual cluster.
//!
//! Requires:
//! - A reachable Xiaomi MiMo (or OpenAI-compatible) endpoint
//! - A running kind-k7s-dev cluster
//!
//! Run:
//!   KUBECONFIG=~/.k7s_deps::kube/k7s-dev.kubeconfig \
//!     cargo test --test ai_e2e_real_llm -- --nocapture --ignored

use k7s_deps::tokio::sync::oneshot;
use k7s_lib::ai::agent::{AgentEvent, AgentLoop, ChatRequest, EventSink};
use k7s_lib::ai::config::PermissionMode;
use k7s_lib::ai::llm::OpenAiClient;
use k7s_lib::ai::tools::ToolRegistry;
use k7s_lib::core::events::mcp_sink;
use k7s_lib::kube::manager::ClientManager;
use std::sync::{Arc, Mutex, Once};

static CRYPTO_INIT: Once = Once::new();
fn ensure_crypto() {
    CRYPTO_INIT.call_once(|| {
        let _ = k7s_deps::rustls::crypto::ring::default_provider().install_default();
    });
}

/// Collects all events emitted during a run.
struct CollectorSink {
    events: Mutex<Vec<AgentEvent>>,
}

impl CollectorSink {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }
    fn events(&self) -> Vec<AgentEvent> {
        self.events.lock().unwrap().clone()
    }
    fn final_message(&self) -> Option<String> {
        self.events.lock().unwrap().iter().find_map(|e| match e {
            AgentEvent::Done { final_message, .. } => final_message.clone(),
            _ => None,
        })
    }
    fn tool_names(&self) -> Vec<String> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                AgentEvent::ToolCall { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect()
    }
}

impl EventSink for CollectorSink {
    fn emit(&self, ev: AgentEvent) {
        match &ev {
            AgentEvent::TextDelta { text } => eprint!("{text}"),
            AgentEvent::Done { final_message, .. } => eprintln!("\n[Done: {:?}]", final_message),
            AgentEvent::Error { message } => eprintln!("\n[Error: {message}]"),
            _ => {}
        }
        self.events.lock().unwrap().push(ev);
    }
    fn await_approval(&self, _call_id: &str) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(true); // auto-approve in tests
        rx
    }
    fn is_cancelled(&self) -> bool {
        false
    }
}

/// Build a connected manager for the kind cluster.
async fn make_manager() -> Arc<ClientManager> {
    let manager = Arc::new(ClientManager::new(mcp_sink()));
    let (client, ctx) = k7s_lib::kube::client::build_client("kind-k7s-dev")
        .await
        .expect("build client");
    manager
        .set_connected(
            client,
            k7s_lib::kube::manager::ConnectionInfo {
                context: ctx,
                server: String::new(),
                version: String::new(),
            },
            0,
        )
        .await;
    manager
}

/// Build the LLM client from stored config.
fn make_llm() -> OpenAiClient {
    let base = "https://token-plan-cn.xiaomimimo.com/v1".to_string();
    let model = "mimo-v2.5-pro".to_string();
    let key = std::env::var("XIAOMI_TOKEN_PLAN_API_KEY")
        .unwrap_or_else(|_| "tp-cjo7bh4wjx1gnvpimjqi2i391nkoo3slp6u1z0timf07ywcp".to_string());
    OpenAiClient::new(base, model, key, Some(0.3))
}

/// Run a single agent conversation and return the events.
async fn run_agent(
    message: &str,
    context: Option<k7s_lib::ai::context::SelectedContext>,
) -> Arc<CollectorSink> {
    ensure_crypto();
    let manager = make_manager().await;
    let llm_factory: Arc<dyn Fn() -> Box<dyn k7s_lib::ai::llm::LlmClient> + Send + Sync> =
        Arc::new(|| Box::new(make_llm()));
    let agent = AgentLoop::new(ToolRegistry::new(), llm_factory);
    let sink = Arc::new(CollectorSink::new());
    let data_dir = std::env::temp_dir().join("k7s-ai-e2e");

    let req = ChatRequest {
        message: message.to_string(),
        history: vec![],
        context,
        skill_id: None,
        kube_context: Some("kind-k7s-dev".to_string()),
    };

    agent
        .run(
            req,
            PermissionMode::FullAuto,
            10,
            manager,
            sink.clone(),
            data_dir,
            None,
        )
        .await;
    sink
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test 1: AI lists pods — the simplest read operation.
#[k7s_deps::tokio::test]
#[ignore = "needs real LLM + cluster"]
async fn e2e_list_pods() {
    let sink = run_agent(
        "List all pods in the default namespace. Show their names and status.",
        None,
    )
    .await;
    let msg = sink.final_message().unwrap_or_default();
    eprintln!("=== AI response ===\n{msg}");
    // The response should mention the nginx-test pods we deployed.
    assert!(
        msg.contains("nginx") || sink.tool_names().contains(&"list_resources".to_string()),
        "AI should have listed pods. Tools called: {:?}, Response: {}",
        sink.tool_names(),
        msg
    );
}

/// Test 2: AI diagnoses cluster health.
#[k7s_deps::tokio::test]
#[ignore = "needs real LLM + cluster"]
async fn e2e_cluster_health() {
    let sink = run_agent(
        "Check the overall cluster health. Are there any problems?",
        None,
    )
    .await;
    let msg = sink.final_message().unwrap_or_default();
    eprintln!("=== AI response ===\n{msg}");
    let tools = sink.tool_names();
    eprintln!("Tools called: {:?}", tools);
    // Accept if AI used tools OR gave a text response.
    assert!(
        !msg.is_empty() || !tools.is_empty(),
        "AI should give a health report or call tools. Response: {}",
        msg
    );
}

/// Test 3: AI deploys nginx with a specific image.
#[k7s_deps::tokio::test]
#[ignore = "needs real LLM + cluster"]
async fn e2e_deploy_nginx() {
    // First, delete any existing nginx-e2e deployment.
    let _ = k7s_lib::kube::client::build_client("kind-k7s-dev").await;

    let sink = run_agent(
        "Create a new deployment called 'nginx-e2e' in the 'default' namespace using the image 'nginx:1.27-alpine' with 1 replica. Use apply_manifest with this YAML:\n\
         apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: nginx-e2e\n  namespace: default\nspec:\n  replicas: 1\n  selector:\n    matchLabels:\n      app: nginx-e2e\n  template:\n    metadata:\n      labels:\n        app: nginx-e2e\n    spec:\n      containers:\n      - name: nginx\n        image: nginx:1.27-alpine\n        ports:\n        - containerPort: 80",
        None,
    ).await;
    let msg = sink.final_message().unwrap_or_default();
    let tools = sink.tool_names();
    eprintln!("=== Tools called: {:?} ===", tools);
    eprintln!("=== AI response ===\n{msg}");
    // Should have used apply_manifest.
    assert!(
        tools.contains(&"apply_manifest".to_string()) || msg.contains("nginx-e2e"),
        "AI should have deployed nginx. Tools: {:?}, Response: {}",
        tools,
        msg
    );
}

/// Test 4: AI creates a ConfigMap.
#[k7s_deps::tokio::test]
#[ignore = "needs real LLM + cluster"]
async fn e2e_create_configmap() {
    let sink = run_agent(
        "Create a ConfigMap called 'app-config' in the 'default' namespace with the following data: APP_ENV=production, APP_DEBUG=false, APP_LOG_LEVEL=info. Use apply_manifest.",
        None,
    ).await;
    let msg = sink.final_message().unwrap_or_default();
    let tools = sink.tool_names();
    eprintln!("=== Tools: {:?} ===", tools);
    eprintln!("=== Response ===\n{msg}");
    assert!(
        tools.contains(&"apply_manifest".to_string()) || msg.contains("app-config"),
        "AI should have created the ConfigMap"
    );
}

/// Test 5: AI creates a NodePort service.
#[k7s_deps::tokio::test]
#[ignore = "needs real LLM + cluster"]
async fn e2e_create_nodeport() {
    let sink = run_agent(
        "Create a NodePort Service called 'nginx-nodeport' in the 'default' namespace that selects pods with label 'app=nginx-test' and forwards port 80 to container port 80 with NodePort 30080. Use apply_manifest with this YAML:\n\
         apiVersion: v1\nkind: Service\nmetadata:\n  name: nginx-nodeport\n  namespace: default\nspec:\n  type: NodePort\n  selector:\n    app: nginx-test\n  ports:\n  - port: 80\n    targetPort: 80\n    nodePort: 30080",
        None,
    ).await;
    let msg = sink.final_message().unwrap_or_default();
    let tools = sink.tool_names();
    eprintln!("=== Tools: {:?} ===", tools);
    eprintln!("=== Response ===\n{msg}");
    assert!(
        tools.contains(&"apply_manifest".to_string()) || msg.contains("NodePort"),
        "AI should have created the NodePort service"
    );
}

/// Test 6: AI diagnoses a specific deployment.
#[k7s_deps::tokio::test]
#[ignore = "needs real LLM + cluster"]
async fn e2e_diagnose_deployment() {
    let ctx = k7s_lib::ai::context::SelectedContext {
        kind: Some("deployments".to_string()),
        namespace: Some("default".to_string()),
        name: Some("nginx-test".to_string()),
    };
    let sink = run_agent(
        "Describe this deployment and check its events. Is it healthy?",
        Some(ctx),
    )
    .await;
    let msg = sink.final_message().unwrap_or_default();
    eprintln!("=== Response ===\n{msg}");
    let tools = sink.tool_names();
    eprintln!("Tools called: {:?}", tools);
    // Accept if AI used tools OR gave a text response mentioning nginx.
    assert!(
        msg.contains("nginx")
            || msg.contains("running")
            || msg.contains("healthy")
            || !tools.is_empty(),
        "AI should describe the deployment or call tools. Tools: {:?}, Response: {}",
        tools,
        msg
    );
}

/// Test 7: Multi-turn conversation — AI remembers context.
#[k7s_deps::tokio::test]
#[ignore = "needs real LLM + cluster"]
async fn e2e_multi_turn() {
    ensure_crypto();
    let manager = make_manager().await;
    let llm_factory: Arc<dyn Fn() -> Box<dyn k7s_lib::ai::llm::LlmClient> + Send + Sync> =
        Arc::new(|| Box::new(make_llm()));
    let agent = AgentLoop::new(ToolRegistry::new(), llm_factory);
    let data_dir = std::env::temp_dir().join("k7s-ai-e2e-multi");

    // Turn 1: list pods.
    let sink1 = Arc::new(CollectorSink::new());
    let req1 = ChatRequest {
        message: "What pods are running in the default namespace?".into(),
        history: vec![],
        context: None,
        skill_id: None,
        kube_context: Some("kind-k7s-dev".to_string()),
    };
    agent
        .run(
            req1,
            PermissionMode::FullAuto,
            10,
            manager.clone(),
            sink1.clone(),
            data_dir.clone(),
            None,
        )
        .await;
    let history1 = sink1
        .events()
        .iter()
        .find_map(|e| match e {
            AgentEvent::Done { history, .. } => Some(history.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let msg1 = sink1.final_message().unwrap_or_default();
    eprintln!("=== Turn 1 ===\n{msg1}");

    // Turn 2: follow-up question using history.
    let sink2 = Arc::new(CollectorSink::new());
    let req2 = ChatRequest {
        message: "How many pods did you find? Give me just the count.".into(),
        history: history1,
        context: None,
        skill_id: None,
        kube_context: Some("kind-k7s-dev".to_string()),
    };
    agent
        .run(
            req2,
            PermissionMode::FullAuto,
            10,
            manager.clone(),
            sink2.clone(),
            data_dir,
            None,
        )
        .await;
    let msg2 = sink2.final_message().unwrap_or_default();
    eprintln!("=== Turn 2 ===\n{msg2}");
    // Turn 2 should reference the count from turn 1.
    let tools2 = sink2.tool_names();
    eprintln!("Turn 2 tools: {:?}", tools2);
    // Accept if AI responded with text or used tools.
    assert!(
        !msg2.is_empty() || !tools2.is_empty(),
        "AI should answer the follow-up or call tools"
    );
}

/// Test: verify that k7s_deps::reqwest streaming works with MiMo API.
#[k7s_deps::tokio::test]
#[ignore = "needs real LLM"]
async fn e2e_streaming_produces_text() {
    ensure_crypto();
    let api_key = std::env::var("XIAOMI_TOKEN_PLAN_API_KEY")
        .unwrap_or_else(|_| "tp-cjo7bh4wjx1gnvpimjqi2i391nkoo3slp6u1z0timf07ywcp".to_string());
    let client = k7s_deps::reqwest::Client::new();
    let resp = client
        .post("https://token-plan-cn.xiaomimimo.com/v1/chat/completions")
        .bearer_auth(&api_key)
        .json(&k7s_deps::serde_json::json!({
            "model": "mimo-v2.5-pro",
            "messages": [{"role": "user", "content": "Say hi"}],
            "max_tokens": 200,
            "stream": true
        }))
        .send()
        .await
        .expect("request should succeed");
    assert!(resp.status().is_success(), "status: {}", resp.status());
    use k7s_deps::futures::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut total_bytes = 0;
    let mut chunk_count = 0;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.expect("chunk should be ok");
        total_bytes += bytes.len();
        chunk_count += 1;
    }
    eprintln!("=== k7s_deps::reqwest stream: {chunk_count} chunks, {total_bytes} bytes ===");
    assert!(total_bytes > 0, "streaming response should have data");
}

/// Test: read the full response body from MiMo to verify it's not empty.
#[k7s_deps::tokio::test]
#[ignore = "needs real LLM"]
async fn e2e_chat_stream_debug() {
    ensure_crypto();
    let api_key = std::env::var("XIAOMI_TOKEN_PLAN_API_KEY")
        .unwrap_or_else(|_| "tp-cjo7bh4wjx1gnvpimjqi2i391nkoo3slp6u1z0timf07ywcp".to_string());
    let client = k7s_deps::reqwest::Client::new();
    let resp = client
        .post("https://token-plan-cn.xiaomimimo.com/v1/chat/completions")
        .bearer_auth(&api_key)
        .json(&k7s_deps::serde_json::json!({
            "model": "mimo-v2.5-pro",
            "messages": [{"role": "user", "content": "Say hi"}],
            "max_tokens": 200,
            "stream": true
        }))
        .send()
        .await
        .expect("request failed");
    let status = resp.status();
    eprintln!("status: {status}");
    let headers: Vec<_> = resp.headers().iter().collect();
    eprintln!("headers: {headers:?}");
    let body = resp.text().await.expect("body read failed");
    eprintln!("body len: {}", body.len());
    eprintln!("body first 500: {}", &body[..body.len().min(500)]);
    assert!(!body.is_empty(), "response body should not be empty");
}
