use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use serde::Deserialize;
use std::time::Instant;
use std::{error, time};
use tokio::net::TcpListener; // <-- Added for dummy server
use tokio::io::AsyncWriteExt; // <-- Added for dummy server
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, Clone)]
struct Globals {
    app_token: String,
    _bot_user_oauth_token: String,
    port: String, // <-- Added to capture Render's injected PORT
}

impl Globals {
    fn fetch() -> Result<Globals, dotenv::Error> {
        Ok(Globals {
            app_token: dotenv::var("APP_TOKEN")?,
            _bot_user_oauth_token: dotenv::var("BOT_USER_OAUTH_TOKEN")?,
            // Render binds to random ports but maps them externally. Default to "80" if not specified.
            port: std::env::var("PORT").unwrap_or_else(|_| "80".to_string()),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Bot {
    keys: Globals,
}

#[derive(Deserialize)]
struct AppsConnectionsOpenResponse {
    ok: bool,
    url: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize, Debug)]
struct EventEnvelope {
    envelope_id: String,
    r#type: String,
    payload: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
#[allow(unused)]
struct CatFact {
    fact: String,
    length: u32,
}

impl Bot {
    pub fn init() -> Bot {
        dotenv::dotenv().ok();
        let globals = Globals::fetch()
            .map_err(|e| {
                error!("Failed to load environment variables: {}", e);
                error!("Ensure APP_TOKEN and BOT_USER_OAUTH_TOKEN are set");
                std::process::exit(1);
            })
            .unwrap();

        Bot { keys: globals }
    }

    async fn start_dummy_server(port: String) {
        let addr = format!("0.0.0.0:{}", port);
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind dummy web server to {}: {}", addr, e);
                return;
            }
        };

        info!("Dummy web server listening on http://{}", addr);

        loop {
            if let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let response = "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nContent-Type: text/plain\r\n\r\nBot Online!";
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.flush().await;
                });
            }
        }
    }

    async fn fetch_ws_url(&self) -> Result<String, Box<dyn error::Error>> {
        let client = reqwest::Client::new();
        let res: AppsConnectionsOpenResponse = client
            .post("https://slack.com/api/apps.connections.open")
            .bearer_auth(&self.keys.app_token)
            .send()
            .await?
            .json()
            .await?;

        if !res.ok {
            return Err(format!("Slack API Error: {:?}", res.error).into());
        }

        res.url
            .ok_or_else(|| "No WebSocket URL returned from Slack".into())
    }

    pub async fn start(self) -> Result<(), Box<dyn error::Error>> {
        // dummy web server
        let dummy_port = self.keys.port.clone();
        tokio::spawn(async move {
            Self::start_dummy_server(dummy_port).await;
        });

        loop {
            info!("Fetching WebSocket URL...");
            let ws_url = match self.fetch_ws_url().await {
                Ok(url) => url,
                Err(e) => {
                    error!("Failed to fetch WS URL: {}. Retrying in 5 seconds...", e);
                    tokio::time::sleep(time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            info!("Connecting...");
            let (ws_stream, _) = match connect_async(ws_url).await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("Failed to connect: {}. Retrying in 5 seconds...", e);
                    tokio::time::sleep(time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let (mut write_half, mut read_half) = ws_stream.split();
            info!("Bot online!");

            loop {
                let next_message = timeout(time::Duration::from_secs(60), read_half.next()).await;

                let message = match next_message {
                    Ok(Some(msg)) => msg,
                    Ok(None) => {
                        warn!("WebSocket stream closed");
                        break;
                    }
                    Err(_) => {
                        error!("Timeout - dead connection. Reconnecting...");
                        break;
                    }
                };

                let msg = match message {
                    Ok(Message::Text(text)) => text,
                    Ok(Message::Ping(p)) => {
                        debug!("Responding to ping");
                        let _ = write_half.send(Message::Pong(p)).await;
                        continue;
                    }
                    Ok(Message::Close(_)) => {
                        warn!("WebSocket closed by server");
                        break;
                    }
                    Err(e) => {
                        error!("WebSocket error: {}.", e);
                        break;
                    }
                    _ => continue,
                };

                let Ok(envelope) = serde_json::from_str::<EventEnvelope>(&msg) else {
                    continue;
                };

                let ack_payload = serde_json::json!({ "envelope_id": envelope.envelope_id });

                if envelope.r#type == "disconnect" {
                    warn!("Received disconnect signal from Slack. Refreshing connection...");
                    break;
                }

                if envelope.r#type == "slash_commands" {
                    if self
                        .handle_commands(&envelope, &ack_payload, &mut write_half)
                        .await
                    {
                        continue;
                    }
                }

                let _ = write_half
                    .send(Message::Text(ack_payload.to_string().into()))
                    .await;
            }

            warn!("Connection lost. Reconnecting to Slack in 3 seconds...");
            tokio::time::sleep(time::Duration::from_secs(3)).await;
        }
    }

    async fn handle_commands<S>(
        &self,
        envelope: &EventEnvelope,
        ack_payload: &serde_json::Value,
        write_half: &mut S,
    ) -> bool
    where
        S: futures_util::Sink<Message> + Unpin,
        <S as futures_util::Sink<Message>>::Error: std::fmt::Debug,
    {
        let Some(ref inner_payload) = envelope.payload else {
            return false;
        };
        let Some(command) = inner_payload.get("command").and_then(|c| c.as_str()) else {
            return false;
        };

        if command == "/fbh-help" {
            let _ = write_half.send(Message::Text(
                serde_json::json!({
                    "envelope_id": envelope.envelope_id,
                    "payload": {
                        "response_type": "ephemeral",
                        "text": "A very simple Slack bot. Only cool because it's written without any Slack SDK wrappers. Commands:\n   /fbh-help - this\n   /fbh-ping - get the current latency\n   /fbh-catfact - get a random cat fact\n   /fbh-catimg - get a random cat :3",
                    }
                }).to_string().into(),
            )).await;

            true
        } else if command == "/fbh-ping" {
            // satisfy websocket by sending ack
            let _ = write_half
                .send(Message::Text(ack_payload.to_string().into()))
                .await;

            // respond directly
            let Some(response_url) = inner_payload.get("response_url").and_then(|r| r.as_str())
            else {
                return true;
            };

            let start = Instant::now();

            let client = reqwest::Client::new();
            let http_payload = serde_json::json!({
                "response_type": "ephemeral",
                "text": "Calculating..."
            });

            let response = client.post(response_url).json(&http_payload).send().await;

            let latency = start.elapsed().as_millis();

            if response.is_ok() {
                let update_payload = serde_json::json!({
                    "response_type": "ephemeral",
                    "text": format!("Pong!\nLatency: {}ms", latency)
                });
                let _ = client.post(response_url).json(&update_payload).send().await;
            }
            true
        } else if command == "/fbh-catfact" {
            let _ = write_half
                .send(Message::Text(ack_payload.to_string().into()))
                .await;

            let Some(response_url) = inner_payload.get("response_url").and_then(|r| r.as_str())
            else {
                return true;
            };

            let client = reqwest::Client::new();

            let message_text = match client.get("https://catfact.ninja/fact").send().await {
                Ok(response) => {
                    if let Ok(cat_fact) = response.json::<CatFact>().await {
                        cat_fact.fact
                    } else {
                        "Failed to parse the cat fact data.".to_string()
                    }
                }
                Err(e) => format!("Error fetching cat fact: {}", e),
            };

            let http_payload = serde_json::json ! ({
            "response_type": "ephemeral",
            "text": message_text
            });

            let _ = client.post(response_url).json(&http_payload).send().await;

            true
        } else if command == "/fbh-catimg" {
            let timestamp = time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let cat_image_url = format!("https://cataas.com/cat/says/mrreow?ts={}", timestamp);

            let combined_payload = serde_json::json!({
                "envelope_id": envelope.envelope_id,
                "payload": {
                    "response_type": "ephemeral",
                    "text": "*mrreow*",
                    "blocks": [
                        {
                            "type": "image",
                            "image_url": cat_image_url,
                            "alt_text": "A random cute cat"
                        }
                    ]
                }
            });

            let _ = write_half
                .send(Message::Text(combined_payload.to_string().into()))
                .await;

            true
        } else {
            false
        }
    }
}
