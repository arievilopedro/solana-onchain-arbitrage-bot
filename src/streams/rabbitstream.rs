//! RabbitStream shred listener wiring.
//!
//! RabbitStream is the fast trigger source. In controlled V1 it should not own
//! pool discovery state; it should emit candidate signals that are checked
//! against the RPC/Geyser-maintained registry.

use crate::config::StreamEndpointConfig;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RabbitStreamPlan {
    pub url: String,
    pub x_token: String,
}

impl RabbitStreamPlan {
    pub fn controlled_v1(endpoint: &StreamEndpointConfig) -> anyhow::Result<Option<Self>> {
        if !endpoint.enabled {
            return Ok(None);
        }

        if endpoint.url.trim().is_empty() {
            anyhow::bail!("rabbitstream.url is required when rabbitstream.enabled=true");
        }

        if endpoint.x_token.trim().is_empty() {
            anyhow::bail!("rabbitstream.x_token is required when rabbitstream.enabled=true");
        }

        Ok(Some(Self {
            url: endpoint.url.clone(),
            x_token: endpoint.x_token.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controlled_plan_is_disabled_when_endpoint_disabled() {
        let endpoint = StreamEndpointConfig {
            enabled: false,
            url: String::new(),
            x_token: String::new(),
        };

        assert!(RabbitStreamPlan::controlled_v1(&endpoint)
            .unwrap()
            .is_none());
    }

    #[test]
    fn controlled_plan_keeps_rabbitstream_credentials_separate() {
        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: "https://rabbitstream.example".to_string(),
            x_token: "rabbit-token".to_string(),
        };

        let plan = RabbitStreamPlan::controlled_v1(&endpoint).unwrap().unwrap();

        assert_eq!(plan.url, endpoint.url);
        assert_eq!(plan.x_token, endpoint.x_token);
    }

    #[test]
    fn controlled_plan_requires_enabled_endpoint_fields() {
        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: String::new(),
            x_token: "token".to_string(),
        };
        assert!(RabbitStreamPlan::controlled_v1(&endpoint).is_err());

        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: "https://rabbitstream.example".to_string(),
            x_token: String::new(),
        };
        assert!(RabbitStreamPlan::controlled_v1(&endpoint).is_err());
    }
}
