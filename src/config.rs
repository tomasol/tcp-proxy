use anyhow::{bail, Context};

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct AppsContainer {
    pub apps: Vec<AppConfig>,
}

impl AppsContainer {
    pub async fn load_config(path: &str) -> Result<Self, anyhow::Error> {
        use tokio::fs::File;
        use tokio::io::AsyncReadExt; // for read_to_end()

        let mut file = File::open(path).await?;
        let mut contents = vec![];
        file.read_to_end(&mut contents).await?;
        let container: Self =
            serde_json::from_slice(&contents).context("Cannot deserialize config")?;
        // validate empty targets, empty ports
        if container.apps.is_empty() {
            bail!("no apps in config.json");
        }
        for app in &container.apps {
            if app.ports.is_empty() {
                bail!("empty app ports: {}", app.name);
            }
            if app.targets.is_empty() {
                bail!("empty app targets: {}", app.name);
            }
        }
        Ok(container)
    }
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct AppConfig {
    name: String,
    pub ports: Vec<u16>,
    pub targets: Vec<String>, // no parsing because of DNS - resolve on reconnect
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::config::AppsContainer;

    #[test]
    fn parse() {
        let json = json!(
            {
                "Apps": [
                  {
                    "Name": "five-thousand",
                    "Ports": [
                      5001,
                      5200,
                      5300,
                      5400
                    ],
                    "Targets": [
                      "tcp-echo.fly.dev:5001",
                      "tcp-echo.fly.dev:5002"
                    ]
                  },
                  {
                    "Name": "six-thousand",
                    "Ports": [
                      6001,
                      6200,
                      6300,
                      6400
                    ],
                    "Targets": [
                      "tcp-echo.fly.dev:6001",
                      "tcp-echo.fly.dev:6002",
                      "bad.target.for.testing:6003"
                    ]
                  },
                  {
                    "Name": "seven-thousand",
                    "Ports": [
                      7001,
                      7200,
                      7300,
                      7400
                    ],
                    "Targets": [
                      "tcp-echo.fly.dev:7001",
                      "tcp-echo.fly.dev:7002"
                    ]
                  }
                ]
              }
        );
        let deserialized: AppsContainer = serde_json::from_value(json).unwrap();
        println!("deserialized = {:?}", deserialized);
    }
}
