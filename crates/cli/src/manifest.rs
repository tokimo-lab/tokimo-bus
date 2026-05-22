//! Helpers for reading compile-time embedded `tokimo-app.toml` manifests.

use serde::Deserialize;

#[derive(Deserialize)]
struct Manifest {
    database: Option<DatabaseSection>,
    runtime: Option<RuntimeSection>,
}

#[derive(Deserialize)]
struct DatabaseSection {
    schema: Option<String>,
}

#[derive(Deserialize)]
struct RuntimeSection {
    ui_dist: Option<String>,
}

/// Parse a `tokimo-app.toml` string and return the `[database] schema` field.
///
/// Returns `Ok(None)` when:
/// - the manifest has no `[database]` section, or
/// - `[database]` exists but omits the `schema` key.
///
/// Returns `Err` only on TOML parse failure.
pub fn parse_app_schema(toml_str: &str) -> anyhow::Result<Option<String>> {
    let manifest: Manifest = toml::from_str(toml_str)?;
    Ok(manifest.database.and_then(|db| db.schema))
}

/// Parse a `tokimo-app.toml` string and return the `[runtime] ui_dist` field.
///
/// Returns `Ok(None)` when:
/// - the manifest has no `[runtime]` section, or
/// - `[runtime]` exists but omits the `ui_dist` key.
///
/// Returns `Err` only on TOML parse failure.
pub fn parse_app_ui_dist(toml_str: &str) -> anyhow::Result<Option<String>> {
    let manifest: Manifest = toml::from_str(toml_str)?;
    Ok(manifest.runtime.and_then(|r| r.ui_dist))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_database_schema() {
        let toml = r#"
[app]
id = "myapp"

[database]
schema = "my_schema"
migrations_dir = "migrations"
"#;
        assert_eq!(parse_app_schema(toml).unwrap(), Some("my_schema".to_string()));
    }

    #[test]
    fn without_database_section() {
        let toml = r#"
[app]
id = "myapp"
"#;
        assert_eq!(parse_app_schema(toml).unwrap(), None);
    }

    #[test]
    fn database_without_schema_field() {
        let toml = r#"
[app]
id = "myapp"

[database]
migrations_dir = "migrations"
"#;
        assert_eq!(parse_app_schema(toml).unwrap(), None);
    }

    #[test]
    fn with_runtime_ui_dist() {
        let toml = r#"
[app]
id = "myapp"

[runtime]
ui_dist = "ui/dist"
"#;
        assert_eq!(parse_app_ui_dist(toml).unwrap(), Some("ui/dist".to_string()));
    }

    #[test]
    fn without_runtime_section() {
        let toml = r#"
[app]
id = "myapp"
"#;
        assert_eq!(parse_app_ui_dist(toml).unwrap(), None);
    }
}
