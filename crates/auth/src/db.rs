use chrono::{DateTime, FixedOffset, Utc};
use sea_orm::{ActiveModelTrait, ConnectOptions, Database, DatabaseConnection, Set, entity::prelude::*};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(schema_name = "public", table_name = "api_keys")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub user_id: Uuid,
    pub token: String,
    pub expires_at: Option<DateTime<FixedOffset>>,
    pub last_used_at: Option<DateTime<FixedOffset>>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Debug)]
pub struct VerifiedUser {
    pub user_id: Uuid,
    pub api_key_id: Uuid,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("{0}")]
    MissingDatabaseUrl(String),
    #[error("database connection error: {0}")]
    Connect(#[from] sea_orm::DbErr),
    #[error("invalid or unknown token")]
    InvalidToken,
    #[error("token has expired")]
    Expired,
}

pub fn load_database_url() -> Result<String, AuthError> {
    let mut dir_opt = std::env::current_dir().ok();
    for _ in 0..=5 {
        match dir_opt {
            Some(ref d) => {
                let env_path = d.join(".env");
                if env_path.exists() {
                    let _ = dotenvy::from_path(&env_path);
                    break;
                }
                dir_opt = d.parent().map(std::path::Path::to_path_buf);
            }
            None => break,
        }
    }
    std::env::var("DATABASE_URL").map_err(|_| {
        AuthError::MissingDatabaseUrl(
            "DATABASE_URL not found. Add it to a .env file or set the DATABASE_URL environment variable.".to_string(),
        )
    })
}

pub async fn connect_db() -> Result<DatabaseConnection, AuthError> {
    let url = load_database_url()?;
    let mut opts = ConnectOptions::new(url);
    opts.sqlx_logging(false);
    Ok(Database::connect(opts).await?)
}

pub async fn verify_token(db: &DatabaseConnection, token: &str) -> Result<VerifiedUser, AuthError> {
    // Schema migration (commit d21c2c68f): api_keys now stores tokens in
    // plaintext (column `token`, unique index). Previously this was a
    // SHA-256 hash in `key_hash`; the column is gone. Look up by plaintext.
    let row = Entity::find()
        .filter(Column::Token.eq(token))
        .one(db)
        .await?
        .ok_or(AuthError::InvalidToken)?;

    let now_fixed: DateTime<FixedOffset> = Utc::now().into();
    if let Some(expires) = row.expires_at
        && expires < now_fixed
    {
        return Err(AuthError::Expired);
    }

    let id = row.id;
    let user_id = row.user_id;

    let db_clone = db.clone();
    tokio::spawn(async move {
        let am = ActiveModel {
            id: Set(id),
            last_used_at: Set(Some(Utc::now().into())),
            ..Default::default()
        };
        if let Err(e) = am.update(&db_clone).await {
            tracing::warn!(error = %e, "failed to update api_key last_used_at");
        }
    });

    Ok(VerifiedUser {
        user_id,
        api_key_id: id,
    })
}
