use anyhow::Result;
use clap::Parser;
use iroh::{Endpoint, SecretKey};
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short = 'k', long = "secret-key")]
    secret_key: Option<String>,

    #[arg(short = 'f', long = "secret-key-file")]
    secret_file: Option<String>,

    #[arg(short = 'p', long = "print-secret-key")]
    print_secret_key: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("----- Viduo -----");

    let args = Args::parse();

    // Secret key priority order
    // 1. From argument
    // 2. From file
    // 3. From env
    // 4. Generate new key
    let secret_key = match (
        args.secret_key,
        args.secret_file,
        std::env::var("VIDUO_SECRET_KEY"),
    ) {
        (Some(secret_key_arg), _, _) => SecretKey::from_str(&secret_key_arg)?,
        (_, Some(secret_key_file), _) => {
            let secret_key = std::fs::read_to_string(secret_key_file)?;
            SecretKey::from_str(&secret_key)?
        }
        (_, _, Ok(secret_key_env)) => SecretKey::from_str(&secret_key_env)?,
        _ => SecretKey::generate(rand::rngs::OsRng),
    };
    if args.print_secret_key {
        println!("Using secret key: {}", secret_key);
    }

    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        // Enable n0 discovery. This allows you to
        // dial by `NodeId`, and allows you to be
        // dialed by `NodeId`.
        .discovery_n0()
        .bind()
        .await?;
    println!("Endpoint node id: {}", endpoint.node_id());

    Ok(())
}
