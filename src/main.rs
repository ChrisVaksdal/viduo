use anyhow::Result;
use clap::Parser;
use iroh::{Endpoint, SecretKey};
use std::io;
use std::str::FromStr;

mod vidial;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short = 'k', long = "secret-key")]
    secret_key: Option<String>,

    #[arg(short = 'f', long = "secret-key-file")]
    secret_file: Option<String>,

    #[arg(short = 'p', long = "print-secret-key", default_value = "false")]
    print_secret_key: Option<bool>,
}

fn user_input(prompt: &str) -> String {
    if !prompt.is_empty() {
        print!("{}", prompt);
    }
    let mut message = String::new();
    io::stdin()
        .read_line(&mut message)
        .expect("error: unable to read user input");
    message.remove(message.len() - 1);
    message
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

    if args.print_secret_key.unwrap() {
        println!("Using secret key: {}", secret_key);
    }

    let vidial = vidial::ViDial::spawn(secret_key).await;
    println!("Endpoint node id: {}", vidial.endpoint().node_id());

    println!("Enter peer id (leave blank to only wait for incoming connections)");
    let peer_id = user_input("peer id: ");

    if !peer_id.is_empty() {}

    loop {
        vidial
            .send_message(iroh::NodeId::from_str(&peer_id)?, user_input("message>: "))
            .await?;
        // tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    Ok(())
}
