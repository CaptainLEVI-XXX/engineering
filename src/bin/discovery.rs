use std::time::Duration;

use anyhow::{Context, Result, ensure};
use engineering_challenge::discovery::polymarket::{Command, Discovery, GAMMA_MARKETS, Gamma};

/// Separate from the supplied SBE example. Diagnostics only; stdout stays empty.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut seconds = 120;
    let mut once = false;
    let mut strict = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--seconds" => {
                seconds = args
                    .next()
                    .context("--seconds requires a value")?
                    .parse::<u64>()?
            }
            "--once" => once = true,
            "--strict" => strict = true,
            _ => anyhow::bail!("usage: discovery [--seconds N] [--once] [--strict]"),
        }
    }
    ensure!(seconds > 0, "duration must be positive");
    let (mut discovery, commands, _changes) = Discovery::new(Gamma::new(GAMMA_MARKETS)?);
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = commands.send(Command::Stop).await;
        }
    });
    let report = discovery.run_for(Duration::from_secs(seconds), once).await;
    signal.abort();
    let _ = signal.await;
    eprintln!("{}", serde_json::to_string_pretty(&report)?);
    if strict {
        ensure!(
            report.coverage_accepted(),
            "strict discovery acceptance failed; inspect coverage report"
        );
    }
    ensure!(
        report.scans.iter().any(|s| s.pages > 0),
        "no discovery page succeeded"
    );
    Ok(())
}
