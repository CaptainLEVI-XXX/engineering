use std::time::Duration;

use anyhow::{Context, Result, ensure};
use engineering_challenge::discovery::polymarket::{Command, Discovery, GAMMA_MARKETS, Gamma};

/// Separate from the supplied SBE example. Diagnostics only; stdout stays empty.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut seconds = 120;
    let mut once = false;
    let mut strict = false;
    let mut evidence_path = None;
    let mut evidence_max_bytes = 512 * 1024 * 1024_u64;
    let mut explicit_evidence_cap = false;
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
            "--evidence" => {
                evidence_path = Some(args.next().context("--evidence requires a new file path")?);
            }
            "--evidence-max-bytes" => {
                evidence_max_bytes = args
                    .next()
                    .context("--evidence-max-bytes requires a value")?
                    .parse()?;
                explicit_evidence_cap = true;
            }
            _ => anyhow::bail!(
                "usage: discovery [--seconds N] [--once] [--strict] [--evidence NEW_FILE] [--evidence-max-bytes N]"
            ),
        }
    }
    ensure!(seconds > 0, "duration must be positive");
    ensure!(
        !explicit_evidence_cap || evidence_path.is_some(),
        "--evidence-max-bytes requires --evidence"
    );
    let (mut discovery, commands, _changes) = Discovery::new(Gamma::new(GAMMA_MARKETS)?);
    if let Some(path) = evidence_path {
        discovery.enable_evidence(path, evidence_max_bytes)?;
    }
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = commands.send(Command::Stop).await;
        }
    });
    let report = discovery.run_for(Duration::from_secs(seconds), once).await;
    signal.abort();
    let _ = signal.await;
    eprintln!("{}", serde_json::to_string_pretty(&report)?);
    if let Some(status) = &report.evidence {
        ensure!(
            status.complete,
            "incomplete discovery evidence: {}",
            status.error.as_deref().unwrap_or("missing completion")
        );
    }
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
