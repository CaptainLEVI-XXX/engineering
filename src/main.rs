//! Collector entry point: the runtime, options and exit status only.
use engineering_challenge::collector::{self, Options};
use std::{process::ExitCode, time::Duration};

fn main() -> ExitCode {
    let result = Options::parse(std::env::args().skip(1)).and_then(|options| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .map_err(anyhow::Error::from)
            .and_then(|runtime| {
                let result = runtime.block_on(collector::run(options));
                runtime.shutdown_timeout(Duration::from_secs(1));
                result
            })
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("collector_failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}
