use std::{
    fs::OpenOptions,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing_subscriber::{
    Layer,
    filter::{LevelFilter, Targets},
    fmt::{self, time::FormatTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

pub fn init_logging(log_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(log_dir)?;

    let raft_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("raft.log"))?;

    let counter_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("counter.log"))?;

    let raft_layer = fmt::layer()
        .with_writer(raft_file)
        .with_target(true)
        .with_ansi(false)
        .with_timer(NanoTime)
        .with_filter(Targets::new().with_target("raft", tracing::Level::INFO));

    let counter_layer = fmt::layer()
        .with_writer(counter_file)
        .with_target(true)
        .with_ansi(false)
        .with_timer(NanoTime)
        .with_filter(Targets::new().with_target("counter", tracing::Level::INFO));

    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_timer(NanoTime)
        .with_filter(LevelFilter::TRACE);

    tracing_subscriber::registry()
        .with(raft_layer)
        .with(counter_layer)
        .with(stdout_layer)
        .init();

    Ok(())
}

struct NanoTime;
impl FormatTime for NanoTime {
    fn format_time(&self, w: &mut fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        write!(w, "{}.{:9}", now.as_secs(), now.subsec_nanos())
    }
}
