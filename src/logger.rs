use log::{Level, Log, Metadata, Record, SetLoggerError};

pub struct Logger {
    level: Level,
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        println!("[{}] {}", record.level(), record.args())
    }

    fn flush(&self) {}
}

pub fn init(max_level: Level) -> Result<(), SetLoggerError> {
    log::set_boxed_logger(Box::new(Logger { level: max_level }))?;
    log::set_max_level(max_level.to_level_filter());
    Ok(())
}
