
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {{
        use std::time::SystemTime;

        let now = SystemTime::now();
        let timestamp = match now.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        };
        println!("[INFO ][{}] {}", timestamp, format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {{
        use std::time::SystemTime;

        let now = SystemTime::now();
        let timestamp = match now.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        };
        println!("[WARN ][{}] {}", timestamp, format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {{
        use std::time::SystemTime;

        let now = SystemTime::now();
        let timestamp = match now.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        };
        println!("[ERROR][{}] {}", timestamp, format_args!($($arg)*));
    }};
}