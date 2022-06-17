#[macro_export]
macro_rules! ddprintln {
    ($($arg:tt)*) => (if cfg!(debug_assertions) {
        print!("DustData -> ");
        println!($($arg)*);
    })
}

pub(crate) use ddprintln;