#[macro_export]
macro_rules! logs {
    () => (print!("\n"));
    ($($arg:tt)*) => ({
        print!("[DustData] ");
        println!($($arg)*);
    })
}
