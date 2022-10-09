#[macro_export]
macro_rules! dd_println {
    () => (print!("\n"));
    ($($arg:tt)*) => ({
        print!("[DustData] ");
        println!($($arg)*);
    })
}
