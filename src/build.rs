fn main() {
    if option_env!("CARGO_PRIMARY_PACKAGE").is_some() {
        hooky::init(true)
    }
}
