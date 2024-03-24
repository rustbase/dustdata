fn main() {
    let run_hooky = std::env::var("RUN_HOOKY");

    if run_hooky == Ok("true".to_owned()) {
        hooky::init(true)
    }
}
