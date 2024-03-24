fn main() {
    let run_hooky = std::env::var("RUN_HOOKY").unwrap();

    if run_hooky == "true" {
        hooky::init(true)
    }
}
