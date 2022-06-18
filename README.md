# DustData
A data concurrency control storage engine to [Rustbase](https://github.com/rustbase/rustbase)

# 🔗 Contribute
[Click here](./CONTRIBUTING.md) to see how to Contribute

# Dependencies
 - [bson](https://crates.io/crates/bson)

# Usage
Initialize a DustData interface.
```rust
// DustData Configuration
let config = dustdata::DustDataConfig {
    path: "./test_data".to_string(),
    lsm_config: storage::lsm::LsmConfig {
        flush_threshold: 128,
        sstable_path: "./test_data/sstable".to_string(),
    },
    cache_size: 256,
}

// Create a DustData interface
let mut dustdata = dustdata::initialize(config);
```

## Insert a data
```rust
// ...
// Creating a data
let data = bson::doc! {
    "name": "DustData",
    "version": "0.1.0",
    "description": "A data concurrency control storage engine to Rustbase",
    "author": "DustData",
    "license": "MIT"
}

dustdata.insert("key", data);
```

## Getting a data
```rust
// ...
let value = dustdata.get("key");
println!("{:?}", value);
```

## Deleting a data
```rust
// ...
dustdata.delete("key");
```

# To-dos
 - [x] Memtable 
 - [ ] SStable
 - [ ] Logs
 - [ ] Encryption

# Authors
<div align="center">

| [<img src="https://github.com/pedrinfx.png?size=115" width=115><br><sub>@pedrinfx</sub>](https://github.com/pedrinfx) |
| :-------------------------------------------------------------------------------------------------------------------: |
</div>

# License
[MIT License](./LICENSE)