[![crates.io](https://img.shields.io/crates/v/dustdata?color=EA4342&style=flat-square)](https://crates.io/crates/dustdata)


# DustData
A data concurrency control storage engine to [Rustbase](https://github.com/rustbase/rustbase)

# ⚠️ Warning 
This is a work in progress. The API is not stable yet.

# 🔗 Contribute
[Click here](./CONTRIBUTING.md) to see how to Contribute

# Dependencies
These are dependencies that are required to use the DustData.
 - [bson](https://crates.io/crates/bson)

# How to install
Add the following to your `Cargo.toml`:
```toml
[dependencies]
dustdata = "0.2.1"
```

# Usage
Initialize a DustData interface.
```rust
// DustData Configuration
let config = dustdata::DustDataConfig {
    path: "./test_data".to_string(),
    lsm_config: storage::lsm::LsmConfig {
        flush_threshold: 128 * 1024 * 1024, // 128MB
    },
    cache_size: 256 * 1024 * 1024, // 256MB
}

// Create a DustData interface
let mut dustdata = dustdata::initialize(config);
```

## Insert a data
```rust
// ...
// Creating a data
let data = bson::doc! {
    "name": "John Doe",
    "age": 30,
}

dustdata.insert("key", data);
```

## Getting a data
```rust
// ...
let value = dustdata.get("key");
println!("{:?}", value);
```

## Updating a data
```rust
// ...
let data = bson::doc! {
    "name": "Joe Mamma",
    "age": 42,
}

dustdata.update("key", data);
```

## Deleting a data
```rust
// ...
dustdata.delete("key");
```

# To-dos
 - [x] Memtable (06/19/22)
 - [x] SSTable (08/20/22)
 - [ ] Document oriented
 - [ ] Logs
 - [ ] Encryption

# Authors
<div align="center">

| [<img src="https://github.com/pedrinfx.png?size=115" width=115><br><sub>@pedrinfx</sub>](https://github.com/pedrinfx) |
| :-------------------------------------------------------------------------------------------------------------------: |
</div>

# License
[MIT License](./LICENSE)