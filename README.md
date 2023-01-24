[![crates.io](https://img.shields.io/crates/v/dustdata?color=EA4342&style=flat-square)](https://crates.io/crates/dustdata)

# DustData

A data concurrency control storage engine to [Rustbase](https://github.com/rustbase/rustbase)

Join our [community](https://discord.gg/m5ZzWPumbd) and [chat](https://discord.gg/m5ZzWPumbd) with other Rust users.

# ⚠️ Warning

This is a work in progress. The API is not stable yet.

# 🔗 Contribute

[Click here](./CONTRIBUTING.md) to see how to Contribute

# Dependencies

These are dependencies that are required to use the DustData.

-   [bson](https://crates.io/crates/bson)

# How to install

Add the following to your `Cargo.toml`:

```toml
[dependencies]
dustdata = "1.3.0"
```

# Usage

Initialize a DustData interface.

```rust
// DustData Configuration
let config = dustdata::DustDataConfig {
    path: "./test_data".to_string(),
    lsm_config: dustdata::LsmConfig {
        flush_threshold: dustdata::Size::Megabytes(128),
    }
};

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
let value = dustdata.get("key").unwrap().unwrap();
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

-   [x] Memtable (06/19/22)
-   [x] SSTable (08/20/22)
-   [x] Snapshots (12/16/22)

# Authors

<div align="center">

| [<img src="https://github.com/peeeuzin.png?size=115" width=115><br><sub>@peeeuzin</sub>](https://github.com/peeeuzin) |
| :-------------------------------------------------------------------------------------------------------------------: |

</div>

# License

[MIT License](./LICENSE)
