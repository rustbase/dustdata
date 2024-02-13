[![crates.io](https://img.shields.io/crates/v/dustdata?color=EA4342&style=flat-square)](https://crates.io/crates/dustdata)

# DustData
A data concurrency control storage engine to [Rustbase](https://github.com/rustbase/rustbase)

Join our [community](https://discord.gg/m5ZzWPumbd) and [chat](https://discord.gg/m5ZzWPumbd) with other Rust users.

# ⚠️ Warning
This is a work in progress. The API is not stable yet.

# 🔗 Contribute
[Click here](./CONTRIBUTING.md) to see how to Contribute

# How to install
Add the following to your `Cargo.toml`:

```toml
[dependencies]
dustdata = "2.0.0-beta.1"
```

# Usage
Initialize a new `DustData` instance with the default configuration:
```rust
use dustdata::DustData;

let mut dustdata = DustData::new(Default::default()).unwrap();
```

## Inserting data into a collection

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
struct User {
    name: String,
    age: i32,
}

let collection = dustdata.collection::<User>("users");

let user = User {
    name: "Pedro".to_string(),
    age: 21,
};

// Creating a new transaction.
let mut transaction = collection.start();

// Inserting the user into the transaction.
transaction.insert("user:1", user);

// Committing the transaction.
collection.commit(&mut transaction).unwrap();

// Done!
```

## Reading data from a collection

```rust
let collection = dustdata.collection::<User>("users").unwrap();

let user = collection.get("user:1").unwrap();
```


# Authors

<div align="center">

| [<img src="https://github.com/peeeuzin.png?size=115" width=115><br><sub>@peeeuzin</sub>](https://github.com/peeeuzin) |
| :-------------------------------------------------------------------------------------------------------------------: |

</div>

# License

[MIT License](./LICENSE)
