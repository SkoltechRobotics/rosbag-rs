# rosbag-rs

[![Crate][crate-image]][crate-link]
[![Docs][docs-image]][docs-link]
![Apache2/MIT licensed][license-image]
![Rust Version][rustc-image]
[![Build Status][build-image]][build-link]
[![Dependency Status][deps-image]][deps-link]

A pure Rust crate for reading ROS bag files.

## Example
```rust
use rosbag::{RosBag, Record};

let bag = RosBag::new(path).unwrap();
// create low-level iterator over rosbag records
let mut records = bag.records();
// acquire `BagHeader` record, which should be first one
let header = match records.next() {
    Some(Ok(Record::BagHeader(bh))) => bh,
    _ => panic!("Failed to acquire bag header record"),
};
// get first `Chunk` record and iterate over `Message` records in it
for record in &mut records {
    match record? {
        Record::Chunk(chunk) => {
            for msg in chunk.iter_msgs() {
                println!("{}", msg?.time)
            }
            break;
        },
        _ => (),
    }
}
// jump to index records
records.seek(header.index_pos).unwrap();
for record in records {
    println!("{:?}", record?);
}
```

## Minimum Supported Rust Version

Rust **1.56** or higher.

Minimum supported Rust version can be changed in the future, but it will be
done with a minor version bump.

## License

The crate is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.

[//]: # (badges)

[crate-image]: https://img.shields.io/crates/v/rosbag.svg
[crate-link]: https://crates.io/crates/rosbag
[docs-image]: https://docs.rs/rosbag/badge.svg
[docs-link]: https://docs.rs/rosbag
[rustc-image]: https://img.shields.io/badge/rustc-1.56+-blue.svg
[license-image]: https://img.shields.io/badge/license-Apache2.0/MIT-blue.svg
[build-image]: https://github.com/SkoltechRobotics/rosbag-rs/actions/workflows/rosbag.yml/badge.svg
[build-link]: https://github.com/SkoltechRobotics/rosbag-rs/actions/workflows/rosbag.yml
[deps-image]: https://deps.rs/repo/github/SkoltechRobotics/rosbag-rs/status.svg
[deps-link]: https://deps.rs/repo/github/SkoltechRobotics/rosbag-rs
