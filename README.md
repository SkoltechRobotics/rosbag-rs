# rosbag-rs [![crates.io](https://img.shields.io/crates/v/rosbag.svg)](https://crates.io/crates/rosbag) [![Documentation](https://docs.rs/rosbag/badge.svg)](https://docs.rs/rosbag)

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

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.