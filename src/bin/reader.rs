extern crate rosbag;

use rosbag::{Record, RecordsIterator};
use std::env;

use rosbag::record_types::{
    BagHeader, Chunk, Connection, MessageData, IndexData, ChunkInfo,
};

use rosbag::msg_iter::ChunkRecord;

fn main() {
    let path = "/media/newpavlov/DATA/2011-03-28-08-38-59.bag";
    let mut bag = RecordsIterator::new(path).unwrap();
    let header = match bag.next() {
        Some(Ok(Record::BagHeader(bh))) => bh,
        _ => panic!("Failed to acquire bag header record"),
    };
    // get first chunk and iterate over its content
    for record in &mut bag {
        let record = record.unwrap();
        match record {
            Record::Chunk(chunk) => {
                for msg in chunk.iter_msgs() {
                    println!("{:?}", msg.unwrap().time)
                }
                //break;
            },
            _ => (),
        }
    }
    // jump to index records
    bag.seek(header.index_pos).unwrap();
    for record in bag {
        let record = record.unwrap();
        //println!("{:?}", record);
    }
}
