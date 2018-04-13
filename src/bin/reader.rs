extern crate rosbag;

use rosbag::{Record, RecordsIterator};
use std::io::SeekFrom;
use std::env;

use rosbag::record_types::{
    BagHeader, Chunk, Connection, MessageData, IndexData, ChunkInfo,
};

fn main() {
    // /media/newpavlov/DATA/2011-03-28-08-38-59.bag
    println!("BagHeader {:?}", std::mem::size_of::<BagHeader>());
    println!("Chunk {:?}", std::mem::size_of::<Chunk>());
    println!("Connection {:?}", std::mem::size_of::<Connection>());
    println!("MessageData {:?}", std::mem::size_of::<MessageData>());
    println!("IndexData {:?}", std::mem::size_of::<IndexData>());
    println!("ChunkInfo {:?}", std::mem::size_of::<ChunkInfo>());
    println!("Record {:?}", std::mem::size_of::<Record>());

    let path = env::args().nth(1).expect("Provide bag file");
    let mut bag = RecordsIterator::new(path)
        .expect("failed to open");
    bag.seek(SeekFrom::Start(2030904831)).unwrap();
    //bag.seek(SeekFrom::Start(2030307104)).unwrap();
    // start_time: 1565464126985122541, end_time: 3692201180274930413
    let mut i = 0;
    let chunk = loop {
        let record = match bag.next() {
            Some(v) => v,
            None => break,
        };
        i += 1;
        //if i == 100 {break}
        match record.unwrap() {
            //Record::Chunk(v) => break v,
            //Record::IndexData(v) => println!("{} {:?}", i, v),
            Record::BagHeader(v) => println!("{} {:?}", i, v),
            //Record::Connection(v) => println!("{} {:?}", i, v),
            //Record::MessageData(v) => println!("{} {:?}", i, v),
            Record::ChunkInfo(v) => println!("{} {:?}", i, v),
            v => println!("{} {}", i, v.get_type()),
            _ => (),
        }
    };
    println!("processed records: {}", i);
    /*
    let msg = chunk.iter().nth(0).unwrap().unwrap();
    println!("{} {}", msg.time, msg.data.len());
    println!("{:?}", &msg.data[..100]);

    //println!("records: {:?} {}", i, std::mem::size_of::<Record>());
    // start_time: 1334532321118827242, end_time: 1390498304786215659
    */
}
