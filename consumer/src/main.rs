use kafka::consumer::{Consumer, FetchOffset};

fn main() {
    let mut consumer =
        Consumer::from_host(vec!("broker:9092".to_owned()))
            .with_topic("hhstories".to_owned())
            .with_fallback_offset(FetchOffset::Earliest)
            .create()
            .unwrap();
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                let str = String::from_utf8_lossy(m.value);
                println!("{:?}", str);
            }
            let _ = consumer.consume_messageset(ms);
        }
        consumer.commit_consumed().unwrap();
    }
}
