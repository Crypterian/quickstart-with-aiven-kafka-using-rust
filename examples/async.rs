use std::error::Error;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use tokio::try_join;
use tokio::time::sleep;

use quickstart_with_aiven_kafka_using_rust::TempratureSensor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    println!("Running librdkafka async!");

    let topic = "iot";
    //CHANGE ME
    let bootstrap_servers = "{KAFKA_SERVICE}.aivencloud.com:{PORT}";
    let queue_timeout_s = 5;
    let message_timeout_ms = "5000";
    let security_protocol = "ssl";
    let sleep_time_s = 5;

    let service_cert = "service.cert";
    let service_key = "service.key";
    let ca_cert = "ca.pem";

    let producer: FutureProducer = ClientConfig::new()
    .set("bootstrap.servers", bootstrap_servers)
    .set("ssl.certificate.location", service_cert)
    .set("ssl.key.location", service_key)
    .set("ssl.ca.location", ca_cert)
    .set("message.timeout.ms", message_timeout_ms)
    .set("security.protocol", security_protocol)
    .create()?;


    let bedroom = TempratureSensor::new("bedroom".to_string());
    let livingroom = TempratureSensor::new("livingroom".to_string());

    loop {
        //Perform both send operations concurrently. Will continue once both has completed.
        try_join!(
            send(&producer, topic, &bedroom, queue_timeout_s), 
            send(&producer, topic, &livingroom, queue_timeout_s)
        )?;

        //Sleep without blocking the thread and wakeup after specified time.
        sleep(Duration::from_secs(sleep_time_s)).await;
    }
}

async fn send(producer: &FutureProducer, topic: &str, sensor: &TempratureSensor, timeout_s: u64) -> Result<(), Box<dyn Error>> {
    
    let mesurement = sensor.get_measurement_json()?;
    let record = FutureRecord::<String, String>::to(topic).payload(&mesurement);
    
    
    let result = producer.send(
        record,
        Duration::from_secs(timeout_s)
    ).await;
    
    match result {
        Ok(_) => Ok(()),
        Err((error, _)) => Err(error)?,
    }
}