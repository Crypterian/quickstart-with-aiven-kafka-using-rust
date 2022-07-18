use std::error::Error;
use std::thread::sleep;
use std::time::Duration;

use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::client::SecurityConfig;

use openssl::ssl::{SslConnector, SslFiletype, SslMethod};

use quickstart_with_aiven_kafka_using_rust::TempratureSensor;


fn main() -> Result<(), Box<dyn Error>>{
    println!("Running rust-kafka sync!");

    let topic = "iot";
    //CHANGE ME
    let bootstrap_servers = vec!["{KAFKA_SERVICE}.aivencloud.com:{PORT}".to_string()];
    let sleep_time_s = 5;

    let service_cert = "service.cert";
    let service_key = "service.key";
    let ca_cert = "ca.pem";

    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_certificate_file(service_cert, SslFiletype::PEM)?;
    builder.set_private_key_file(service_key, SslFiletype::PEM)?;
    builder.set_ca_file(ca_cert)?;
    builder.check_private_key()?;

    let connector = builder.build();

    let mut producer = Producer::from_hosts(bootstrap_servers)
        .with_ack_timeout(Duration::from_secs(5))
        .with_required_acks(RequiredAcks::One)
        .with_security(SecurityConfig::new(connector).with_hostname_verification(true))
        .create()?;

    let bedroom = TempratureSensor::new("bedroom".to_string());
    let livingroom = TempratureSensor::new("livingroom".to_string());

        
    loop {
        producer.send(&Record::from_value(topic, bedroom.get_measurement_json()?))?;
        producer.send(&Record::from_value(topic, livingroom.get_measurement_json()?))?;

        sleep(Duration::from_secs(sleep_time_s));
    }
}