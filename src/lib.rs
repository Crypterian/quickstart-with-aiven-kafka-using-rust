use std::error::Error;

use serde::Serialize;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use rand::Rng;



#[derive(Serialize, Debug)]
pub struct SensorReading {
    id: Uuid,
    timestamp: DateTime<Utc>,
    location: String,
    temperature: f64,
}

pub struct TempratureSensor {
    location: String,
}


impl TempratureSensor {
    pub fn new(location: String) -> Self {
        Self { location }
    }

    pub fn measure(&self) -> SensorReading {

        SensorReading {
            id: Uuid::new_v4(), 
            timestamp: Utc::now(), 
            location: self.location.clone(), 
            temperature: rand::thread_rng().gen_range(12.0..=30.0),
        }
    }

    pub fn get_measurement_json(&self) -> Result<String, Box<dyn Error>> {
        Ok(serde_json::to_string(&self.measure())?)
    }
}