use serde::Serialize;

#[derive(Debug, Serialize)]
struct GpsTelemetry {
    event_id: String,
    device_id: String,
    latitude: f64,
    longitude: f64,
    speed_kmh: f64,
}

#[tokio::main]
async fn main() {
    let telemetry = GpsTelemetry {
        event_id: "GPS-RUST-001".to_string(),
        device_id: "ORBI-001".to_string(),
        latitude: 5.6037,
        longitude: -0.1870,
        speed_kmh: 42.5,
    };

    println!("GPS telemetry prepared:");
    println!("{telemetry:#?}");
}
