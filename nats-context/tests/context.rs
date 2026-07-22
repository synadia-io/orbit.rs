use std::path::PathBuf;

use nats_context::Settings;

#[test]
fn read_settings_test() {
    let demo_path = PathBuf::from("tests/contexts/demo.json");
    let demo_settings = Settings::read_from_file(demo_path).expect("read settings");
    assert_eq!(
        demo_settings,
        Settings {
            url: "nats://demo.nats.io:4222".to_string(),
            ..Default::default()
        }
    );
}
