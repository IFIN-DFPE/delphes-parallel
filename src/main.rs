use std::io::BufReader;
use std::time::Instant;
use std::{fs::File, io::BufWriter};

use hepmc2::{Reader as HepMC2Reader, Writer as HepMC2Writer};

fn main() {
    let input_file = File::open("/data/gmajeri/delphes-parallel/tests/events.hepmc")
        .expect("Could not open input file");
    let input_reader = BufReader::new(input_file);
    let events_reader = HepMC2Reader::from(input_reader);

    let output_file = File::create("/data/gmajeri/delphes-parallel/tests/copied.hepmc")
        .expect("Unable to create output file");
    let output_writer = BufWriter::new(output_file);
    let mut events_writer = HepMC2Writer::try_from(output_writer)
        .expect("Failed to configure buffered writer for events output");

    let mut events_count = 0;
    let start_time = Instant::now();

    let mut batch_start_time = Instant::now();

    for event in events_reader {
        let event = event.expect("Failed to parse line in HepMC2 file");
        events_writer
            .write(&event)
            .expect("Failed to write event to output file");
        events_count += 1;

        if events_count % 100 == 0 {
            let batch_duration = batch_start_time.elapsed().as_secs_f64();
            let per_event_duration = batch_duration / 100.0;
            println!(
                "Written {} events in about {:.5} seconds/event",
                events_count, per_event_duration
            );
            batch_start_time = Instant::now();
        }
    }

    events_writer
        .finish()
        .expect("Failed to finish writing events file");

    let duration = start_time.elapsed().as_secs_f64();
    println!(
        "Copying {} events took {:.2} seconds",
        events_count, duration
    )
}
