use std::{
    fs::File,
    io::{BufRead, BufWriter},
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Instant,
};

use bytesize::ByteSize;
use hepmc2::{Reader as HepMC2Reader, Writer as HepMC2Writer};

/// Given a file size (in bytes), returns it as a human-readable string
/// (possibly in another binary unit).
pub fn format_binary_size(bytes: u64) -> String {
    format_bytesize(ByteSize::b(bytes))
}

fn format_bytesize(size: ByteSize) -> String {
    if size <= ByteSize::kib(1) {
        format!("{:.2} bytes", size)
    } else if size <= ByteSize::mib(1) {
        format!("{:.2} KiB", size.as_kib())
    } else if size <= ByteSize::gib(1) {
        format!("{:.2} MiB", size.as_mib())
    } else {
        format!("{:.2} GiB", size.as_gib())
    }
}

/// Given an input HepMC2 reader object, reads all events and spreads them evenly
/// across a number of file shards. Returns the vector of paths to the shards.
pub fn split_input_into_shards<R: BufRead>(
    events_readers: &mut [HepMC2Reader<R>],
    num_shards: usize,
    output_directory: &Path,
) -> Vec<PathBuf> {
    let mut shard_paths = Vec::<PathBuf>::with_capacity(num_shards);
    let mut shard_writers = Vec::<HepMC2Writer<BufWriter<File>>>::with_capacity(num_shards);

    // Initialize/open the shard files
    for index in 0..num_shards {
        let shard_path: PathBuf = [
            output_directory,
            &PathBuf::from(format!("shard_{}.hepmc2", index)),
        ]
        .iter()
        .collect();

        let output_file =
            File::create(&shard_path).expect("Failed to create file for output shard");
        let output_writer = BufWriter::new(output_file);
        let events_writer = HepMC2Writer::try_from(output_writer)
            .expect("Failed to configure writer for events output");

        shard_paths.push(shard_path);
        shard_writers.push(events_writer);
    }

    let mut event_index = 0usize;
    let start_time = Instant::now();

    let mut batch_start_time = Instant::now();

    // Go through each input reader and consume all events from it
    for events_reader in events_readers.iter_mut() {
        for event in events_reader {
            let event = event.expect("Failed to parse line in HepMC2 file");

            let shard_index = event_index % num_shards;

            let events_writer = &mut shard_writers[shard_index];
            events_writer
                .write(&event)
                .expect("Failed to write event to output file");
            event_index += 1;

            if event_index % 100 == 0 {
                let batch_duration = batch_start_time.elapsed().as_secs_f64();
                let per_event_duration = batch_duration / 100.0;
                println!(
                    "Written {} events in about {:.5} seconds/event",
                    event_index, per_event_duration
                );
                batch_start_time = Instant::now();
            }
        }
    }

    // Finalize all the files (write the standard HepMC2 epilogue),
    // then implicitly close all the files (since we move the `shard_writers` here)
    for events_writer in shard_writers {
        events_writer
            .finish()
            .expect("Failed to finish writing events file");
    }

    // Print some statistics
    let events_count = event_index;
    let duration = start_time.elapsed().as_secs_f64();
    println!(
        "Splitting input file into shards took {:.2} seconds ({} events were processed)",
        duration, events_count
    );

    shard_paths
}

/// Given a list of HepMC2 files (the data shards),
/// launches an independent Delphes process to process each in parallel.
///
/// Returns the list of output ROOT file paths.
pub fn process_shards(
    shard_paths: &Vec<PathBuf>,
    delphes_config_file_path: &Path,
    output_directory: &Path,
) -> Vec<PathBuf> {
    let num_shards = shard_paths.len();
    println!("Spawning {} parallel Delphes subprocesses...", num_shards);

    let start_time = Instant::now();

    let mut root_output_paths = Vec::<PathBuf>::with_capacity(num_shards);
    let mut children = Vec::<Child>::with_capacity(num_shards);

    for index in 0..num_shards {
        let output_file_path: PathBuf = [
            output_directory,
            &PathBuf::from(format!("shard_{}.root", index)),
        ]
        .iter()
        .collect();
        let input_file_path = &shard_paths[index];

        let child = Command::new("DelphesHepMC2")
            .args([
                delphes_config_file_path.to_str().unwrap(),
                output_file_path.to_str().unwrap(),
                input_file_path.to_str().unwrap(),
            ])
            .spawn()
            .expect("Failed to launch Delphes process");

        root_output_paths.push(output_file_path);
        children.push(child);
    }

    let duration = start_time.elapsed().as_secs_f64();
    println!(
        "Launching {} Delphes processes took {:4} seconds",
        num_shards, duration
    );

    // Now wait for them to finish
    println!("Waiting for Delphes subprocesses to all finish successfully");

    for mut child in children {
        let status = child
            .wait()
            .expect("Failed to wait for Delphes child process to finish");

        if !status.success() {
            panic!(
                "Delphes child process finished with non-success exit status {:?}",
                status
            );
        }
    }

    root_output_paths
}

/// Merges the given list of ROOT files (as outputed from Delphes)
/// into a single large ROOT file (merging common trees).
///
/// Uses the `hadd` tool from ROOT, so it requires the ROOT framework to be installed.
/// Since it's a dependency of Delphes, this should not be a problem.
pub fn merge_shards(root_files_paths: &Vec<PathBuf>, output_file_path: &Path) {
    let mut input_file_paths = root_files_paths
        .iter()
        .map(|path| path.to_str().unwrap())
        .collect();

    let mut arguments = vec![output_file_path.to_str().unwrap()];
    arguments.append(&mut input_file_paths);

    let status = Command::new("hadd")
        .args(arguments)
        .status()
        .expect("Failed to run ROOT's `hadd` tool");

    if !status.success() {
        panic!("ROOT's `hadd` tool returned non-zero status code");
    }
}
