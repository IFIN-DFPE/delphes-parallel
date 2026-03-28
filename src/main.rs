use std::fs::create_dir_all;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Instant;
use std::{fs::File, io::BufWriter};

use bytesize::ByteSize;
use hepmc2::{Reader as HepMC2Reader, Writer as HepMC2Writer};
use tempdir::TempDir;

fn main() {
    // Step 1: break input file into multiple shards

    let num_shards = 64usize;
    println!("Selected number of shards: {}", num_shards);

    let input_path = "/data/gmajeri/delphes-parallel/tests/events.hepmc";
    println!("Processing input file '{}'", input_path);

    let input_file = File::open(input_path).expect("Could not open input file");

    match input_file.metadata() {
        Ok(metadata) => {
            let file_size = metadata.len();
            println!("File size: {}", format_binary_size(file_size));

            println!(
                "Expected average shard size: {} (x {} shards)",
                format_binary_size(file_size / (num_shards as u64)),
                num_shards
            )
        }
        Err(err) => {
            println!(
                "Failed to read metadata for input file '{}': {}",
                input_path, err
            )
        }
    }

    let input_reader = BufReader::new(input_file);
    let events_reader = HepMC2Reader::from(input_reader);

    let tmp_dir = TempDir::new("delphes_parallel")
        .expect("Failed to create temporary directory for data shards");

    let working_dir = tmp_dir.path();

    // let working_dir = &PathBuf::from("/data/gmajeri/delphes-parallel/data/");

    let shards_directory = [working_dir, &PathBuf::from("shards")]
        .iter()
        .collect::<PathBuf>();

    println!(
        "Directory where shards will be stored: {}",
        shards_directory.as_os_str().to_string_lossy()
    );

    create_dir_all(&shards_directory).expect("Failed to create shards output subdirectory");

    let shard_paths = break_input_file_into_shards(events_reader, num_shards, working_dir);

    // let shard_paths = (0..num_shards)
    //     .map(|index| {
    //         [
    //             &shards_directory,
    //             &PathBuf::from(format!("shard_{}.hepmc2", index)),
    //         ]
    //         .iter()
    //         .collect::<PathBuf>()
    //     })
    //     .collect::<Vec<_>>();

    // Step 2: run Delphes in parallel

    let root_outputs_directory = [working_dir, &PathBuf::from("root_outputs")]
        .iter()
        .collect::<PathBuf>();

    println!(
        "Directory where processed .root shard outputs will be stored: {}",
        root_outputs_directory.as_os_str().to_string_lossy()
    );

    create_dir_all(&root_outputs_directory).expect("Failed to create ROOT outputs subdirectory");

    let root_files_paths = process_shards(&shard_paths, &root_outputs_directory);

    // let root_files_paths = (0..num_shards)
    //     .map(|index| {
    //         [
    //             &root_outputs_directory,
    //             &PathBuf::from(format!("shard_{}.root", index)),
    //         ]
    //         .iter()
    //         .collect::<PathBuf>()
    //     })
    //     .collect::<Vec<_>>();

    // Step 3: recombine output ROOT files

    println!("Using ROOT's `hadd` command to merge output .root files");

    let output_file_path = "/data/gmajeri/delphes-parallel/tests/output-merged.root";
    let mut input_file_paths = root_files_paths
        .iter()
        .map(|path| path.to_str().unwrap())
        .collect();

    let mut arguments = vec![output_file_path];
    arguments.append(&mut input_file_paths);

    let status = Command::new("hadd")
        .args(arguments)
        .status()
        .expect("Failed to run ROOT's `hadd` tool");

    if !status.success() {
        panic!("ROOT's `hadd` tool returned non-zero status code");
    }

    println!("Done");
}

fn format_binary_size(bytes: u64) -> String {
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

fn break_input_file_into_shards(
    events_reader: HepMC2Reader<BufReader<File>>,
    num_shards: usize,
    output_directory: &Path,
) -> Vec<PathBuf> {
    let mut shard_paths = Vec::<PathBuf>::with_capacity(num_shards);
    let mut shard_writers = Vec::<HepMC2Writer<BufWriter<File>>>::with_capacity(num_shards);

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

    for events_writer in shard_writers {
        events_writer
            .finish()
            .expect("Failed to finish writing events file");
    }

    let events_count = event_index;
    let duration = start_time.elapsed().as_secs_f64();
    println!(
        "Splitting input file into shards took {:.2} seconds ({} events were processed)",
        duration, events_count
    );

    shard_paths
}

fn process_shards(shard_paths: &Vec<PathBuf>, output_directory: &Path) -> Vec<PathBuf> {
    let num_shards = shard_paths.len();
    println!("Spawning {} parallel Delphes subprocesses...", num_shards);

    let start_time = Instant::now();

    let delphes_config_file_path =
        "/data/bsm/software/installed/delphes/3.5.1/cards/delphes_card_ATLAS.tcl";

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
                delphes_config_file_path,
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
