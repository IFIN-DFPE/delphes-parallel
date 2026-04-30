use std::env;
use std::fs::{File, canonicalize, create_dir_all, exists, remove_dir_all};
use std::io::{BufReader, stdin};
use std::mem::forget;
use std::path::{Path, PathBuf, absolute};
use std::process::exit;
use std::vec::Vec;

use hepmc2::Reader as HepMC2Reader;

use clap::Parser;

use delphes_parallel::{format_binary_size, merge_shards, process_shards, split_input_into_shards};
use tempdir::TempDir;

#[derive(Parser)]
#[command(
    version,
    about = "Processes HepMC2 input files through Delphes, with multicore parallelization support"
)]
struct Cli {
    /// Configuration file path (Tcl format).
    #[arg(required = true)]
    config_file: PathBuf,

    /// Output file path (ROOT format).
    #[arg(required = true)]
    output_file: PathBuf,

    /// Input file(s) path (HepMC2 format).
    ///
    /// When no input files are provided, or when input file is `-`,
    /// reads HepMC2 data from standard input.
    #[arg(required = false)]
    input_files: Vec<PathBuf>,

    /// Directory where temporary files will be stored (shards, ROOT outputs).
    ///
    /// Must be on a disk with free space equal to at least twice the size of the input files
    /// (we'll have to store the shards of the input files, as well as
    /// the processed ROOT outputs for the shards).
    ///
    /// If not set, will create a temporary directory for this purpose
    /// (which will be deleted after the program ends, see the `cleanup` flag).
    #[arg(long)]
    working_directory: Option<PathBuf>,

    /// Controls whether the created temporary directory
    /// will be deleted (along with its contents) at the end of the program.
    #[arg(long, default_value_t = true)]
    cleanup: bool,

    /// Path to original DelphesHepMC2 executable.
    #[arg(long)]
    delphes_hepmc2_executable: Option<PathBuf>,

    /// How many shards to break the input file(s) into.
    ///
    /// Limits the amount of parallelization which can be performed.
    /// Makes no sense to be larger than the number of (physical) processor cores on the system.
    /// If not set, will automatically configure it based on the number of cores the system has.
    #[arg(long)]
    num_shards: Option<usize>,
}

fn main() {
    let cli: Cli = Cli::parse();

    println!("Starting `DelphesHepMC2` parallelization wrapper...");

    let config_file_path =
        canonicalize(cli.config_file).expect("Failed to resolve path to config file");
    let output_file_path =
        absolute(cli.output_file).expect("Failed to resolve path to output file");

    println!("Config file: {}", config_file_path.to_string_lossy());
    println!("Output file: {}", output_file_path.to_string_lossy());

    let output_file_exists =
        exists(&output_file_path).expect("Failed to check if output file already exists");

    if output_file_exists {
        panic!("Output file already exists");
    }

    let delphes_hepmc2_executable_path = cli
        .delphes_hepmc2_executable
        .or_else(|| env::var("DELPHES_HEPMC2_PATH").map(PathBuf::from).ok())
        .expect("Failed to determine `DelphesHepMC2` executable path");

    println!(
        "Wrapping original `DelphesHepMC2` executable '{}'",
        delphes_hepmc2_executable_path.to_str().unwrap()
    );

    let num_shards = cli.num_shards.or_else( ||
        env::var("DELPHES_PARALLEL_NUM_SHARDS").ok().map(|s| {
            s.parse::<usize>()
                    .expect("Failed to parse `DELPHES_PARALLEL_NUM_SHARDS` environment variable as an integer")
        })
    ).or_else(|| {
        env::var("DELPHES_PARALLEL_NUM_CORES").ok().map(|s| {
            s.parse::<usize>()
                .expect("Failed to parse `DELPHES_PARALLEL_NUM_CORES` environment variable as an integer")
        })
    })
    .unwrap_or_else(|| {
        let num_cores = num_cpus::get_physical();
        if num_cores >= 16 {
            num_cores - 4
        } else if num_cores >= 4 {
            num_cores - 1
        } else {
            num_cores
        }
    } );

    println!(
        "Parallelizing Delphes by breaking up input file(s) into {} shards",
        num_shards
    );

    let tmp_dir = TempDir::new("delphes_parallel").expect(
        "Failed to create temporary directory for storing parallelization intermediary data files",
    );

    let tmp_dir_path = tmp_dir.path();

    {
        let cleanup = cli.cleanup;
        let tmp_dir_path_buf = tmp_dir_path.to_owned();

        ctrlc::set_handler(move || {
            if cleanup {
                println!("Interrupt signal received, attempting to clean up");
                let _ = remove_dir_all(&tmp_dir_path_buf);

                println!("Cleanup done, exiting");
            }

            exit(1);
        })
        .expect("Failed to set Ctrl+C handler");
    }

    let working_directory_path;
    if let Some(working_directory) = cli.working_directory {
        working_directory_path = working_directory;
    } else {
        working_directory_path = tmp_dir_path.to_path_buf();
    }

    let available_space = fs2::available_space(&working_directory_path)
        .expect("Failed to check available space on the disk of the working directory");

    println!(
        "Available disk space within the working directory: {} KiB",
        format_binary_size(available_space)
    );

    let shards_directory = [&working_directory_path, Path::new("shards")]
        .iter()
        .collect::<PathBuf>();

    println!(
        "Directory where shards will be stored: {}",
        shards_directory.as_os_str().to_string_lossy()
    );

    create_dir_all(&shards_directory).expect("Failed to create shards output subdirectory");

    let input_from_stdin = cli.input_files.is_empty()
        || (cli.input_files.len() == 1 && cli.input_files[0].to_str().unwrap().trim() == "-");

    let shard_paths;
    if input_from_stdin {
        println!("Reading input HepMC2 data from stdin");

        let buf_reader = BufReader::new(stdin());
        let events_reader = HepMC2Reader::try_from(buf_reader)
            .expect("Failed to open standard input for reading events");

        shard_paths = split_input_into_shards(&mut [events_reader], num_shards, &shards_directory);
    } else {
        let input_file_paths = cli
            .input_files
            .iter()
            .map(|path| canonicalize(path).expect("Failed to resolve path to an input file"))
            .collect::<Vec<_>>();

        let input_files_string = input_file_paths
            .iter()
            .map(|path| path.to_str().unwrap())
            .collect::<Vec<_>>()
            .join(" ");
        println!("Input file path(s): {}", input_files_string);

        let mut total_file_size = 0u64;

        let mut events_readers = input_file_paths
            .into_iter()
            .map(|path| {
                let file = File::open(&path).expect("Could not open input file");

                match file.metadata() {
                    Ok(metadata) => {
                        let file_size = metadata.len();
                        println!(
                            "Size of file '{}': {}",
                            path.to_str().unwrap(),
                            format_binary_size(file_size)
                        );
                        total_file_size += file_size;
                    }
                    Err(err) => {
                        println!(
                            "Failed to read metadata for input file '{}': {}",
                            path.to_str().unwrap(),
                            err
                        )
                    }
                }

                let buf_reader = BufReader::new(file);

                HepMC2Reader::try_from(buf_reader).expect("Failed to open HepMC2 file for reading")
            })
            .collect::<Vec<_>>();

        // We need to store both the input Delphes file shards
        // (which will be processed by each subprocess in parallel)
        // as well as the ROOT output shards.
        let expected_disk_space_usage = total_file_size * 2;

        // Add a 5% margin for safety.
        let required_disk_space = expected_disk_space_usage + expected_disk_space_usage / 5;

        if available_space < required_disk_space {
            panic!(
                "Not enough available disk space on working directory's drive.\nRequired (expected): {}\nAvailable (as reported by OS): {}",
                format_binary_size(required_disk_space),
                format_binary_size(available_space)
            )
        }

        println!(
            "Expected average shard size: {} (x {} shards)",
            format_binary_size(total_file_size / (num_shards as u64)),
            num_shards
        );

        shard_paths = split_input_into_shards(&mut events_readers, num_shards, &shards_directory);
    }

    let outputs_directory = [&working_directory_path, Path::new("outputs")]
        .iter()
        .collect::<PathBuf>();

    println!(
        "Directory where processed shard outputs in ROOT format will be stored: {}",
        outputs_directory.to_str().unwrap()
    );

    create_dir_all(&outputs_directory).expect("Failed to create shard outputs subdirectory");

    let root_output_files_paths = process_shards(
        &delphes_hepmc2_executable_path,
        &shard_paths,
        &config_file_path,
        &outputs_directory,
    );

    merge_shards(&root_output_files_paths, &output_file_path);

    if cli.cleanup {
        drop(tmp_dir);
    } else {
        forget(tmp_dir);
    }

    println!("Done");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_input() {
        use clap::CommandFactory;
        let cmd = Cli::command();
        cmd.try_get_matches_from(vec![
            "DelphesHepMC2",
            "config.tcl",
            "output.root",
            "input1.hepmc2",
            "input2.hepmc2",
        ])
        .expect("Valid arguments");
    }
}
