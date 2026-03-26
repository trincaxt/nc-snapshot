mod errors;
mod node_detect;
mod snapshot;
mod types;
mod verify;

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use types::{BridgeResult, SnapshotConfig, SnapshotMode};

#[derive(Parser, Debug)]
#[command(
    name = "nc-snapshot",
    about = "⚡ Nine Chronicles blockchain snapshot tool",
    version,
    long_about = "Fast, production-grade snapshot tool for Nine Chronicles blockchain.\n\
Creates tar.zst archives with BLAKE3 integrity verification.\n\n\
Modes:\n\
- state (default)  State snapshot: indexes + state data (~127 GiB)\n\
- partition        Base/partition snapshot: block + tx epochs (~230+ GiB)\n\
- full             Full snapshot: everything in the store directory"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create a new snapshot archive
    Create {
        /// Source blockchain directory
        #[arg(short, long, default_value = "~/9c-blockchain")]
        source: String,

        /// Output archive path (.tar.zst) — overrides --output-dir and auto-naming
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Output directory for auto-named snapshot (e.g. ~/snapshots/base)
        #[arg(long)]
        output_dir: Option<PathBuf>,

        /// Snapshot mode: state (default), partition (base), full
        #[arg(short, long, default_value = "state")]
        mode: String,

        /// Zstd compression level 1-19 (1=fastest, default=1)
        #[arg(short, long, default_value = "1")]
        level: i32,

        /// Number of compression threads (0=all CPUs)
        #[arg(short, long, default_value = "0")]
        threads: usize,

        /// Directories to EXCLUDE from snapshot
        #[arg(short, long)]
        exclude: Vec<String>,

        /// Directories to INCLUDE (overrides mode defaults)
        #[arg(short, long)]
        include: Vec<String>,

        /// Epoch limit for partition mode (skip epochs below this number)
        #[arg(long)]
        epoch_limit: Option<u64>,

        /// APV for metadata generation
        #[arg(long)]
        apv: Option<String>,

        /// Block before current tip
        #[arg(long, default_value_t = 1)]
        block_before: i32,

        /// Proceed even if node is detected running
        #[arg(long)]
        force: bool,

        /// Output results as JSON
        #[arg(long)]
        json: bool,

        /// Scan only, don't create archive
        #[arg(long)]
        dry_run: bool,

        /// Skip unchanged files since last snapshot
        #[arg(long)]
        incremental: bool,
    },

    /// Verify an existing archive's integrity
    Verify {
        /// Path to the .tar.zst archive to verify
        archive: PathBuf,

        /// Output results as JSON
        #[arg(long)]
        json: bool,
    },
}

fn expand_tilde(path: &str) -> PathBuf {
    if path.starts_with("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(&path[2..]);
        }
    }
    PathBuf::from(path)
}

fn fetch_metadata(
    source: &Path,
    apv: &str,
    block_before: i32,
    mode: &str,
) -> anyhow::Result<BridgeResult> {
    let prepare_args = serde_json::json!({
        "Apv": apv,
        "OutputDirectory": ".",
        "StorePath": source.to_string_lossy(),
                                         "BlockBefore": block_before,
                                         "BypassCopyStates": true,
                                         "SnapshotType": mode
    });

    // Chama o binário publicado — sem dotnet run, sem .csproj, sem dependências
    let bridge_bin = Path::new(env!("CARGO_MANIFEST_DIR"))
    .join("bridge-bin")
    .join("NineChronicles.Snapshot.Bridge");

    let output = Command::new(&bridge_bin)
    .arg(prepare_args.to_string())
    .output()?;

    if !output.status.success() {
        anyhow::bail!(
            "C# Bridge failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let last_line = stdout
    .lines()
    .last()
    .ok_or_else(|| anyhow::anyhow!("Empty bridge output"))?;
    let res: BridgeResult = serde_json::from_str(last_line)?;
    Ok(res)
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Create {
            source,
            output,
            output_dir,
            mode,
            level,
            threads,
            exclude,
            include,
            mut epoch_limit,
            apv,
            block_before,
            force,
            json,
            dry_run,
            incremental,
        } => {
            let source_path = expand_tilde(&source);
            let threads = if threads == 0 {
                num_cpus::get()
            } else {
                threads
            };
            let mode_enum: SnapshotMode = mode.parse().unwrap_or(SnapshotMode::State);

            let mut bridge_res = None;
            if let Some(ref apv_val) = apv {
                if !json {
                    eprintln!("🚀 Fetching blockchain metadata...");
                }
                match fetch_metadata(&source_path, apv_val, block_before, &mode) {
                    Ok(res) => {
                        if !res.success {
                            eprintln!("❌ Bridge error: {:?}", res.error);
                            process::exit(1);
                        }
                        if mode_enum == SnapshotMode::Partition && epoch_limit.is_none() {
                            epoch_limit = Some(res.current_metadata_block_epoch as u64);
                        }
                        bridge_res = Some(res);
                    }
                    Err(e) => {
                        eprintln!("⚠️ Failed to fetch metadata: {}", e);
                    }
                }
            }

            // Determine final output path:
            // 1. --output (explicit path) → usa direto
            // 2. --output-dir + auto-name via bridge → ~/snapshots/base/snapshot-20536-20536.tar.zst
            // 3. sem nada → arquivo no diretório atual com auto-name
            let final_output = if let Some(p) = output {
                p
            } else {
                let auto_name = if let Some(ref res) = bridge_res {
                    if mode_enum == SnapshotMode::Partition {
                        format!("{}.tar.zst", res.partition_base_filename)
                    } else {
                        "state_latest.tar.zst".to_string()
                    }
                } else {
                    format!("{}_snapshot.tar.zst", mode)
                };

                match output_dir {
                    Some(ref dir) => {
                        // Cria o diretório se não existir
                        if !dir.exists() {
                            if let Err(e) = std::fs::create_dir_all(dir) {
                                eprintln!("❌ Failed to create output-dir: {}", e);
                                process::exit(1);
                            }
                        }
                        dir.join(auto_name)
                    }
                    None => PathBuf::from(auto_name),
                }
            };

            if !json {
                eprintln!("╔══════════════════════════════════════════╗");
                eprintln!("║   ⚡ NC Blockchain Snapshot Tool         ║");
                eprintln!("╚══════════════════════════════════════════╝");
                eprintln!("  Source  : {}", source_path.display());
                eprintln!("  Output  : {}", final_output.display());
                eprintln!("  Mode    : {}", mode);
                eprintln!("  Level   : zstd-{}", level);
                eprintln!("  Threads : {}", threads);
                if let Some(el) = epoch_limit {
                    eprintln!("  Epoch≥  : {}", el);
                }
                eprintln!();
            }

            if !force {
                let locked = node_detect::check_node_running(&source_path);
                if !locked.is_empty() {
                    eprintln!("⚠️  Node appears to be running!");
                    process::exit(1);
                }
            }

            let config = SnapshotConfig {
                source: source_path,
                output: final_output,
                level,
                threads,
                exclude,
                include,
                mode: mode_enum,
                epoch_limit,
                force,
                json,
                dry_run,
                incremental,
                apv,
                block_before,
            };

            match snapshot::create_snapshot(&config, bridge_res) {
                Ok(result) => {
                    if json {
                        println!("{}", serde_json::to_string_pretty(&result).unwrap());
                    }
                }
                Err(e) => {
                    eprintln!("❌ Snapshot failed: {:#}", e);
                    process::exit(1);
                }
            }
        }

        Commands::Verify { archive, json } => {
            match verify::verify_archive(&archive, json) {
                Ok(result) => {
                    if json {
                        println!("{}", serde_json::to_string_pretty(&result).unwrap());
                    }
                }
                Err(e) => {
                    eprintln!("❌ Verification failed: {:#}", e);
                    process::exit(1);
                }
            }
        }
    }
}
