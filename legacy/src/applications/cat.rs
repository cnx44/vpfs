use clap::Parser;

use std::sync::Arc;
use std::io::{self, Write};

use vpfs::*;
use vpfs::messages::*;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Opt {
    #[arg(short, long)]
    lines: bool,

    files: Vec<String>,

    #[arg(short, long, default_value_t = 8080)]
    port: u16,
}

fn main() -> Result<(), VPFSError> {
    let opt = Opt::parse();

    let vpfs = Arc::new(VPFS::connect(opt.port).expect("Failed to connect to local daemon"));
    
    let mut line_number = 1;

    for file in &opt.files {
        // println!("{file}");
        if opt.lines {
            vpfs_cat_lines(vpfs.clone(), file, &mut line_number)?;
        } else {
            vpfs_cat(vpfs.clone(), file)?;
        }
    }

    Ok(())
}

fn vpfs_cat(vpfs: Arc<VPFS>, path: &str) -> Result<(), VPFSError> {
    let fd = vpfs.open(path)?;
    loop {
        let buf = vpfs.read_fd(fd, 1024)?;
        if buf.is_empty() {
            break;
        }
        io::stdout().write_all(&buf).unwrap();
    }

    vpfs.close(fd)?;
    Ok(())
}

fn vpfs_cat_lines(vpfs: Arc<VPFS>, path: &str, line_number: &mut i32) -> Result<(), VPFSError> {
    let fd = vpfs.open(path)?;

    loop {
        let line = vpfs.read_line_fd(fd)?;
        if line.is_empty() {
            break;
        }

        let line_num_str = format!("{:6}\t", line_number);
        io::stdout().write_all(line_num_str.as_bytes()).unwrap();
        io::stdout().write_all(&line).unwrap();

        *line_number += 1;
    }

    vpfs.close(fd)?;
    Ok(())
}
