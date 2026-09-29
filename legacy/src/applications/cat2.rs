use clap::Parser;

use std::io::{self, Write};

use vpfs::*;
use vpfs::messages::*;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cat {
    #[arg(short = 'n', long)]
    number: bool,

    #[arg(short = 'b', long)]
    number_nonblank: bool,

    #[arg(short = 's', long)]
    squeeze_blank: bool,

    #[arg(short = 'E', long)]
    show_ends: bool,

    #[arg(short = 'T', long)]
    show_tabs: bool,

    #[arg(short = 'v', long)]
    show_nonprinting: bool,

    #[arg(short = 'A', long)]
    show_all: bool,

    #[arg(short = 'e')]
    show_end_nonprinting: bool,

    #[arg(short = 't')]
    show_tabs_nonprinting: bool,

    #[arg(short = 'u', long)]
    unbuffered: bool,

    files: Vec<String>,

    #[arg(short, long, default_value_t = 8080)]
    port: u16,
}

impl Cat {
    fn process_byte(&self, byte: u8) -> Vec<u8> {
        let mut output = Vec::new();

        match byte {
            0..=8 | 11..=12 | 14..=31 => {
                if self.show_nonprinting {
                    output.extend_from_slice(b"^");
                    output.push(byte + 64);
                } else {
                    output.push(byte);
                }
            }
            9 => { // Tab
                if self.show_tabs {
                    output.extend_from_slice(b"^I");
                } else {
                    output.push(byte);
                }
            }
            10 => { // Newline
                if self.show_ends {
                    output.extend_from_slice(b"$\n");
                } else {
                    output.push(byte);
                }
            }
            127 => {
                if self.show_nonprinting {
                    output.extend_from_slice(b"^?");
                } else {
                    output.push(byte);
                }
            }
            128..=255 => {
                if self.show_nonprinting {
                    output.extend_from_slice(b"M-");
                    let adjusted_byte = byte - 128;
                    if adjusted_byte >= 32 && adjusted_byte <= 126 {
                        output.push(adjusted_byte);
                    } else if adjusted_byte == 127 {
                        output.extend_from_slice(b"^?");
                    } else {
                        output.extend_from_slice(b"^");
                        output.push(adjusted_byte + 64);
                    }
                } else {
                    output.push(byte);
                }
            }
            _ => {
                output.push(byte);
            }
        }

        output
    }
}

fn main() -> Result<(), VPFSError> {
    let mut cat = Cat::parse();
    if cat.show_all {
        cat.show_nonprinting = true;
        cat.show_ends = true;
        cat.show_tabs = true;
    }
    if cat.show_end_nonprinting {
        cat.show_ends = true;
        cat.show_nonprinting = true;
    }
    if cat.show_tabs_nonprinting {
        cat.show_tabs = true;
        cat.show_nonprinting = true;
    }

    let vpfs = VPFS::connect(cat.port).expect("Failed to connect to local daemon");
    
    let mut line_number = 1;
    let mut last_empty_line = false;
    let mut last_newline = true;
    let numbering = cat.number || cat.number_nonblank;
    let read_line = numbering || cat.squeeze_blank;

    for file in &cat.files {
        let fd = vpfs.open(file)?;
        loop {
            let buf = if read_line {
                    vpfs.read_line_fd(fd)?
                } else {
                    vpfs.read_fd(fd, 1024)?
                };

            if buf.is_empty() {
                break;
            }

            if cat.squeeze_blank && buf == b"\n" && last_empty_line {
                continue;
            }

            if numbering && last_newline {
                if !(cat.number_nonblank && buf == b"\n") {
                    let line_num_str = format!("{:6}\t", line_number);
                    io::stdout().write_all(line_num_str.as_bytes()).unwrap();
                    line_number += 1;
                }
            }

            use std::io::{self, Write};

            for &b in &buf {
                let to_print = cat.process_byte(b);
                io::stdout().write_all(&to_print).unwrap();
            }

            last_newline = buf[buf.len()-1] == b'\n';
            last_empty_line = last_newline && buf.len() == 1;
        }

        vpfs.close(fd)?;
    }

    Ok(())
}