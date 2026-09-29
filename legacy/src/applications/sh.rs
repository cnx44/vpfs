use std::{clone, env, io::{self, BufReader, Read, Write}, process::{self, exit, Stdio}, sync::Arc, thread};
use vpfs::*;
use vpfs::messages::*;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "vpfs", about = "Virtual private file system prototype.")]
struct Opt {
    #[arg(short, long, default_value_t = 8082)]
    port: u16,
}

enum RedirectType {
    NoRedirect,
    File(String),
    Piped(Stdio)
}

struct Command {
    program: String,
    args: Vec<String>,
    stdin: RedirectType,
    stderr: RedirectType,
    stdout: RedirectType
}

enum PipeableCommand {
    NonPiped(Command),
    Piped(Command, Box<PipeableCommand>)
}

impl Command {

    fn forward_reads<T: Write>(file: FileEntry, mut pipe: T, vpfs: Arc<VPFS>) {
        match vpfs.read(file){
            Ok(data) => {
                match pipe.write_all(&data) {
                    Ok(_bytes_writen) => {},
                    Err(error) => {
                        println!("Got {} error trying to write to pipe", error);
                    }
                };
            }
            Err(VPFSError::OnlyInCache(cache_file_entry)) => {
                loop {
                    let mut buf = String::new();
                    println!("File only available in cache. Use cached version? (y or n)");
                    io::stdin().read_line(&mut buf).unwrap();
                    match buf.trim() {
                        "y" => {
                            Command::forward_reads(cache_file_entry, pipe, vpfs);
                            break;
                        }
                        "n" => break,
                        _ => continue,
                    }
                }
            }
            Err(_) => {
                println!("Unable to read file");
            }
        }
    }

    fn forward_writes<T: Read>(file_name: String, mut pipe: T, vpfs: Arc<VPFS>) {
        let mut data = vec![];
        match pipe.read_to_end(&mut data) {
            Ok(_) => {
                match vpfs.store(&file_name, &data) {
                    Ok(()) => {}
                    Err(VPFSError::OnlyInCache(cache_file_entry)) => {
                        loop {
                            let mut buf = String::new();
                            println!("File owner currently unreachable. Any modification might result in conflicts. Continue (y or n)?");
                            io::stdin().read_line(&mut buf).unwrap();
                            match buf.trim() {
                                "y" => {
                                    vpfs.write(cache_file_entry, &data).ok();
                                    break;
                                }
                                "n" => break,
                                _ => continue,
                            }
                        }
                    }
                    Err(_) => {
                        println!("No longer able to write to file");
                    }
                }
            }
            Err(error) => {
                println!("Got {} error trying to read from pipe", error);
            }
        }
    }

    pub fn spawn(self, vpfs: Arc<VPFS>) -> io::Result<(process::Child, Vec<thread::JoinHandle<()>>)> {
        let mut process_command = process::Command::new(&self.program);

        let mut stdin_file_entry: Option<FileEntry> = None;
        let mut stdout_file: Option<String> = None;
        let mut stderr_file: Option<String> = None;

        //Set up stdin redirection
        match self.stdin {
            RedirectType::NoRedirect => {},
            RedirectType::File(stdin_file) => {
                match vpfs.find(stdin_file.as_str()) {
                    Ok(file_entry) =>  {
                        stdin_file_entry = Some(file_entry);
                    },
                    _ => {
                        println!("Could not locate {:?}", stdin_file);
                        return Err(io::Error::from(io::ErrorKind::NotFound));
                    }
                }
                process_command.stdin(Stdio::piped());
            }
            RedirectType::Piped(stdin_pipe) => {
                process_command.stdin(stdin_pipe);
            }
        }

        // Set up stdout redirection
        match self.stdout {
            RedirectType::NoRedirect => {},
            RedirectType::File(stdout_file_name) => {
                stdout_file = Some(stdout_file_name);
                process_command.stdout(Stdio::piped());
                //vpfs.place(&stdout_file, stdout_location.clone().unwrap());
            },
            RedirectType::Piped(stdout_pipe) => {
                process_command.stdout(stdout_pipe);
            }
        }

        // Set up stderr redirection
        match self.stderr {
            RedirectType::NoRedirect => {},
            RedirectType::File(stderr_file_name) => {
                stderr_file = Some(stderr_file_name);
                process_command.stderr(Stdio::piped());                
                //vpfs.place(&stderr_file, stderr_location.clone().unwrap());
            },
            RedirectType::Piped(stderr_pipe) => {
                process_command.stderr(stderr_pipe);
            }
        }

        // Spawn child
        let fork_ret = process_command.args(&self.args).spawn();

        if let Ok(mut child) = fork_ret {
            let mut handles = vec![];

            // Spawn thread for forwarding stdin to VPFS as needed
            if let Some(stdin_file_entry) = stdin_file_entry {
                let vpfs_clone = vpfs.clone();
                handles.push(thread::spawn(move || {
                    Command::forward_reads(stdin_file_entry, child.stdin.take().unwrap(), vpfs_clone);
                }));
                child.stdin = None;
            }

            // Spawn thread for forwarding stdout to VPFS as needed
            if let Some(stdout_file) = stdout_file {
                let vpfs_clone = vpfs.clone();
                handles.push(thread::spawn(move || {
                    Command::forward_writes(stdout_file, child.stdout.take().unwrap(), vpfs_clone);
                }));
                child.stdout = None;
            }

            // Spawn thread for forwarding stderr to VPFS as needed
            if let Some(stderr_file) = stderr_file {
                let vpfs_clone = vpfs.clone();
                handles.push(thread::spawn(move || {
                    Command::forward_writes(stderr_file, child.stderr.take().unwrap(), vpfs_clone);
                }));
                child.stderr = None
            }

            Ok((child, handles))
        }
        else {
            Err(fork_ret.unwrap_err())
        }
    }
}

fn normalize_path(path: String) -> String {
    let mut stack: Vec<String> = Vec::new();

    for part in path.split('/') {
        match part {
            "" | "." => {
                continue;
            }
            ".." => {
                stack.pop();
            }
            _ => {
                stack.push(part.to_string());
            }
        }
    }

    let mut result = String::new();
    if path.starts_with('/') {
        result.push('/');
    }
    result.push_str(&stack.join("/"));

    result
}

fn file_name_to_full_path(cwd: &str, file_name: &str) -> String {
    let full_path = if file_name.starts_with('/') {
            file_name[1..].to_string()
        }
        else if cwd != ""{
            format!("{}/{}", cwd, file_name)
        }
        else {
            file_name.to_string()
        };
    normalize_path(full_path)

}

fn parse_piped_command(command_string: &str, cwd: &str) -> Option<PipeableCommand> {
    let (lhs_string, rhs_string) = command_string.split_once('|').unwrap();
    let lhs_command = parse_nonpiped_command(lhs_string, cwd);
    let rhs_command = parse_command(rhs_string, cwd);

    if rhs_command.is_none() {
        println!("Syntax error, right side of pipe invalid");
        None
    }
    else if let Some(lhs_command) = lhs_command
    {
        let rhs_command = Box::from(rhs_command.unwrap());
        Some(PipeableCommand::Piped(lhs_command,  rhs_command))
    }
    else {
        println!("Syntax error, left side of pipe invalid");
        None
    }
}


fn parse_nonpiped_command(mut command_string: &str, cwd: &str) -> Option<Command> {
    let mut args: Vec<String> = vec![];
    let mut program: Option<&str> = None;
    let mut input_file_name: Option<&str> = None;
    let mut output_file_name: Option<&str> = None;

    let mut next_token_input_file = false;
    let mut next_token_output_file = false;

    let mut token_start = command_string.find(|c: char| !c.is_whitespace());

    while let Some(token_start_index) = token_start{
        let token_end = command_string[token_start_index..].find(|c: char| c.is_whitespace() || c == '<' || c == '>');
        let mut token_end_index =  match token_end {
                                    Some(token_end) => token_end + token_start_index,
                                    None => command_string.len() - 1
                                };
        match &command_string[token_start_index .. token_start_index + 1] {
            ">" => {
                if next_token_output_file || next_token_input_file{
                    println!("Syntax error near >");
                    return None;
                }
                next_token_output_file = true;
                token_end_index += 1;
            }
            "<" => {
                if next_token_output_file || next_token_input_file{
                    println!("Syntax error near <");
                    return None;
                }
                next_token_input_file = true;
                token_end_index += 1;
            }
            _ => {
                let token = &command_string[token_start_index..token_end_index];
                if next_token_input_file {
                    input_file_name = Some(token);
                    next_token_input_file = false;
                }
                else if next_token_output_file {
                    output_file_name = Some(token);
                    next_token_output_file = false;
                }
                else if program.is_none() {
                    program = Some(token);
                }
                else {
                    args.push(String::from(token));
                }
            }
        };

        command_string = &command_string[token_end_index..];
        token_start = command_string.find(|c: char| !c.is_whitespace());
    }

    if let Some(program) = program{
        let mut command = Command {
            program: String::from(program),
            args: args,
            stdin: RedirectType::NoRedirect,
            stdout: RedirectType::NoRedirect,
            stderr: RedirectType::NoRedirect
        };

        if let Some(input_file_name) = input_file_name{
            command.stdin = RedirectType::File(file_name_to_full_path(cwd, input_file_name));
        }
        if let Some(output_file_name) = output_file_name {
            command.stdout = RedirectType::File(file_name_to_full_path(cwd, output_file_name));
        }
        Some(command)
    }
    else {
        None
    }
}


fn parse_command(command_string: &str, cwd: &str) -> Option<PipeableCommand> {

    if command_string.contains('|') {
        parse_piped_command(command_string, cwd)
    }
    else {
        let command = parse_nonpiped_command(command_string, cwd);
        match command {
            Some(command) => Some(PipeableCommand::NonPiped(command)),
            None => None
        }
    }
}

fn run_ls(command: Command, vpfs: Arc<VPFS>, cwd: &str) {
    let ls_result = vpfs.ls(cwd);
    if let Ok(file_entries) = ls_result {
        for entry in file_entries {
            println!("{} {}", entry.name, entry.owner);
        }
    }
    else {
        println!("Failed to list files for {}", cwd);
    }
}

fn run_cat(vpfs: Arc<VPFS>, command: &Command, cwd: &str) {
    for file_name in &command.args {
        let full_path = file_name_to_full_path(cwd, file_name);
        match vpfs.fetch(&full_path) {
            Ok(data) => {
                io::stdout().write_all(&data).unwrap();
            }
            Err(e) => println!("vpfs cat error {}: {:?}", file_name, e),
        }
    }
}

fn run_nonpiped_command(command: Command, vpfs: Arc<VPFS>, cwd: &mut String) {
    let program = command.program.clone();
    match program.as_str() {
        // Built-ins
        "exit" => exit(0),
        // "cd" => run_cd(command, vpfs, cwd),
        "pwd" => println!("/{}", cwd),        
        // "mkdir" => run_mkdir(command, vpfs, cwd),
        "ls" => run_ls(command, vpfs, cwd),
        // "cat" => run_cat(vpfs.clone(), &command, cwd),
        // Normal binaries
        _ => {
            let fork_ret = command.spawn(vpfs);
            if let Ok((mut child, handles)) = fork_ret {
                child.wait().expect("Failed to wait for child");
                for handle in handles { handle.join().ok(); }
            }
            else {
                println!("Failed to run {:?}", program);
            }
        }
    }
}


fn run_piped_command(mut lhs_command: Command, rhs_command: PipeableCommand, vpfs: Arc<VPFS>, cwd: &mut String) {
    let lhs_program = lhs_command.program.clone();
    lhs_command.stdout = RedirectType::Piped(Stdio::piped());
    let fork_ret = lhs_command.spawn(vpfs.clone());
    if let Ok((mut child, handles)) = fork_ret {
        match rhs_command {
            PipeableCommand::NonPiped(mut rhs_command) => {
                rhs_command.stdin = RedirectType::Piped(Stdio::from(child.stdout.take().unwrap()));
                run_nonpiped_command(rhs_command, vpfs, cwd);
            }
            PipeableCommand::Piped(mut rhs_first_command, rhs_next_commands) => {
                rhs_first_command.stdin = RedirectType::Piped(Stdio::from(child.stdout.take().unwrap()));
                run_piped_command(rhs_first_command, *rhs_next_commands, vpfs, cwd);
            }
        }
        child.wait().expect("Faild to wait for child");
        for handle in handles { handle.join().ok(); }
    }
    else {
        println!("Failed to run {:?}", lhs_program);
        run_command(rhs_command, vpfs, cwd);
    }

}

fn run_command(command: PipeableCommand, vpfs: Arc<VPFS>, cwd: &mut String) {
    match command {
        PipeableCommand::NonPiped(command) => {
            run_nonpiped_command(command, vpfs, cwd);
        }
        PipeableCommand::Piped(lhs_command, rhs_command) => {
            run_piped_command(lhs_command, *rhs_command, vpfs, cwd);
        }
    }

}


fn main() {
    let opt = Opt::parse();
    let vpfs = Arc::new(VPFS::connect(opt.port).expect("Failed to connect to local daemon"));
    let mut cwd = "".to_string();

    loop {
        print!("{}:/{}$ ",vpfs.local, cwd);
        io::stdout().flush().expect("Failed to flush stdout");

        let mut command_string = String::new();
        if io::stdin().read_line(&mut command_string).is_ok(){
            if let Some(command) = parse_command(&command_string, &cwd){
                run_command(command, vpfs.clone(), &mut cwd);
            }    
        }
        else {
            exit(0);
        }
    }
}
