//! Unit tests for the client binaries' pure logic (shell parser, cat2 byte
//! rendering, CLI defaults). The binaries are included verbatim so their
//! private items are reachable from child test modules.
// Warnings come from the included binary sources, which this suite must not modify.
#![allow(warnings)]

mod sh {
    include!("../src/applications/sh.rs");

    mod tests {
        use super::*;

        fn redirect(r: &RedirectType) -> Option<String> {
            match r {
                RedirectType::NoRedirect => None,
                RedirectType::File(f) => Some(f.clone()),
                RedirectType::Piped(_) => Some("<pipe>".into()),
            }
        }

        fn single(input: &str, cwd: &str) -> Command {
            match parse_command(input, cwd) {
                Some(PipeableCommand::NonPiped(c)) => c,
                Some(PipeableCommand::Piped(..)) => panic!("unexpected pipe for {input:?}"),
                None => panic!("parse failed for {input:?}"),
            }
        }

        #[test]
        fn parses_program_and_whitespace_separated_args() {
            let c = single("ls\n", "");
            assert_eq!(c.program, "ls");
            assert!(c.args.is_empty());

            let c = single("  echo  a \t  b\n", "");
            assert_eq!(c.program, "echo");
            assert_eq!(c.args, vec!["a", "b"]);
            assert_eq!(redirect(&c.stdin), None);
            assert_eq!(redirect(&c.stdout), None);
            assert_eq!(redirect(&c.stderr), None);
        }

        #[test]
        fn parses_input_and_output_redirection_with_or_without_spaces() {
            let c = single("cat < in.txt > out.txt\n", "");
            assert_eq!(c.program, "cat");
            assert!(c.args.is_empty());
            assert_eq!(redirect(&c.stdin).as_deref(), Some("in.txt"));
            assert_eq!(redirect(&c.stdout).as_deref(), Some("out.txt"));

            let c = single("echo hi>f\n", "");
            assert_eq!(c.args, vec!["hi"]);
            assert_eq!(redirect(&c.stdout).as_deref(), Some("f"));

            let c = single("sort<in\n", "");
            assert_eq!(c.program, "sort");
            assert_eq!(redirect(&c.stdin).as_deref(), Some("in"));
        }

        #[test]
        fn last_redirect_of_a_kind_wins() {
            let c = single("echo x > a > b\n", "");
            assert_eq!(redirect(&c.stdout).as_deref(), Some("b"));
            let c = single("cat < a < b\n", "");
            assert_eq!(redirect(&c.stdin).as_deref(), Some("b"));
        }

        #[test]
        fn redirect_paths_are_resolved_against_cwd_and_normalized() {
            let c = single("cat < x > y\n", "dir");
            assert_eq!(redirect(&c.stdin).as_deref(), Some("dir/x"));
            assert_eq!(redirect(&c.stdout).as_deref(), Some("dir/y"));
            assert_eq!(redirect(&single("cat < /abs/x\n", "dir").stdin).as_deref(), Some("abs/x"));
            assert_eq!(redirect(&single("cat < ../x\n", "d/e").stdin).as_deref(), Some("d/x"));
            assert_eq!(redirect(&single("cat < ./a/./b\n", "").stdin).as_deref(), Some("a/b"));
            // Program arguments are NOT resolved.
            assert_eq!(single("cat ./a\n", "dir").args, vec!["./a"]);
        }

        #[test]
        fn normalize_path_behaviour() {
            assert_eq!(normalize_path("a/b/../c".into()), "a/c");
            assert_eq!(normalize_path("/a//b/".into()), "/a/b");
            assert_eq!(normalize_path("./.".into()), "");
            assert_eq!(normalize_path("".into()), "");
            // `..` above the root is silently dropped.
            assert_eq!(normalize_path("a/../../b".into()), "b");
            assert_eq!(file_name_to_full_path("", "/x"), "x");
            assert_eq!(file_name_to_full_path("", "x"), "x");
            assert_eq!(file_name_to_full_path("d", "x"), "d/x");
        }

        #[test]
        fn empty_or_programless_input_is_rejected() {
            assert!(parse_command("\n", "").is_none());
            assert!(parse_command("   \t\n", "").is_none());
            assert!(parse_command("", "").is_none());
            assert!(parse_command("> f\n", "").is_none());
            assert!(parse_command("< f\n", "").is_none());
        }

        #[test]
        fn doubled_redirect_operators_are_syntax_errors() {
            assert!(parse_command("cat < < f\n", "").is_none());
            assert!(parse_command("cat > < f\n", "").is_none());
            assert!(parse_command("cat >> f\n", "").is_none(), "append (>>) is not supported");
        }

        /// Current behaviour: a redirect operator with no target is silently ignored.
        #[test]
        fn dangling_redirect_is_ignored() {
            let c = single("cat >\n", "");
            assert_eq!(redirect(&c.stdout), None);
        }

        /// Current behaviour: no quoting and no `2>` support (`2` becomes an arg).
        #[test]
        fn no_quoting_or_stderr_redirection() {
            let c = single("echo \"a b\"\n", "");
            assert_eq!(c.args, vec!["\"a", "b\""]);
            let c = single("cmd 2> err\n", "");
            assert_eq!(c.args, vec!["2"]);
            assert_eq!(redirect(&c.stdout).as_deref(), Some("err"));
            assert_eq!(redirect(&c.stderr), None);
        }

        #[test]
        fn pipes_nest_to_the_right() {
            match parse_command("a 1 | b < in | c > out\n", "") {
                Some(PipeableCommand::Piped(a, rest)) => {
                    assert_eq!(a.program, "a");
                    assert_eq!(a.args, vec!["1"]);
                    match *rest {
                        PipeableCommand::Piped(b, rest) => {
                            assert_eq!(b.program, "b");
                            assert_eq!(redirect(&b.stdin).as_deref(), Some("in"));
                            match *rest {
                                PipeableCommand::NonPiped(c) => {
                                    assert_eq!(c.program, "c");
                                    assert_eq!(redirect(&c.stdout).as_deref(), Some("out"));
                                }
                                _ => panic!("expected final non-piped command"),
                            }
                        }
                        _ => panic!("expected nested pipe"),
                    }
                }
                _ => panic!("expected pipe"),
            }
        }

        #[test]
        fn pipe_with_missing_side_is_rejected() {
            assert!(parse_command("| b\n", "").is_none());
            assert!(parse_command("a |\n", "").is_none());
            assert!(parse_command("a || b\n", "").is_none());
        }

        #[test]
        fn default_daemon_port_is_8082() {
            assert_eq!(Opt::parse_from(["sh"]).port, 8082);
            assert_eq!(Opt::parse_from(["sh", "-p", "9"]).port, 9);
        }
    }
}

mod cat {
    include!("../src/applications/cat.rs");

    mod tests {
        use super::*;

        /// Current behaviour (suspected bug): `cat` defaults to port 8080 while
        /// the daemon listens on 8082 by default, so it fails without `-p`.
        #[test]
        fn default_port_is_8080() {
            let opt = Opt::parse_from(["cat", "f1", "f2"]);
            assert_eq!(opt.port, 8080);
            assert_eq!(opt.files, vec!["f1", "f2"]);
            assert!(!opt.lines);
            assert!(Opt::parse_from(["cat", "-l", "f"]).lines);
        }
    }
}

mod cat2 {
    include!("../src/applications/cat2.rs");

    mod tests {
        use super::*;

        fn cat(args: &[&str]) -> Cat {
            Cat::parse_from(std::iter::once("cat2").chain(args.iter().copied()))
        }

        fn render(c: &Cat, bytes: &[u8]) -> Vec<u8> {
            bytes.iter().flat_map(|&b| c.process_byte(b)).collect()
        }

        /// Current behaviour (suspected bug): same 8080 default as `cat`.
        #[test]
        fn default_port_is_8080() {
            assert_eq!(cat(&[]).port, 8080);
        }

        #[test]
        fn plain_output_passes_every_byte_through() {
            let all: Vec<u8> = (0..=255).collect();
            assert_eq!(render(&cat(&[]), &all), all);
        }

        #[test]
        fn show_tabs_and_ends() {
            assert_eq!(render(&cat(&["-T"]), b"a\tb\n"), b"a^Ib\n");
            assert_eq!(render(&cat(&["-E"]), b"a\tb\n"), b"a\tb$\n");
            assert_eq!(render(&cat(&["-T", "-E"]), b"\t\n"), b"^I$\n");
        }

        #[test]
        fn show_nonprinting_uses_caret_and_meta_notation() {
            let c = cat(&["-v"]);
            assert_eq!(render(&c, &[0]), b"^@");
            assert_eq!(render(&c, &[1]), b"^A");
            assert_eq!(render(&c, &[27]), b"^[");
            assert_eq!(render(&c, &[127]), b"^?");
            assert_eq!(render(&c, &[128]), b"M-^@");
            assert_eq!(render(&c, &[160]), b"M- ");
            assert_eq!(render(&c, &[200]), b"M-H");
            assert_eq!(render(&c, &[255]), b"M-^?");
            // tab and newline are untouched by -v alone
            assert_eq!(render(&c, b"\t\n"), b"\t\n");
            assert_eq!(render(&c, b"az~ "), b"az~ ");
        }

        /// Current behaviour (suspected bug): carriage return (13) is excluded
        /// from the control range, so `-v` prints it raw (GNU cat shows `^M`).
        #[test]
        fn show_nonprinting_leaves_carriage_return_raw() {
            assert_eq!(render(&cat(&["-v"]), b"\r"), b"\r");
        }

        /// `-A`, `-e`, `-t` are only expanded in `main`; the parsed flags alone
        /// do not change `process_byte`.
        #[test]
        fn combined_flags_are_parsed_but_expanded_only_in_main() {
            let c = cat(&["-A"]);
            assert!(c.show_all);
            assert_eq!(render(&c, b"\t\n"), b"\t\n");
            assert!(cat(&["-e"]).show_end_nonprinting);
            assert!(cat(&["-t"]).show_tabs_nonprinting);
        }
    }
}
