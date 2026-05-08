/// Print a unified help page that flattens all leaf subcommands into a single table.
///
/// Prints the long_about (or about), a usage line, a flat command table, global options,
/// and a footer hint. Designed to be called from the no-subcommand branch of `main`.
#[allow(clippy::print_stdout)]
pub fn print_help_unified(cmd: &mut clap::Command) {
    use std::io::Write as _;

    cmd.build();

    let name = cmd.get_name().to_string();

    // Header: long_about preferred over about
    let header = cmd.get_long_about().or_else(|| cmd.get_about()).map(|s| s.to_string());
    if let Some(text) = header {
        println!("{text}");
        println!();
    }

    // Usage line
    println!("Usage: {name} [OPTIONS] <SUBCOMMAND>");
    println!();

    // Flat command table (clap-generated "help" pseudo-subcommands are excluded)
    let leaves = collect_leaves(cmd, &[]);
    if !leaves.is_empty() {
        println!("Commands:");
        let max_sig = leaves.iter().map(|(s, _)| s.len()).max().unwrap_or(0);
        for (sig, desc) in &leaves {
            if desc.is_empty() {
                println!("  {sig}");
            } else {
                println!("  {sig:<max_sig$}  {desc}");
            }
        }
        println!();
    }

    // Global options (skip auto-added help/version flags)
    let globals: Vec<_> = cmd
        .get_arguments()
        .filter(|a| {
            a.is_global_set() && !a.is_positional() && a.get_id().as_str() != "help" && a.get_id().as_str() != "version"
        })
        .collect();

    if !globals.is_empty() {
        println!("Global Options:");
        let formatted: Vec<(String, String)> = globals
            .iter()
            .map(|a| (format_option_left(a), format_option_right(a)))
            .collect();
        let max_left = formatted.iter().map(|(l, _)| l.len()).max().unwrap_or(0);
        for (left, right) in &formatted {
            if right.is_empty() {
                println!("  {left}");
            } else {
                println!("  {left:<max_left$}  {right}");
            }
        }
        println!();
    }

    println!("Run '{name} <subcommand> --help' for details on a specific command.");

    // Flush so callers that invoke std::process::exit(0) immediately don't lose output.
    let _ = std::io::stdout().flush();
}

fn collect_leaves<'a>(cmd: &'a clap::Command, parents: &[&'a str]) -> Vec<(String, String)> {
    let mut result = Vec::new();
    for sub in cmd.get_subcommands() {
        let sub_name = sub.get_name();
        // Skip clap's auto-generated "help" pseudo-subcommand at every level.
        if sub_name == "help" {
            continue;
        }
        let mut path = parents.to_vec();
        path.push(sub_name);
        if sub.has_subcommands() {
            result.extend(collect_leaves(sub, &path));
        } else {
            let sig = build_signature(sub, &path);
            let desc = sub
                .get_about()
                .map(|a| {
                    let first = a.to_string().lines().next().unwrap_or("").trim().to_owned();
                    trim_trailing_punctuation(first)
                })
                .unwrap_or_default();
            result.push((sig, desc));
        }
    }
    result
}

/// Trim common Chinese and ASCII sentence-ending punctuation from the right of a string.
fn trim_trailing_punctuation(mut s: String) -> String {
    // Chars to strip from the right (Chinese full-width + ASCII)
    const PUNCT: &[char] = &[
        '。', '！', '？', '；', '，', '、', '…', '\u{2026}', '.', '!', '?', ';', ',',
    ];
    while s.ends_with(|c: char| PUNCT.contains(&c)) {
        let new_len = s.trim_end_matches(|c: char| PUNCT.contains(&c)).len();
        s.truncate(new_len);
    }
    s
}

fn build_signature(cmd: &clap::Command, path: &[&str]) -> String {
    let mut parts: Vec<String> = path.iter().map(|s| (*s).to_owned()).collect();

    for arg in cmd.get_arguments().filter(|a| a.is_positional()) {
        let name = arg.get_id().as_str().to_uppercase();
        let multiple = arg.get_num_args().map(|r| r.max_values() > 1).unwrap_or(false);
        let placeholder = if arg.is_required_set() {
            format!("<{name}>")
        } else {
            format!("[{name}]")
        };
        if multiple {
            parts.push(format!("{placeholder}..."));
        } else {
            parts.push(placeholder);
        }
    }

    parts.join(" ")
}

fn format_option_left(arg: &clap::Arg) -> String {
    let Some(long) = arg.get_long() else {
        return String::new();
    };
    let mut s = format!("--{long}");
    if is_value_taking(arg) {
        let placeholder = arg
            .get_value_names()
            .and_then(|ns| ns.first())
            .map(|n| n.to_string().to_uppercase())
            .unwrap_or_else(|| arg.get_id().as_str().to_uppercase());
        s.push_str(&format!(" <{placeholder}>"));
    }
    s
}

fn format_option_right(arg: &clap::Arg) -> String {
    let first_line = arg
        .get_help()
        .map(|h| h.to_string().lines().next().unwrap_or("").trim().to_owned())
        .unwrap_or_default();

    if let Some(env) = arg.get_env() {
        let env_str = env.to_string_lossy();
        if first_line.is_empty() {
            format!("[env: {env_str}]")
        } else {
            format!("{first_line}  [env: {env_str}]")
        }
    } else {
        first_line
    }
}

fn is_value_taking(arg: &clap::Arg) -> bool {
    !matches!(
        arg.get_action(),
        clap::ArgAction::SetTrue
            | clap::ArgAction::SetFalse
            | clap::ArgAction::Count
            | clap::ArgAction::Help
            | clap::ArgAction::Version
    )
}
