/// Print a unified help page with progressive disclosure (one level deep).
///
/// Prints the long_about (or about), a usage line, a one-level command table, global options,
/// and a footer hint. Groups (subcommands that have their own subcommands) are shown with just
/// their name; leaf subcommands include positional argument signatures.
/// Designed to be called from the no-subcommand branch of `main`.
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

    // One-level command table (hidden subcommands excluded)
    let commands = collect_direct_children(cmd);
    if !commands.is_empty() {
        println!("Commands:");
        let max_sig = commands.iter().map(|(s, _)| s.len()).max().unwrap_or(0);
        for (sig, desc) in &commands {
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

/// Collect one level of direct child subcommands for display.
///
/// Hidden subcommands (e.g. clap's auto-generated `help`) are skipped.
/// Groups (subcommands that themselves have subcommands) show only the subcommand name.
/// Leaves show the subcommand name followed by positional argument placeholders.
/// No parent path prefix is prepended — the caller's context is implied.
fn collect_direct_children(cmd: &clap::Command) -> Vec<(String, String)> {
    let mut result = Vec::new();
    for sub in cmd.get_subcommands() {
        // Skip hidden subcommands (explicitly hidden or clap's auto-generated "help").
        // Note: in clap 4.6 the auto-generated help subcommand is not marked via is_hide_set(),
        // so we also guard by name.
        if sub.is_hide_set() || sub.get_name() == "help" {
            continue;
        }
        let is_group = sub.get_subcommands().next().is_some();
        let sig = if is_group {
            // Groups: name only — children are discovered via their own help page.
            sub.get_name().to_owned()
        } else {
            build_leaf_signature(sub)
        };
        let desc = sub
            .get_about()
            .map(|a| {
                let first = a.to_string().lines().next().unwrap_or("").trim().to_owned();
                trim_trailing_punctuation(first)
            })
            .unwrap_or_default();
        result.push((sig, desc));
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

fn build_leaf_signature(cmd: &clap::Command) -> String {
    let mut parts: Vec<String> = vec![cmd.get_name().to_owned()];

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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Arg, Command};

    /// Root command with a group + a leaf: group is not expanded, leaf includes positional args.
    #[test]
    fn test_root_group_not_expanded_leaf_includes_args() {
        let mut root = Command::new("bus")
            .subcommand(
                Command::new("items")
                    .about("Item management")
                    .subcommand(Command::new("list").about("List items"))
                    .subcommand(
                        Command::new("add")
                            .about("Add an item")
                            .arg(Arg::new("content").required(true)),
                    ),
            )
            .subcommand(
                Command::new("status")
                    .about("Show status")
                    .arg(Arg::new("id").required(true)),
            );
        root.build();

        let children = collect_direct_children(&root);

        // "items" is a group — only its name, no expansion
        let items_entry = children.iter().find(|(s, _)| s.starts_with("items")).unwrap();
        assert_eq!(
            items_entry.0, "items",
            "group must show name only, not expanded children"
        );

        // "status" is a leaf — must include positional arg
        let status_entry = children.iter().find(|(s, _)| s.starts_with("status")).unwrap();
        assert_eq!(status_entry.0, "status <ID>");

        assert_eq!(children.len(), 2);
    }

    /// Group command passed directly to collect_direct_children: direct leaves rendered without
    /// parent prefix ("items " must NOT appear).
    #[test]
    fn test_group_expands_direct_leaves_without_parent_prefix() {
        let mut items_cmd = Command::new("items")
            .subcommand(Command::new("list").about("List items"))
            .subcommand(
                Command::new("add")
                    .about("Add an item")
                    .arg(Arg::new("content").required(true)),
            )
            .subcommand(
                Command::new("update")
                    .about("Update an item")
                    .arg(Arg::new("id").required(true))
                    .arg(Arg::new("content").required(true)),
            );
        items_cmd.build();

        let children = collect_direct_children(&items_cmd);

        assert_eq!(children.len(), 3);

        let sigs: Vec<&str> = children.iter().map(|(s, _)| s.as_str()).collect();
        // No parent prefix "items" in any signature
        assert!(sigs.contains(&"list"), "list must have no parent prefix");
        assert!(
            sigs.contains(&"add <CONTENT>"),
            "add must include positional, no prefix"
        );
        assert!(
            sigs.contains(&"update <ID> <CONTENT>"),
            "update must include positionals, no prefix"
        );
    }

    /// Clap's auto-generated hidden `help` subcommand must be filtered out.
    #[test]
    fn test_hidden_help_subcommand_filtered() {
        let mut root = Command::new("bus").subcommand(Command::new("status").about("Show status"));
        root.build();

        let children = collect_direct_children(&root);

        // Only "status" should appear; no "help"
        let names: Vec<&str> = children.iter().map(|(s, _)| s.as_str()).collect();
        assert!(!names.contains(&"help"), "hidden 'help' subcommand must be filtered");
        assert!(names.contains(&"status"));
    }

    /// Explicitly hidden subcommands are also filtered.
    #[test]
    fn test_explicitly_hidden_subcommand_filtered() {
        let mut root = Command::new("bus")
            .subcommand(Command::new("public").about("Public command"))
            .subcommand(Command::new("internal").about("Internal command").hide(true));
        root.build();

        let children = collect_direct_children(&root);

        let names: Vec<&str> = children.iter().map(|(s, _)| s.as_str()).collect();
        assert!(names.contains(&"public"));
        assert!(
            !names.contains(&"internal"),
            "explicitly hidden subcommand must be filtered"
        );
    }

    /// Optional positional args render with brackets; multi-value appends `...`.
    #[test]
    fn test_leaf_signature_optional_and_multi_args() {
        // A single optional multi-value positional (num_args 0..)
        let mut cmd_multi = Command::new("search").arg(Arg::new("query").required(false).num_args(0..));
        cmd_multi.build();
        let sig = build_leaf_signature(&cmd_multi);
        assert!(sig.contains("[QUERY]..."), "multi optional must be [ARG]...");

        // A required positional followed by a single optional — valid ordering in clap
        let mut cmd_mixed = Command::new("copy")
            .arg(Arg::new("src").required(true))
            .arg(Arg::new("dest").required(false));
        cmd_mixed.build();
        let sig2 = build_leaf_signature(&cmd_mixed);
        assert!(sig2.contains("<SRC>"), "required arg must use <>");
        assert!(sig2.contains("[DEST]"), "optional arg must use []");
        assert!(!sig2.contains("<DEST>"), "optional must not use <>");
    }

    /// Global options propagation still works after build().
    #[test]
    fn test_global_options_visible_after_build() {
        let mut root = Command::new("test-app")
            .arg(Arg::new("config").long("config").global(true).help("Config file path"))
            .subcommand(Command::new("items").subcommand(Command::new("list")));
        root.build();

        let root_globals: Vec<_> = root.get_arguments().filter(|a| a.is_global_set()).collect();
        assert!(!root_globals.is_empty(), "root must have global options");

        let items = root.find_subcommand("items").expect("items subcommand must exist");
        let items_globals: Vec<_> = items.get_arguments().filter(|a| a.is_global_set()).collect();
        assert!(
            !items_globals.is_empty(),
            "subcommand must inherit global options after build"
        );
    }
}
