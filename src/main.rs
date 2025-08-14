use std::env;

fn print_help() {
    let help = "\
spr creates a series of GitHub PRs that are 'stacked' by managing the base branch
Each commit that should start a new PR should be tagged with `pr:<unique name>`
spr update creates 1 PR per commit with a `pr:<tag>`
Any intermediate commits without a tag get added to the first ancestor PR that has a tag, as a separate commit";

    println!("{help}");
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|a| a == "--help" || a == "--h") {
        print_help();
    }
}
