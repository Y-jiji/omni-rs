use std::io::BufRead;
use serde_json::Value as JValue;
use clap::Parser;

#[derive(clap::Parser, std::fmt::Debug)]
#[command(version, about, long_about = None)]
/// Various command line tools for processing files
pub struct Cli {
    #[command(subcommand)]
    sub: Sub
}

#[derive(clap::Subcommand, std::fmt::Debug)]
#[command(version, about, long_about = None)]
pub enum Sub {
    /// extract a given key for jsona file
    JsonArrayExtractKey {
        #[arg(short, long)]
        /// the key to extract
        key: String,
        #[arg(short, long)]
        /// the input file name
        input: String,
        #[arg(short, long)]
        /// the output file name
        output: String
    },
    /// diff two text files
    DiffSortedString {
        #[arg(short, long)]
        input_a: String,
        #[arg(short, long)]
        input_b: String,
        #[arg(short, long)]
        output_a_minus_b: String,
        #[arg(short, long)]
        output_b_minus_a: String,
        #[arg(short, long)]
        output_intersect: String
    }
}

/// open file for read/write operations
macro_rules! file {
    (<R> $f: expr) => {
        {
            let Ok(input) = std::fs::File::open(&$f) else {
                println!("Cannot open input file! {:?}", $f);
                return;
            };
            std::io::BufReader::new(input)
        }
    };
    (<W> $f: expr) => {
        {
            let Ok(output) = std::fs::File::create(&$f) else {
                println!("Cannot open output file! {:?}", $f);
                return;
            };
            std::io::BufWriter::new(output)
        }
    }
}

fn main() {
    match Cli::parse().sub {
        Sub::JsonArrayExtractKey { key, input, output } => {
            let input = file!(<R> input);
            let mut output = file!(<W> output);
            for line in input.lines() {
                let Ok(line) = &line else {
                    println!("WARNING: a line is somehow corrupted {line:?}");
                    continue;
                };
                let Ok(JValue::Object(map)) = serde_json::from_str::<JValue>(&line) else {
                    println!("WARNING: a line cannot be parsed as a json map");
                    continue;
                };
                if let Some(JValue::String(value)) = map.get(&key) {
                    use std::io::Write;
                    writeln!(&mut output, "{value}").expect("FATAL: write to output failed");
                } else {
                    println!("WARNING: no such key in json map s.t. the value is string. ");
                }
            }
        }
        Sub::DiffSortedString {
            input_a, 
            input_b, 
            output_a_minus_b, 
            output_b_minus_a, 
            output_intersect
        } => {
            use std::io::Write;
            let mut input_a = file!(<R> input_a).lines().filter_map(|x| x.ok()).peekable();
            let mut input_b = file!(<R> input_b).lines().filter_map(|x| x.ok()).peekable();
            let mut output_a_minus_b = file!(<W> output_a_minus_b);
            let mut output_b_minus_a = file!(<W> output_b_minus_a);
            let mut output_intersect = file!(<W> output_intersect);
            while input_a.peek().is_some() || input_b.peek().is_some() {
                if input_a.peek().is_none() {
                    writeln!(&mut output_b_minus_a, "{}", input_b.next().unwrap())
                        .expect("FATAL: write to output failed");
                    continue;
                }
                if input_b.peek().is_none() {
                    writeln!(&mut output_a_minus_b, "{}", input_a.next().unwrap())
                        .expect("FATAL: write to output failed");
                    continue;
                }
                let a = input_a.peek().unwrap();
                let b = input_b.peek().unwrap();
                if a < b {
                    writeln!(&mut output_a_minus_b, "{a}")
                        .expect("FATAL: write to output failed");
                    input_a.next();
                } else if a > b {
                    writeln!(&mut output_b_minus_a, "{b}")
                        .expect("FATAL: write to output failed");
                    input_b.next();
                } else {
                    writeln!(&mut output_intersect, "{a}")
                        .expect("FATAL: write to output failed");
                    input_a.next();
                    input_b.next();
                }
            }
            output_a_minus_b.flush().expect("FATAL: file flush failed");
            output_b_minus_a.flush().expect("FATAL: file flush failed");
            output_intersect.flush().expect("FATAL: file flush failed");
        }
    }
}
