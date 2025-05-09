use std::{cmp::Reverse, collections::BTreeSet, fs::remove_file, io::{BufRead, Seek, Write}};
use serde_json::Value as JValue;
use clap::Parser;
use sha2::Digest;

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
        #[arg(long)]
        /// the key to extract
        key: String,
        #[arg(long)]
        /// the input file name
        input: String,
        #[arg(long)]
        /// the output file name
        output: String
    },
    /// extract keys and hash these keys to a 
    JsonArrayExtractSHA256 {
        #[arg(long, value_delimiter = ',')]
        keys: Vec<String>,
        #[arg(long)]
        input: String,
        #[arg(long)]
        output: String
    },
    /// verify a string is sorted & distinct
    VerifyStringSortedDistinct {
        /// the input file name
        #[arg(long)]
        input: String,
    },
    /// diff two text files
    DiffSortedString {
        #[arg(long)]
        input_a: String,
        #[arg(long)]
        input_b: String,
        #[arg(long)]
        output_a_minus_b: String,
        #[arg(long)]
        output_b_minus_a: String,
        #[arg(long)]
        output_intersect: String
    },
    /// diff two text files using naive method
    DiffSortedStringNaive {
        #[arg(long)]
        input_a: String,
        #[arg(long)]
        input_b: String,
        #[arg(long)]
        output_a_minus_b: String,
        #[arg(long)]
        output_b_minus_a: String,
        #[arg(long)]
        output_intersect: String
    },
    /// Run external sorting on a string
    ExternalSort {
        #[arg(long)]
        input: String,
        #[arg(long)]
        output: String,
        #[arg(long)]
        intermediate: String,
        #[arg(long, default_value="4194304")]
        batch_size: usize,
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

pub struct MergeStream<I: Iterator>(Option<I::Item>, I);

impl<I: Iterator> MergeStream<I> {
    fn new(mut iterator: I) -> Option<Self> {
        iterator.next().map(|item| Self(Some(item), iterator))
    }
}

impl<I: Iterator> PartialEq for MergeStream<I> where I::Item: Ord {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<I: Iterator> Eq for MergeStream<I> where I::Item: Ord {}

impl<I: Iterator> PartialOrd for MergeStream<I> where I::Item: Ord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl<I: Iterator> Ord for MergeStream<I> where I::Item: Ord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<I: Iterator> Iterator for MergeStream<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_none() {
            None
        } else if let Some(next) = self.1.next() {
            self.0.replace(next)
        } else {
            self.0.take()
        }
    }
}

fn main() {
    match Cli::parse().sub {
        Sub::ExternalSort { input, output, intermediate, batch_size } => {
            let input = file!(<R> input);
            let mut temp = file!(<W> intermediate);
            let mut offsets = vec![];
            let mut batch = vec![];
            for line in input.lines() {
                let Ok(line) = &line else {
                    println!("WARNING: a line is somehow corrupted {line:?}");
                    continue;
                };
                batch.push(line.to_string());
                if batch.len() == batch_size {
                    batch.sort();
                    let Ok(sp) = temp.stream_position() else {
                        println!("FATAL: cannnot get stream position from intermediate file");
                        return;
                    };
                    offsets.push(sp);
                    for s in batch.drain(..) {
                        writeln!(&mut temp, "{s}").expect("FATAL: write to intermediate file failed");
                    }
                }
            }
            if batch.len() != 0 {
                batch.sort();
                let Ok(sp) = temp.stream_position() else {
                    println!("FATAL: cannnot get stream position from intermediate file");
                    return;
                };
                println!("STREAM POSITION: {sp}");
                offsets.push(sp);
                for s in batch.drain(..) {
                    writeln!(&mut temp, "{s}").expect("FATAL: write to intermediate file failed");
                }
            }
            drop(temp);
            let mut merge_streams = std::collections::BinaryHeap::new();
            for offset in offsets {
                let mut temp = file!(<R> intermediate);
                temp.seek(std::io::SeekFrom::Start(offset))
                    .expect("FATAL: cannot open temporary file for seeking");
                let temp = temp.lines()
                    .map(|x| x.expect("FATAL: cannot read a line from intermediate file"))
                    .take(batch_size);
                let Some(stream) = MergeStream::new(temp) else {
                    println!("WARNING: empty stream");
                    return;
                };
                merge_streams.push(Reverse(stream));
            }
            let mut output = file!(<W> output);
            while let Some(Reverse(mut top)) = merge_streams.pop() {
                let Some(next) = top.next() else { continue };
                writeln!(&mut output, "{next}").expect("FATAL: write to final output failed");
                merge_streams.push(Reverse(top));
            }
            // remove_file(intermediate).expect("FATAL: cannot remove intermediate file");
        }
        Sub::JsonArrayExtractSHA256 { keys, input, output } => {
            let input = file!(<R> input);
            let mut output = file!(<W> output);
            'outer: for line in input.lines() {
                let Ok(line) = &line else {
                    println!("WARNING: a line is somehow corrupted {line:?}");
                    continue;
                };
                let Ok(JValue::Object(map)) = serde_json::from_str::<JValue>(&line) else {
                    println!("WARNING: a line cannot be parsed as a json map");
                    continue;
                };
                let mut sha256 = sha2::Sha256::default();
                for key in &keys {
                    use std::io::Write;
                    if let Some(jvalue) = map.get(key) {
                        write!(&mut sha256, "{}", jvalue.to_string()).expect("FATAL: write to output failed");
                    } else {
                        println!("WARNING: no such key in json map s.t. the value is string. ");
                        continue 'outer;
                    }
                }
                writeln!(&mut output, "0x{:x}", sha256.finalize()).expect("FATAL: write to output failed");
            }
        }
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
        Sub::DiffSortedStringNaive {
            input_a, 
            input_b, 
            output_a_minus_b, 
            output_b_minus_a, 
            output_intersect
        } => {
            use std::io::Write;
            let input_a = file!(<R> input_a).lines().filter_map(|x| x.ok()).collect::<BTreeSet<String>>();
            let input_b = file!(<R> input_b).lines().filter_map(|x| x.ok()).collect::<BTreeSet<String>>();
            let mut output_a_minus_b = file!(<W> output_a_minus_b);
            let mut output_b_minus_a = file!(<W> output_b_minus_a);
            let mut output_intersect = file!(<W> output_intersect);
            for line in input_a.difference(&input_b) {
                writeln!(&mut output_a_minus_b, "{line}")
                    .expect("FATAL: write to output failed");
            }
            for line in input_b.difference(&input_a) {
                writeln!(&mut output_b_minus_a, "{line}")
                    .expect("FATAL: write to output failed");
            }
            for line in input_a.intersection(&input_b) {
                writeln!(&mut output_intersect, "{line}")
                    .expect("FATAL: write to output failed");
            }
            output_a_minus_b.flush().expect("FATAL: file flush failed");
            output_b_minus_a.flush().expect("FATAL: file flush failed");
            output_intersect.flush().expect("FATAL: file flush failed");
        }
        Sub::VerifyStringSortedDistinct {
            input,
        } => {
            let mut input = file!(<R> input).lines().enumerate().peekable();
            while let Some((at, line)) = input.next() {
                if at % 100000 == 0 && at != 0 {
                    println!("[PROGRESS] AT LINE {at}");
                }
                let Some((_, line_next)) = input.peek() else {
                    break;
                };
                if line.is_err() || line_next.is_err() {
                    println!("[ERROR] File Broken!");
                    return;
                }
                if &line.unwrap() >= line_next.as_ref().unwrap() {
                    println!("[ERROR] Not Sorted! AT LINE {at}");
                    return;
                }
            }
            println!("[OK] Sorted");
        }
    }
}
