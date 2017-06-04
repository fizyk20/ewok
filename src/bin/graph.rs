extern crate regex;
extern crate clap;
use regex::Regex;
use clap::{App, Arg};
use std::collections::{BTreeSet, BTreeMap};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::io::{BufReader, BufRead, Write, BufWriter};
use std::fmt;

fn hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Members(pub BTreeSet<String>);

impl fmt::Display for Members {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut count = 0;
        for name in &self.0 {
            count += 1;
            write!(f, "<font color=\"#{}\">{}</font>, ", name, name)?;
            if count % 3 == 0 {
                write!(f, "<br/>")?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Block {
    pub prefix: String,
    pub version: u64,
    pub members: Members,
}

impl Block {
    fn get_id(&self) -> String {
        format!("prefix{}_v{}_{}",
                self.prefix,
                self.version,
                hash(&self.members))
    }

    fn get_label(&self) -> String {
        format!("<Prefix: ({})<br/>Version: {}<br/>Members: <br/>{}>",
                self.prefix,
                self.version,
                self.members)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Vote {
    pub from: String,
    pub to: String,
}

fn gen_output(name: &str, blocks: &BTreeMap<String, Block>, votes: &BTreeSet<Vote>) {
    let file = File::create(name).unwrap();
    let mut writer = BufWriter::new(file);
    let _ = write!(writer, "digraph {{\n");
    for (b, block) in blocks {
        let _ = write!(writer,
                       "{} [label = {}; shape=box];\n",
                       b,
                       block.get_label());
    }
    for vote in votes {
        let _ = write!(writer, "{}->{}\n", vote.from, vote.to);
    }
    let _ = write!(writer, "}}\n");
}

fn step_file(name: &str, step: u64) -> String {
    if name.ends_with(".dot") {
        let (name1, name2) = name.split_at(name.len() - 4);
        format!("{}.{}{}", name1, step, name2)
    } else {
        format!("{}.{}.dot", name, step)
    }
}

fn main() {
    let agreement_re = Regex::new(r"^Node\((?P<node>[0-9a-f]{6}\.\.)\): received agreement for Vote \{ from: Block \{ prefix: Prefix\((?P<pfrom>[01]*)\), version: (?P<vfrom>\d+), members: \{(?P<mfrom>[0-9a-f]{6}\.\.(, [0-9a-f]{6}\.\.)*)\} \}, to: Block \{ prefix: Prefix\((?P<pto>[01]*)\), version: (?P<vto>\d+), members: \{(?P<mto>[0-9a-f]{6}\.\.(, [0-9a-f]{6}\.\.)*)\} \} \}").unwrap();
    let step_re = Regex::new(r"^-- step \d+ --").unwrap();

    let matches = App::new("ewok_graph")
        .about("Generates DOT files from Ewok logs")
        .arg(Arg::with_name("output")
                 .short("o")
                 .long("output")
                 .value_name("FILE")
                 .help("The name for the output file."))
        .arg(Arg::with_name("INPUT")
                 .help("Sets the input file to use")
                 .required(true)
                 .index(1))
        .arg(Arg::with_name("step")
                 .short("s")
                 .long("step")
                 .help("If set, generates a DOT file for each simulation step")
                 .takes_value(false))
        .get_matches();
    let input = matches.value_of("INPUT").unwrap();
    let output = matches.value_of("output").unwrap_or("output.dot");
    let step_by_step = matches.is_present("step");
    let mut blocks = BTreeMap::new();
    let mut votes = BTreeSet::new();

    let file = File::open(input).unwrap();
    let mut reader = BufReader::new(file);
    let mut line = String::new();

    println!("Reading log...");
    let mut step = 0;
    while reader.read_line(&mut line).unwrap() > 0 {
        if step_by_step && step_re.is_match(&line) {
            if step > 0 {
                println!("Outputting step {}...", step - 1);
                let filename = step_file(output, step - 1);
                gen_output(&filename, &blocks, &votes);
            }
            step += 1;
            line.clear();
            continue;
        }
        if let Some(caps) = agreement_re.captures(&line) {
            let block_from = Block {
                prefix: caps["pfrom"].to_owned(),
                version: caps["vfrom"]
                    .parse()
                    .ok()
                    .expect("invalid version number"),
                members: Members(caps["mfrom"]
                                     .split(", ")
                                     .map(|s| s.to_owned())
                                     .collect()),
            };
            let block_to = Block {
                prefix: caps["pto"].to_owned(),
                version: caps["vto"]
                    .parse()
                    .ok()
                    .expect("invalid version number"),
                members: Members(caps["mto"].split(", ").map(|s| s.to_owned()).collect()),
            };
            let from_id = block_from.get_id();
            let to_id = block_to.get_id();
            blocks.insert(from_id.clone(), block_from);
            blocks.insert(to_id.clone(), block_to);
            let vote = Vote {
                from: from_id,
                to: to_id,
            };
            votes.insert(vote);
        }
        line.clear();
    }

    println!("Reading finished. Outputting the dot file...");
    let name = if step_by_step {
        step_file(output, step - 1)
    } else {
        output.to_owned()
    };
    gen_output(&name, &blocks, &votes);
}
