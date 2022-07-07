// Library imports
use bitvec::prelude::*;
use clap::{App, Arg};
use crossbeam_channel::{bounded, Receiver, Sender};
use hashbrown::HashMap;
use regex::Regex;
use serde::Serialize;
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use threadpool::ThreadPool;

// From https://github.com/zaari/nmea-parser
const AIS_CHAR_BITS: usize = 6;

// Create a struct to carry the raw and parsed data
// Derive allows the struct to inherit some helper traits
// for serialization and copying.
#[derive(Serialize, Default, Clone, Debug)]
struct PositionReport {
    #[serde(skip_serializing)]
    pub sentence: String,
    pub landfall_time: String,
    pub group: String,
    pub satellite_acquisition_time: String,
    pub source: String,
    pub channel: String,
    pub raw_payload: String,
    pub message_type: u64,
    pub message_class: String,
    pub mmsi: String,
    pub latitude: f64,
    pub longitude: f64,
    pub call_sign: String,
    pub destination: String,
    pub name: String,
    pub ship_type: String,
    pub eta: String,
    pub draught: String,
    pub imo: String,
    pub course_over_ground: String,
    pub position_accuracy: String,
    pub speed_over_ground: String,
    pub navigation_status: String,
} // endof struct PositionReport

// convert six bit ascii to bitvec
// From https://github.com/zaari/nmea-parser
fn parse_payload(payload: &str) -> BitVec {
    let mut bv = BitVec::with_capacity(payload.len() * 6);
    for c in payload.chars() {
        let mut ci = (c as u8) - 48;
        if ci > 40 {
            ci -= 8;
        }
        // Pick bits
        for i in 0..6 {
            bv.push(((ci >> (5 - i)) & 0x01) != 0);
        }
    }
    bv
} // end of parse_payload

// Take the bit slice and convert to a unsigned integer
// From https://github.com/zaari/nmea-parser
fn pick_u64(bv: &BitVec, index: usize, len: usize) -> u64 {
    let mut res = 0;
    for pos in index..(index + len) {
        if let Some(b) = bv.get(pos) {
            res = (res << 1) | (*b as u64);
        } else {
            res <<= 1;
        }
    }
    res
} // end of pick_u64

// Take the bit slice and convert to a signed integer
// From https://github.com/zaari/nmea-parser
fn pick_i64(bv: &BitVec, index: usize, len: usize) -> i64 {
    let mut res = 0;
    for pos in index..(index + len) {
        if let Some(b) = bv.get(pos) {
            res = (res << 1) | (*b as u64);
        } else {
            res <<= 1;
        }
    }
    let sign_bit = 1 << (len - 1);
    if res & sign_bit != 0 {
        ((res & (sign_bit - 1)) as i64) - (sign_bit as i64)
    } else {
        res as i64
    }
} // end of pick_i64

// Take the bit slice and convert to a string
// From https://github.com/zaari/nmea-parser
fn pick_string(bv: &BitVec, index: usize, char_count: usize) -> String {
    let mut res = String::with_capacity(char_count);
    for i in 0..char_count {
        // unwraps below won't panic as char_from::u32 will only ever receive values between
        // 32..=96, all of which are valid. Catch all branch is unreachable as we only request
        // 6-bits from the BitVec.
        match pick_u64(bv, index + i * AIS_CHAR_BITS, AIS_CHAR_BITS) as u32 {
            0 => break,
            ch if ch < 32 => res.push(core::char::from_u32(64 + ch).unwrap()),
            ch if ch < 64 => res.push(core::char::from_u32(ch).unwrap()),
            ch => unreachable!("6-bit AIS character expected but value {} encountered!", ch),
        }
    }
    let trimmed_len = res.trim_end().len();
    res.truncate(trimmed_len);
    res
} // end of pick string

// Format numbers for human readable output.
// From https://stackoverflow.com/questions/34711832/human-readable-numbers
fn readable(mut o_s: String) -> String {
    let mut s = String::new();
    let mut negative = false;
    let values: Vec<char> = o_s.chars().collect();
    if values[0] == '-' {
        o_s.remove(0);
        negative = true;
    }
    for (i, char) in o_s.chars().rev().enumerate() {
        if i % 3 == 0 && i != 0 {
            s.insert(0, ',');
        }
        s.insert(0, char);
    }
    if negative {
        s.insert(0, '-');
    }
    s
} // fn readable

// Parse the AIS payload by extracting the message type, slicing out the bit values
// and converting the bitvec to the appropriate data type.
fn decode_payload(mut line: PositionReport) -> PositionReport {
    // Convert the payload to a bitstring and extract the message type.
    let payload = parse_payload(&line.raw_payload);
    line.message_type = pick_u64(&payload, 0, 6);

    // Check the message type and extract data from the payload into the struct.
    match line.message_type {
        1 | 2 | 3 => {
            // If the message is class A kinetic.
            line.mmsi = format!("{}", pick_u64(&payload, 8, 30));
            line.latitude = pick_i64(&payload, 89, 27) as f64 / 600_000.0;
            line.longitude = pick_i64(&payload, 61, 28) as f64 / 600_000.0;
            line.position_accuracy = pick_u64(&payload, 60, 1).to_string();
            line.speed_over_ground = pick_u64(&payload, 50, 10).to_string();
            line.course_over_ground = pick_u64(&payload, 116, 27).to_string();
            line.navigation_status = pick_u64(&payload, 38, 4).to_string();
        }
        5 => {
            // If the message is class A static.
            line.mmsi = format!("{}", pick_u64(&payload, 8, 30));
            line.call_sign = pick_string(&payload, 70, 42);
            line.name = pick_string(&payload, 112, 120);
            line.ship_type = pick_u64(&payload, 232, 8).to_string();
            line.imo = pick_u64(&payload, 40, 30).to_string();
            line.destination = pick_string(&payload, 302, 120);
            let month = pick_u64(&payload, 274, 4);
            let day = pick_u64(&payload, 278, 5);
            let hour = pick_u64(&payload, 278, 5);
            let minute = pick_u64(&payload, 288, 6);
            let datestub = minute * 60 + hour * 3600 + day * 86400 + month * 2678400;
            let year: f64 = {
                if !line.satellite_acquisition_time.is_empty() {
                    line.satellite_acquisition_time.parse::<f64>().unwrap() / 31_536_000.0
                } else {
                    "0".parse::<f64>().unwrap()
                }
            };
            line.eta = (((year * 31_536_000.0) as u64) + datestub).to_string();
            line.draught = pick_u64(&payload, 294, 8).to_string();
        }
        18 => {
            // If the message is class B kinetic report.
            line.mmsi = format!("{}", pick_u64(&payload, 8, 30));
            line.longitude = pick_i64(&payload, 57, 28) as f64 / 600_000.0;
            line.latitude = pick_i64(&payload, 85, 27) as f64 / 600_000.0;
            line.position_accuracy = pick_u64(&payload, 56, 1).to_string();
            line.course_over_ground = pick_u64(&payload, 112, 12).to_string();
            line.speed_over_ground = pick_u64(&payload, 46, 10).to_string();
        }
        19 => {
            // If the message is a class B extended position report
            line.mmsi = format!("{}", pick_u64(&payload, 8, 30));
            line.speed_over_ground = pick_u64(&payload, 46, 10).to_string();
            line.course_over_ground = pick_u64(&payload, 112, 12).to_string();
            line.position_accuracy = pick_u64(&payload, 56, 1).to_string();
            line.name = pick_string(&payload, 143, 120);
            line.ship_type = pick_u64(&payload, 263, 8).to_string();
        }
        _ => {
            // Message values not covered by the above cases.
        }
    }
    line
}

// Take the last four characters of a string slice.
fn last_four_characters(text: &str) -> &str {
    let len = text.len();
    if len > 3 {
        &text[len - 4..len]
    } else {
        ""
    }
} // endof last_four_characters

fn main() {
    // Workers are the number of CPUs.
    let n_workers = num_cpus::get();

    // Process the command line arguments
    let matches = App::new("AIS parsing program")
        .version("1.0")
        .author("Scott Syms <ezrapound1967@gmail.com>")
        .about("Does selective parsing of a raw AIS stream")
        .arg(
            Arg::new("INPUT")
                .help("Sets the input file to use")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("OUTPUT")
                .help("Sets a custom output file")
                .required(true)
                .takes_value(true)
                .index(2),
        )
        .arg(
            Arg::new("FLOW_LIMIT")
                .help(
                    "Sets a limit on the number of objects in memory at one time (default: 500000)",
                )
                .takes_value(true)
                .index(3),
        )
        .arg(
            Arg::new("PARSE_THREADS")
                .help(
                    "Sets the number of threads to use for parsing (default: number of CPUs)",
                )
                .takes_value(true)
                .index(4),
        )
        .arg(
            Arg::new("MULTILINE_THREADS")
                .help(
                    "Sets the number of threads to use for multiline parsing (default: number of CPUs)",
                )
                .takes_value(true)
                .index(5),
        )
        .get_matches();

    // Match the file input variable with
    let input_file: String = {
        if let Some(i) = matches.value_of("INPUT") {
            i.to_string()
        } else {
            "".to_string()
        }
    }; // let input_file

    // Match the output variable
    let output_file: String = {
        if let Some(i) = matches.value_of("OUTPUT") {
            i.to_string()
        } else {
            "".to_string()
        }
    }; // let output_file

    // Match the flow_limit variable
    let flow_limit: usize = {
        if let Some(i) = matches.value_of("FLOW_LIMIT") {
            i.parse::<usize>().unwrap()
        } else {
            // default value
            500_000
        }
    }; // let flow_limit

    // Match the parse_threads variable
    let extraction_threads: usize = {
        if let Some(i) = matches.value_of("EXTRACTION_THREADS") {
            i.parse::<usize>().unwrap()
        } else {
            // default value
            n_workers
        }
    }; // let parse_threads

    // Match the multiline_threads variable
    let multiline_threads: usize = {
        if let Some(i) = matches.value_of("MULTILINE_THREADS") {
            i.parse::<usize>().unwrap()
        } else {
            // default value
            n_workers
        }
    }; // let multiline_threads

    // Initiate Hashmaps for multisentence AIS messages
    // These are wrapped by ARC and Mutexes for use under multithreading.
    let payload_cache: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let source_cache: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let sat_time_cache: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    /*
    Create the crossbeam channels to relay the data across threads.
    source rx/tx relays the raw file line by line
    multiline rx/tx relays the multiline fragments to the processor
    output rx/tx relays the parsed data to the file writer rendered as a JSON string
    */
    let (raw_file_tx, raw_file_rx): (Sender<PositionReport>, Receiver<PositionReport>) =
        bounded(flow_limit);
    let (multiline_handling_tx, multiline_handling_rx): (
        Sender<PositionReport>,
        Receiver<PositionReport>,
    ) = bounded(flow_limit);
    let (ready_for_output_tx, ready_for_output_rx): (Sender<String>, Receiver<String>) =
        bounded(flow_limit);

    // clone a channel for use in the threads
    let extract_ready_for_output_tx = ready_for_output_tx.clone();

    // How many milliseconds to wait before calling
    // the data queue empty.
    let queue_timeout = 5 * 1000;

    /*
    Create three thread pools
    The first reads the file and sends each line to the processing threads
    for extraction and parsing.
    The second spawns a number of threads to parse the single line data and pass the rest to the multiline parser
    The final pool assembles multiline messages from fragments.
    */
    let reading_thread = ThreadPool::new(1);
    let extraction_pool = ThreadPool::new(extraction_threads);
    let multiline_assembly_thread = ThreadPool::new(multiline_threads);

    // Compile the regexes used to do the initial parse of the raw file
    let sat_re = Regex::new(r"c:(\d+)").unwrap();
    let source_re = Regex::new(r"s:([-\w]+)").unwrap();
    let group_re = Regex::new(r"g:([-\d]+)").unwrap();
    let landfall_re = Regex::new(r"^(\d+)").unwrap();

    for _b in 0..n_workers {
        // Initiate Hashmaps for multisentence AIS messages
        let payload_cache = Arc::clone(&payload_cache);
        let source_cache = Arc::clone(&source_cache);
        let sat_time_cache = Arc::clone(&sat_time_cache);

        // Clonen an output channel for use in the threads
        let ready_for_output_tx = ready_for_output_tx.clone();

        // Clone the crossbeam channels for use in thread
        // let output_tx = output_tx.clone();
        let multiline_handling_rx = multiline_handling_rx.clone();

        // Cache and reassemble the multiline AIS fragments.  Add the results to the output channel.
        multiline_assembly_thread.execute(
            move || {
                println!("Start of assembly thread...");

                let mut counter: i32 = 0;

                // Iterate over the output channel
                while let Ok(mut line) =
                    multiline_handling_rx.recv_timeout(Duration::from_millis(queue_timeout))
                {
                    //     // initiate a counter for the number of lines in the multiline message
                    counter += 1;
                    if counter % 100000 == 0 {
                        println!(
                            "Received {} lines for multiline handling.",
                            readable(counter.to_string())
                        );
                    }
                    // ****************************************************************
                    // Split the line on comma and pick out group, group_suffix and payload
                    // let parsed_line: Vec<_> = line.split(",").collect::<Vec<_>>();
                    // let group = &parsed_line[1];
                    // let group_suffix = last_four_characters(group);
                    // let payload = &parsed_line[5];

                    let mut payload_lock = payload_cache.lock().unwrap();
                    let mut source_lock = source_cache.lock().unwrap();
                    let mut sat_time_lock = sat_time_cache.lock().unwrap();

                    // save the payload to the group cache
                    // payload_lock.insert(parsed_line[1].to_string(), payload.to_string());
                    payload_lock.insert(line.group.clone(), line.raw_payload.clone());

                    // insert into time cache if parsed_line[3] is not empty
                    if !line.satellite_acquisition_time.is_empty() {
                        sat_time_lock.insert(line.group.clone(), line.satellite_acquisition_time);
                    }

                    // insert into source_cache if parsed_line[3] is not empty
                    if !line.source.is_empty() {
                        source_lock.insert(line.group.clone(), line.source);
                    }

                    // Create key variants for the group
                    let part1 = format!("1-2-{}", last_four_characters(&line.group));
                    let part2 = format!("2-2-{}", last_four_characters(&line.group));

                    // If both keys exist in the group cache, assemble the multiline message
                    if payload_lock.contains_key(&part1) && payload_lock.contains_key(&part2) {
                        line.raw_payload = format!(
                            "{}{}",
                            payload_lock.remove(&part1).unwrap(),
                            payload_lock.remove(&part2).unwrap()
                        );

                        // check to see if part1 is a key in time cache and return the values
                        line.satellite_acquisition_time = if sat_time_lock.contains_key(&part1) {
                            sat_time_lock.remove(&part1).unwrap().to_string()
                        } else {
                            "".to_string()
                        };

                        // check to see if part1 is a key in source_cache and return the values
                        line.source = if source_lock.contains_key(&part1) {
                            source_lock.remove(&part1).unwrap().to_string()
                        } else {
                            "".to_string()
                        };
                        // println!("Combined multiline: {:?}", line);
                        line = decode_payload(line);
                        let line_json = serde_json::to_string(&line).unwrap();
                        ready_for_output_tx.send(line_json).unwrap();
                    }

                    // *******
                }
                println!("End of multiline assembly thread");
            }, // endof multiline_assembly_thread
        );
    }

    // Create the threads that do the initial parsing of the raw input data.
    // Single sentence data is sent to the AIS parser and the combined results
    // sent to the output channel.
    // Multiline messages are placed on the multiline channel for handling.

    // Create the threads
    for _a in 0..n_workers {
        // Clone the channels and compiled regexs so they can be used across threads
        let raw_file_rx = raw_file_rx.clone().clone();
        let extract_ready_for_output_tx = extract_ready_for_output_tx.clone();
        let multiline_handling_tx = multiline_handling_tx.clone();
        let sat_re = sat_re.clone();
        let source_re = source_re.clone();
        let landfall_re = landfall_re.clone();
        let group_re = group_re.clone();

        // Instantiate a parser
        // let mut parser = NmeaParser::new();

        // Create the thread
        extraction_pool.execute(move || {
            while let Ok(mut line) = raw_file_rx.recv_timeout(Duration::from_millis(queue_timeout))
            {
                // Split the comma delimited line and pick out the payload and other elements
                let payload = &line.sentence.split(",").collect::<Vec<_>>();
                line.channel = payload[payload.len() - 3].to_string();
                line.raw_payload = payload[payload.len() - 2].to_string();
                // println!("RAW: Payload: {:?}", line.raw_payload);

                // When did the data reach a groundstation?
                line.landfall_time = {
                    match landfall_re.captures(&line.sentence) {
                        Some(x) => x.get(1).unwrap().as_str().to_string(),
                        None => "".to_string(),
                    }
                };

                // Is the sentence part of a multiline group?
                line.group = {
                    match group_re.captures(&line.sentence) {
                        Some(x) => x.get(1).unwrap().as_str().to_string(),
                        None => "".to_string(),
                    }
                };

                // When did the satellite get the message?
                line.satellite_acquisition_time = {
                    match sat_re.captures(&line.sentence) {
                        Some(x) => x.get(1).unwrap().as_str().to_string(),
                        None => "".to_string(),
                    }
                };

                // What is the source of the message?
                line.source = {
                    match source_re.captures(&line.sentence) {
                        Some(x) => x.get(1).unwrap().as_str().to_string(),
                        None => "".to_string(),
                    }
                };

                // If it's a single-line message, send it to the output channel
                // Otherwise push it to the multiline handler
                if line.group.is_empty() {
                    line.message_class = "singleline".to_string();
                    let line = decode_payload(line);
                    let line_json = serde_json::to_string(&line).unwrap();
                    extract_ready_for_output_tx.send(line_json).unwrap();

                    // extract_ready_for_output_tx.send(line).unwrap();
                } else {
                    line.message_class = "multiline".to_string();
                    multiline_handling_tx.send(line).unwrap();
                }
            }
            println!("Single line handling thread terminated");
        });
    }

    // Open a file, read each line and insert the info in the struct
    reading_thread.execute(move || {
        let mut counter: i32 = 0;

        // Open up the files and read, read, read
        let file = File::open(input_file).expect("file not found");
        let reader = BufReader::new(file);

        // For each line, start teasing out the AIS data
        // for line in reader.lines() {
        for line in reader.lines() {
            let line = line;
            let line: String = {
                match line {
                    Ok(i) => i,
                    Err(_e) => "".to_string(),
                }
            }; // endof sentence

            // The line has to have VDM in it
            let isais = line.find("VDM");
            if line.find("VDM") == None {
                continue;
            }

            // Initialize the struct
            let current_sentence = PositionReport {
                sentence: line,
                ..Default::default()
            };

            // Send the struct to the parsing thread and echo a progress count
            let send_this = raw_file_tx.send(current_sentence);
            match send_this {
                Ok(_) => {
                    counter += 1;
                    if counter % 100000 == 0 {
                        println!(
                            "Sent {} lines to the parsing threads.",
                            readable(counter.to_string())
                        );
                    }
                }
                Err(_e) => {
                    continue;
                }
            }
        } // end of line read
        drop(raw_file_tx);
        println!("File is read.");
    }); // reading thread

    // Start the process to write the output
    // initialize a counter for file lines
    let mut counter = 0;

    // open the output file and buffer
    let output = File::create(output_file).unwrap();
    let mut buf = BufWriter::new(output);

    // Consume the results from the ready_for_output_rx channel and write to the output file
    while let Ok(line) = ready_for_output_rx.recv_timeout(Duration::from_millis(queue_timeout)) {
        counter += 1;
        // Print the line count every 100000 lines
        if counter % 100000 == 0 {
            println!("Writing {} lines to file.", readable(counter.to_string()));
        }
        write!(buf, "{}\n", line);
    }

    // wait for the threads to complete
    reading_thread.join();
    multiline_assembly_thread.join();
} //  endof main
