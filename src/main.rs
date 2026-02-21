use clap::{App, Arg};
use crossbeam_channel::{bounded, Sender};
use hashbrown::HashMap;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread;

const AIS_CHAR_BITS: usize = 6;
/// Lines accumulated per batch before sending to the writer channel
const BATCH_SIZE: usize = 2048;
/// Output BufWriter buffer (64 MB)
const WRITER_BUF_SIZE: usize = 64 * 1024 * 1024;
/// Max AIS payload characters (type 5 is the longest, ~70 chars; 128 is safe)
const MAX_PAYLOAD_CHARS: usize = 128;

// ─── PositionReport (only used for multiline caching) ───────────────────────

#[derive(Default, Clone, Debug)]
struct PositionReport {
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
}

// ─── Stack-allocated AIS payload (no heap allocation per message) ────────────

struct Payload {
    data: [u8; MAX_PAYLOAD_CHARS],
    len: usize,
}

impl Payload {
    #[inline]
    fn from_str(s: &str) -> Self {
        let mut p = Self { data: [0u8; MAX_PAYLOAD_CHARS], len: 0 };
        for c in s.bytes().take(MAX_PAYLOAD_CHARS) {
            let mut ci = c.wrapping_sub(48);
            if ci > 40 { ci -= 8; }
            p.data[p.len] = ci;
            p.len += 1;
        }
        p
    }

    #[inline]
    fn as_slice(&self) -> &[u8] { &self.data[..self.len] }
}

// ─── Bit extraction ──────────────────────────────────────────────────────────

#[inline]
fn pick_u64(bv: &[u8], index: usize, len: usize) -> u64 {
    let mut res: u64 = 0;
    for pos in index..(index + len) {
        let ci = pos / 6;
        let bi = 5 - (pos % 6);
        let bit = if ci < bv.len() { ((bv[ci] >> bi) & 1) as u64 } else { 0 };
        res = (res << 1) | bit;
    }
    res
}

#[inline]
fn pick_i64(bv: &[u8], index: usize, len: usize) -> i64 {
    let res = pick_u64(bv, index, len);
    let sign_bit = 1u64 << (len - 1);
    if res & sign_bit != 0 {
        ((res & (sign_bit - 1)) as i64) - (sign_bit as i64)
    } else {
        res as i64
    }
}

fn pick_string(bv: &[u8], index: usize, char_count: usize) -> String {
    let mut res = String::with_capacity(char_count);
    for i in 0..char_count {
        match pick_u64(bv, index + i * AIS_CHAR_BITS, AIS_CHAR_BITS) as u32 {
            0 => break,
            ch if ch < 32 => res.push(char::from_u32(64 + ch).unwrap()),
            ch if ch < 64 => res.push(char::from_u32(ch).unwrap()),
            ch => unreachable!("unexpected 6-bit value {}", ch),
        }
    }
    let t = res.trim_end().len();
    res.truncate(t);
    res
}

// ─── Field extraction helpers (return &str slices, zero allocation) ──────────

fn last_four_characters(text: &str) -> &str {
    let len = text.len();
    if len > 3 { &text[len - 4..] } else { "" }
}

#[inline]
fn extract_digits_after<'a>(s: &'a str, prefix: &str) -> &'a str {
    match s.find(prefix) {
        Some(i) => {
            let r = &s[i + prefix.len()..];
            &r[..r.find(|c: char| !c.is_ascii_digit()).unwrap_or(r.len())]
        }
        None => "",
    }
}

#[inline]
fn extract_digits_dashes_after<'a>(s: &'a str, prefix: &str) -> &'a str {
    match s.find(prefix) {
        Some(i) => {
            let r = &s[i + prefix.len()..];
            &r[..r.find(|c: char| !c.is_ascii_digit() && c != '-').unwrap_or(r.len())]
        }
        None => "",
    }
}

#[inline]
fn extract_word_after<'a>(s: &'a str, prefix: &str) -> &'a str {
    match s.find(prefix) {
        Some(i) => {
            let r = &s[i + prefix.len()..];
            &r[..r
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
                .unwrap_or(r.len())]
        }
        None => "",
    }
}

#[inline]
fn extract_leading_digits(s: &str) -> &str {
    &s[..s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len())]
}

// ─── JSON writing primitives (all write into an existing String, no allocs) ──

/// Write a u64 as decimal digits (stack buffer, no allocation)
#[inline]
fn push_u64(out: &mut String, mut n: u64) {
    if n == 0 { out.push('0'); return; }
    let mut buf = [0u8; 20];
    let mut i = 20usize;
    while n > 0 {
        i -= 1;
        buf[i] = (n % 10) as u8 + b'0';
        n /= 10;
    }
    out.push_str(unsafe { std::str::from_utf8_unchecked(&buf[i..]) });
}

/// Write a u64 as a quoted JSON string value  ("123")
#[inline]
fn push_u64_str(out: &mut String, n: u64) {
    out.push('"');
    push_u64(out, n);
    out.push('"');
}

/// Write a string value that never needs JSON escaping
#[inline]
fn push_safe_str(out: &mut String, s: &str) {
    out.push('"');
    out.push_str(s);
    out.push('"');
}

/// Write a string that may contain `"` or `\` (AIS name/callsign/destination)
#[inline]
fn push_escaped_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Write a f64 as a JSON number (ryu, stack buffer, no allocation)
#[inline]
fn push_f64(out: &mut String, v: f64) {
    let mut buf = ryu::Buffer::new();
    out.push_str(buf.format_finite(v));
}

// ─── Direct-to-buffer JSON for single-line messages (zero intermediate allocs) ─

/// Parse a single-line AIS sentence and append its JSON representation to `out`.
/// For common message types (1,2,3,18) this path has zero heap allocations.
fn append_single_line_json(sentence: &str, out: &mut String) {
    // Split NMEA fields from the right (no Vec allocation)
    let mut parts = sentence.rsplitn(4, ',');
    parts.next(); // skip fillbits*checksum
    let raw_payload = parts.next().unwrap_or("");
    let channel = parts.next().unwrap_or("");

    let landfall_time = extract_leading_digits(sentence);
    let sat_time = extract_digits_after(sentence, "c:");
    let source = extract_word_after(sentence, "s:");

    // Decode payload using stack buffer
    let pl = Payload::from_str(raw_payload);
    let pv = pl.as_slice();
    let message_type = pick_u64(pv, 0, 6);

    // Write header fields (common to all message types)
    out.push_str("{\"landfall_time\":");
    push_safe_str(out, landfall_time);
    out.push_str(",\"group\":\"\",\"satellite_acquisition_time\":");
    push_safe_str(out, sat_time);
    out.push_str(",\"source\":");
    push_safe_str(out, source);
    out.push_str(",\"channel\":");
    push_safe_str(out, channel);
    out.push_str(",\"raw_payload\":");
    push_safe_str(out, raw_payload);
    out.push_str(",\"message_type\":");
    push_u64(out, message_type);
    out.push_str(",\"message_class\":\"singleline\",\"mmsi\":");

    match message_type {
        1 | 2 | 3 => {
            let mmsi    = pick_u64(pv, 8, 30);
            let lat     = pick_i64(pv, 89, 27) as f64 / 600_000.0;
            let lon     = pick_i64(pv, 61, 28) as f64 / 600_000.0;
            let pos_acc = pick_u64(pv, 60, 1);
            let sog     = pick_u64(pv, 50, 10);
            let cog     = pick_u64(pv, 116, 27);
            let nav     = pick_u64(pv, 38, 4);

            push_u64_str(out, mmsi);
            out.push_str(",\"latitude\":");
            push_f64(out, lat);
            out.push_str(",\"longitude\":");
            push_f64(out, lon);
            out.push_str(",\"call_sign\":\"\",\"destination\":\"\",\"name\":\"\",\"ship_type\":\"\",\"eta\":\"\",\"draught\":\"\",\"imo\":\"\",\"course_over_ground\":");
            push_u64_str(out, cog);
            out.push_str(",\"position_accuracy\":");
            push_u64_str(out, pos_acc);
            out.push_str(",\"speed_over_ground\":");
            push_u64_str(out, sog);
            out.push_str(",\"navigation_status\":");
            push_u64_str(out, nav);
        }
        5 => {
            let mmsi        = pick_u64(pv, 8, 30);
            let call_sign   = pick_string(pv, 70, 42);
            let name        = pick_string(pv, 112, 120);
            let ship_type   = pick_u64(pv, 232, 8);
            let imo         = pick_u64(pv, 40, 30);
            let destination = pick_string(pv, 302, 120);
            let month       = pick_u64(pv, 274, 4);
            let day         = pick_u64(pv, 278, 5);
            let hour        = pick_u64(pv, 278, 5);
            let minute      = pick_u64(pv, 288, 6);
            let datestub    = minute * 60 + hour * 3600 + day * 86400 + month * 2678400;
            let year: f64   = if !sat_time.is_empty() {
                sat_time.parse::<f64>().unwrap_or(0.0) / 31_536_000.0
            } else { 0.0 };
            let eta     = ((year * 31_536_000.0) as u64) + datestub;
            let draught = pick_u64(pv, 294, 8);

            push_u64_str(out, mmsi);
            out.push_str(",\"latitude\":0.0,\"longitude\":0.0,\"call_sign\":");
            push_escaped_str(out, &call_sign);
            out.push_str(",\"destination\":");
            push_escaped_str(out, &destination);
            out.push_str(",\"name\":");
            push_escaped_str(out, &name);
            out.push_str(",\"ship_type\":");
            push_u64_str(out, ship_type);
            out.push_str(",\"eta\":");
            push_u64_str(out, eta);
            out.push_str(",\"draught\":");
            push_u64_str(out, draught);
            out.push_str(",\"imo\":");
            push_u64_str(out, imo);
            out.push_str(",\"course_over_ground\":\"\",\"position_accuracy\":\"\",\"speed_over_ground\":\"\",\"navigation_status\":\"\"");
        }
        18 => {
            let mmsi    = pick_u64(pv, 8, 30);
            let lon     = pick_i64(pv, 57, 28) as f64 / 600_000.0;
            let lat     = pick_i64(pv, 85, 27) as f64 / 600_000.0;
            let pos_acc = pick_u64(pv, 56, 1);
            let cog     = pick_u64(pv, 112, 12);
            let sog     = pick_u64(pv, 46, 10);

            push_u64_str(out, mmsi);
            out.push_str(",\"latitude\":");
            push_f64(out, lat);
            out.push_str(",\"longitude\":");
            push_f64(out, lon);
            out.push_str(",\"call_sign\":\"\",\"destination\":\"\",\"name\":\"\",\"ship_type\":\"\",\"eta\":\"\",\"draught\":\"\",\"imo\":\"\",\"course_over_ground\":");
            push_u64_str(out, cog);
            out.push_str(",\"position_accuracy\":");
            push_u64_str(out, pos_acc);
            out.push_str(",\"speed_over_ground\":");
            push_u64_str(out, sog);
            out.push_str(",\"navigation_status\":\"\"");
        }
        19 => {
            let mmsi     = pick_u64(pv, 8, 30);
            let sog      = pick_u64(pv, 46, 10);
            let cog      = pick_u64(pv, 112, 12);
            let pos_acc  = pick_u64(pv, 56, 1);
            let name     = pick_string(pv, 143, 120);
            let ship_type = pick_u64(pv, 263, 8);

            push_u64_str(out, mmsi);
            out.push_str(",\"latitude\":0.0,\"longitude\":0.0,\"call_sign\":\"\",\"destination\":\"\",\"name\":");
            push_escaped_str(out, &name);
            out.push_str(",\"ship_type\":");
            push_u64_str(out, ship_type);
            out.push_str(",\"eta\":\"\",\"draught\":\"\",\"imo\":\"\",\"course_over_ground\":");
            push_u64_str(out, cog);
            out.push_str(",\"position_accuracy\":");
            push_u64_str(out, pos_acc);
            out.push_str(",\"speed_over_ground\":");
            push_u64_str(out, sog);
            out.push_str(",\"navigation_status\":\"\"");
        }
        _ => {
            // Unknown type — all remaining fields are defaults
            out.push_str("\"\"");
            out.push_str(",\"latitude\":0.0,\"longitude\":0.0");
            out.push_str(",\"call_sign\":\"\",\"destination\":\"\",\"name\":\"\",\"ship_type\":\"\",\"eta\":\"\",\"draught\":\"\",\"imo\":\"\"");
            out.push_str(",\"course_over_ground\":\"\",\"position_accuracy\":\"\",\"speed_over_ground\":\"\",\"navigation_status\":\"\"");
        }
    }
    out.push('}');
}

// ─── JSON serialisation for assembled multiline PositionReport ───────────────

fn append_report_json(line: &PositionReport, out: &mut String) {
    out.push_str("{\"landfall_time\":");
    push_safe_str(out, &line.landfall_time);
    out.push_str(",\"group\":");
    push_safe_str(out, &line.group);
    out.push_str(",\"satellite_acquisition_time\":");
    push_safe_str(out, &line.satellite_acquisition_time);
    out.push_str(",\"source\":");
    push_safe_str(out, &line.source);
    out.push_str(",\"channel\":");
    push_safe_str(out, &line.channel);
    out.push_str(",\"raw_payload\":");
    push_safe_str(out, &line.raw_payload);
    out.push_str(",\"message_type\":");
    push_u64(out, line.message_type);
    out.push_str(",\"message_class\":");
    push_safe_str(out, &line.message_class);
    out.push_str(",\"mmsi\":");
    push_safe_str(out, &line.mmsi);
    out.push_str(",\"latitude\":");
    push_f64(out, line.latitude);
    out.push_str(",\"longitude\":");
    push_f64(out, line.longitude);
    out.push_str(",\"call_sign\":");
    push_escaped_str(out, &line.call_sign);
    out.push_str(",\"destination\":");
    push_escaped_str(out, &line.destination);
    out.push_str(",\"name\":");
    push_escaped_str(out, &line.name);
    out.push_str(",\"ship_type\":");
    push_safe_str(out, &line.ship_type);
    out.push_str(",\"eta\":");
    push_safe_str(out, &line.eta);
    out.push_str(",\"draught\":");
    push_safe_str(out, &line.draught);
    out.push_str(",\"imo\":");
    push_safe_str(out, &line.imo);
    out.push_str(",\"course_over_ground\":");
    push_safe_str(out, &line.course_over_ground);
    out.push_str(",\"position_accuracy\":");
    push_safe_str(out, &line.position_accuracy);
    out.push_str(",\"speed_over_ground\":");
    push_safe_str(out, &line.speed_over_ground);
    out.push_str(",\"navigation_status\":");
    push_safe_str(out, &line.navigation_status);
    out.push('}');
}

// ─── Decode AIS payload into a PositionReport (for multiline assembled msgs) ─

fn decode_payload(mut line: PositionReport) -> PositionReport {
    let payload = Payload::from_str(&line.raw_payload);
    let pv = payload.as_slice();
    line.message_type = pick_u64(pv, 0, 6);

    match line.message_type {
        1 | 2 | 3 => {
            line.mmsi              = pick_u64(pv, 8, 30).to_string();
            line.latitude          = pick_i64(pv, 89, 27) as f64 / 600_000.0;
            line.longitude         = pick_i64(pv, 61, 28) as f64 / 600_000.0;
            line.position_accuracy = pick_u64(pv, 60, 1).to_string();
            line.speed_over_ground = pick_u64(pv, 50, 10).to_string();
            line.course_over_ground= pick_u64(pv, 116, 27).to_string();
            line.navigation_status = pick_u64(pv, 38, 4).to_string();
        }
        5 => {
            line.mmsi        = pick_u64(pv, 8, 30).to_string();
            line.call_sign   = pick_string(pv, 70, 42);
            line.name        = pick_string(pv, 112, 120);
            line.ship_type   = pick_u64(pv, 232, 8).to_string();
            line.imo         = pick_u64(pv, 40, 30).to_string();
            line.destination = pick_string(pv, 302, 120);
            let month   = pick_u64(pv, 274, 4);
            let day     = pick_u64(pv, 278, 5);
            let hour    = pick_u64(pv, 278, 5);
            let minute  = pick_u64(pv, 288, 6);
            let datestub = minute * 60 + hour * 3600 + day * 86400 + month * 2678400;
            let year: f64 = if !line.satellite_acquisition_time.is_empty() {
                line.satellite_acquisition_time.parse::<f64>().unwrap_or(0.0) / 31_536_000.0
            } else { 0.0 };
            line.eta     = (((year * 31_536_000.0) as u64) + datestub).to_string();
            line.draught = pick_u64(pv, 294, 8).to_string();
        }
        18 => {
            line.mmsi              = pick_u64(pv, 8, 30).to_string();
            line.longitude         = pick_i64(pv, 57, 28) as f64 / 600_000.0;
            line.latitude          = pick_i64(pv, 85, 27) as f64 / 600_000.0;
            line.position_accuracy = pick_u64(pv, 56, 1).to_string();
            line.course_over_ground= pick_u64(pv, 112, 12).to_string();
            line.speed_over_ground = pick_u64(pv, 46, 10).to_string();
        }
        19 => {
            line.mmsi              = pick_u64(pv, 8, 30).to_string();
            line.speed_over_ground = pick_u64(pv, 46, 10).to_string();
            line.course_over_ground= pick_u64(pv, 112, 12).to_string();
            line.position_accuracy = pick_u64(pv, 56, 1).to_string();
            line.name              = pick_string(pv, 143, 120);
            line.ship_type         = pick_u64(pv, 263, 8).to_string();
        }
        _ => {}
    }
    line
}

// ─── Per-Rayon-thread extraction state ───────────────────────────────────────

struct ExtractionState {
    out_tx: Sender<String>,
    ml_tx:  Sender<PositionReport>,
    batch:  String,
    count:  usize,
}

impl Clone for ExtractionState {
    fn clone(&self) -> Self {
        ExtractionState {
            out_tx: self.out_tx.clone(),
            ml_tx:  self.ml_tx.clone(),
            batch:  String::with_capacity(BATCH_SIZE * 350),
            count:  0,
        }
    }
}

impl ExtractionState {
    fn new(out_tx: Sender<String>, ml_tx: Sender<PositionReport>) -> Self {
        ExtractionState { out_tx, ml_tx, batch: String::with_capacity(BATCH_SIZE * 350), count: 0 }
    }

    #[inline]
    fn process(&mut self, line_bytes: &[u8]) {
        // Strip \r
        let line_bytes = if line_bytes.last() == Some(&b'\r') {
            &line_bytes[..line_bytes.len() - 1]
        } else {
            line_bytes
        };

        // Fast byte-level VDM check (no String allocation)
        if !line_bytes.windows(3).any(|w| w == b"VDM") {
            return;
        }

        // SAFETY: NMEA/AIS data is ASCII
        let sentence = unsafe { std::str::from_utf8_unchecked(line_bytes) };
        let group = extract_digits_dashes_after(sentence, "g:");

        if group.is_empty() {
            // Fast path: single-line, zero intermediate allocations for types 1/2/3/18
            append_single_line_json(sentence, &mut self.batch);
            self.batch.push('\n');
            self.count += 1;
            if self.count >= BATCH_SIZE {
                self.flush();
            }
        } else {
            // Build a minimal PositionReport for multiline caching
            let mut parts = sentence.rsplitn(4, ',');
            parts.next();
            let raw_payload = parts.next().unwrap_or("").to_string();
            let channel     = parts.next().unwrap_or("").to_string();
            let partial = PositionReport {
                landfall_time: extract_leading_digits(sentence).to_string(),
                group: group.to_string(),
                satellite_acquisition_time: extract_digits_after(sentence, "c:").to_string(),
                source: extract_word_after(sentence, "s:").to_string(),
                channel,
                raw_payload,
                message_class: "multiline".to_string(),
                ..Default::default()
            };
            self.ml_tx.send(partial).unwrap();
        }
    }

    fn flush(&mut self) {
        if !self.batch.is_empty() {
            self.out_tx.send(std::mem::take(&mut self.batch)).unwrap();
            self.batch = String::with_capacity(BATCH_SIZE * 350);
            self.count = 0;
        }
    }
}

/// Flush remaining batch on thread exit
impl Drop for ExtractionState {
    fn drop(&mut self) { self.flush(); }
}

// ─── main ────────────────────────────────────────────────────────────────────

fn main() {
    let n_workers = num_cpus::get();
    let _ = n_workers; // rayon auto-detects thread count

    let matches = App::new("AIS parsing program")
        .version("1.0")
        .author("Scott Syms <ezrapound1967@gmail.com>")
        .about("Does selective parsing of a raw AIS stream")
        .arg(Arg::new("INPUT").help("Input file").required(true).index(1))
        .arg(Arg::new("OUTPUT").help("Output file").required(true).takes_value(true).index(2))
        .arg(Arg::new("FLOW_LIMIT").help("Max objects in memory (default: 500000)").takes_value(true).index(3))
        .arg(Arg::new("PARSE_THREADS").help("Parse thread count (default: CPUs)").takes_value(true).index(4))
        .arg(Arg::new("MULTILINE_THREADS").help("(unused)").takes_value(true).index(5))
        .get_matches();

    let input_file  = matches.value_of("INPUT").unwrap_or("").to_string();
    let output_file = matches.value_of("OUTPUT").unwrap_or("").to_string();
    let flow_limit: usize = matches.value_of("FLOW_LIMIT")
        .and_then(|v| v.parse().ok()).unwrap_or(500_000);

    if let Some(t) = matches.value_of("PARSE_THREADS").and_then(|v| v.parse::<usize>().ok()) {
        rayon::ThreadPoolBuilder::new().num_threads(t).build_global().ok();
    }

    let batch_limit = flow_limit / BATCH_SIZE + 256;

    // multiline channel: partial PositionReports
    let (ml_tx, ml_rx) = bounded::<PositionReport>(flow_limit);
    // output channel: pre-concatenated batches of JSON lines
    let (out_tx, out_rx) = bounded::<String>(batch_limit);

    // ── Multiline assembly thread ─────────────────────────────────────────────
    let ml_out_tx = out_tx.clone();
    let ml_thread = thread::spawn(move || {
        let mut payload_cache:  HashMap<String, String> = HashMap::new();
        let mut source_cache:   HashMap<String, String> = HashMap::new();
        let mut sat_time_cache: HashMap<String, String> = HashMap::new();
        let mut batch = String::with_capacity(BATCH_SIZE * 350);

        // Iterates until all ml_tx senders are dropped (channel closed)
        for mut line in ml_rx {
            payload_cache.insert(line.group.clone(), line.raw_payload.clone());
            if !line.satellite_acquisition_time.is_empty() {
                sat_time_cache.insert(line.group.clone(), line.satellite_acquisition_time);
            }
            if !line.source.is_empty() {
                source_cache.insert(line.group.clone(), line.source);
            }

            let part1 = format!("1-2-{}", last_four_characters(&line.group));
            let part2 = format!("2-2-{}", last_four_characters(&line.group));

            if payload_cache.contains_key(&part1) && payload_cache.contains_key(&part2) {
                line.raw_payload = format!(
                    "{}{}",
                    payload_cache.remove(&part1).unwrap(),
                    payload_cache.remove(&part2).unwrap()
                );
                line.satellite_acquisition_time =
                    sat_time_cache.remove(&part1).unwrap_or_default();
                line.source = source_cache.remove(&part1).unwrap_or_default();

                line = decode_payload(line);
                append_report_json(&line, &mut batch);
                batch.push('\n');
                if batch.len() >= BATCH_SIZE * 350 {
                    ml_out_tx.send(std::mem::take(&mut batch)).unwrap();
                    batch = String::with_capacity(BATCH_SIZE * 350);
                }
            }
        }
        if !batch.is_empty() {
            ml_out_tx.send(batch).unwrap();
        }
        println!("Multiline assembly done");
        // ml_out_tx dropped here → one less out_tx clone
    });

    // ── Writer thread (64 MB buffer, processes pre-concatenated batches) ──────
    let writer = thread::spawn(move || {
        let file = File::create(output_file).unwrap();
        let mut buf = BufWriter::with_capacity(WRITER_BUF_SIZE, file);
        let mut total: u64 = 0;

        // Iterates until all out_tx senders are dropped
        for batch in out_rx {
            let lines = batch.bytes().filter(|&b| b == b'\n').count() as u64;
            total += lines;
            if total % 1_000_000 < lines {
                eprintln!("Written {}M lines", total / 1_000_000);
            }
            buf.write_all(batch.as_bytes()).unwrap();
        }
        buf.flush().unwrap();
        eprintln!("Writer done: {} total lines", total);
    });

    // ── Memory-map the input and process all lines in parallel with Rayon ─────
    let file = File::open(&input_file).expect("file not found");
    let mmap = unsafe { Mmap::map(&file).expect("mmap failed") };
    let data: &[u8] = &mmap;

    data.par_split(|&b| b == b'\n')
        .for_each_with(
            ExtractionState::new(out_tx.clone(), ml_tx.clone()),
            |state, line_bytes| state.process(line_bytes),
        );

    // Signal channels: drop our sender copies so threads know we're done
    drop(ml_tx);  // → ml_rx channel closes → ml_thread exits → ml_out_tx drops
    drop(out_tx); // → combined with ExtractionState/ml drops → out_rx closes → writer exits

    ml_thread.join().unwrap();
    writer.join().unwrap();
}
