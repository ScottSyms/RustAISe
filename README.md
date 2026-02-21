# RustAISe — High-Performance AIS Stream Parser

A fast, multi-threaded parser for raw [AIS (Automatic Identification System)](https://en.wikipedia.org/wiki/Automatic_identification_system) NMEA streams, written in Rust. Processes millions of vessel position and static reports from satellite and terrestrial receivers and emits newline-delimited JSON.

> Original article: [Big Data Engineering with Rust](https://www.statcan.gc.ca/en/data-science/network/engineering-rust)

---

## Features

- Parses AIS message types 1, 2, 3 (Class A kinetic), 5 (Class A static), 18 (Class B kinetic), and 19 (Class B extended)
- Handles multi-part (2-sentence) AIS messages
- Extracts satellite acquisition time, source, channel, and landfall time from ORBCOMM-style metadata
- Outputs one JSON object per decoded message to a file
- Processes ~9.7 million messages in under 5 seconds on a modern multi-core machine

## How it works

```
mmap(input)
    │
    ▼
rayon::par_split('\n')          ← all CPU cores in parallel
    │
    ├─ single-line messages ──► append_single_line_json() ──► batched String ──► writer thread
    │                            (zero heap allocations for                        (64 MB BufWriter)
    │                             types 1/2/3/18)
    │
    └─ multiline fragments ───► assembly thread (HashMap cache)
                                  assembles part1+part2 pairs
                                      │
                                      └──────────────────────────► same writer thread
```

1. The input file is memory-mapped (no `BufReader` overhead)
2. Rayon splits the byte slice on newlines and distributes work across all CPU cores automatically
3. Each worker thread accumulates 2048 JSON lines into a pre-allocated `String` batch before sending to the output channel — minimising channel traffic
4. A single dedicated multiline-assembly thread pairs up 2-sentence AIS messages using local `HashMap`s (no lock contention)
5. A dedicated writer thread drains the output channel into a 64 MB `BufWriter`

## Build

Requires [Rust](https://rustup.rs/) 1.60+.

```bash
cargo build --release
```

## Usage

```
./target/release/rustaise <INPUT> <OUTPUT> [FLOW_LIMIT] [PARSE_THREADS]
```

| Argument | Description | Default |
|----------|-------------|---------|
| `INPUT` | Path to raw NMEA/AIS input file | required |
| `OUTPUT` | Path to write newline-delimited JSON output | required |
| `FLOW_LIMIT` | Max messages buffered in memory at once | 500 000 |
| `PARSE_THREADS` | Number of Rayon worker threads | all CPUs |

### Example

```bash
./target/release/rustaise norway.nmea norway.out
```

## Output format

Each line is a JSON object with the following fields:

```json
{
  "landfall_time": "1643588424",
  "group": "",
  "satellite_acquisition_time": "1643588424",
  "source": "2573135",
  "channel": "B",
  "raw_payload": "13LOE0002KPQiIVQ8k8:oHep0H<L",
  "message_type": 1,
  "message_class": "singleline",
  "mmsi": "231200000",
  "latitude": 57.911946666666665,
  "longitude": 7.377578333333333,
  "call_sign": "",
  "destination": "",
  "name": "",
  "ship_type": "",
  "eta": "",
  "draught": "",
  "imo": "",
  "course_over_ground": "91145660",
  "position_accuracy": "1",
  "speed_over_ground": "155",
  "navigation_status": "0"
}
```

Fields not present in a given message type are empty strings or `0.0`.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `rayon` | Data-parallel processing across all CPU cores |
| `crossbeam-channel` | Bounded MPSC channels between assembly and writer threads |
| `hashbrown` | Fast `HashMap` for multiline assembly cache |
| `memmap2` | Memory-mapped file I/O |
| `ryu` | Fast f64 → string formatting (no heap allocation) |
| `clap` | Command-line argument parsing |

## Performance

Tested on a 9.7-million-line NMEA file (~2 GB output):

| Version | Time | Notes |
|---------|------|-------|
| Original | ~37 s | `bitvec`, `regex`, `serde_json`, single-threaded writer |
| After round 1 | ~23 s | Removed bitvec/regex, fixed mutex contention |
| After round 2 | ~15 s | Manual JSON, batched channel, mmap, dedicated writer |
| **After round 3** | **~4.5 s** | Zero-alloc single-line path, Rayon `par_split` |

