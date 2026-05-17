use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

const TS_PACKET_SIZE: usize = 188;
const DATA_SLOTS_PER_TSMF: usize = 52;

const PID_EXTENDED_TSMF_HEADER: u16 = 0x002f;
const PID_SPLIT_TLV: u16 = 0x002d;

// あなたの環境では RSN 1 が本命だった
const TARGET_RSN: u8 = 1;

// 256QAMなら基本4
const EXPECTED_NUMBER_OF_FRAMES: usize = 4;

// fp=0候補をどれだけ集めるか。
// 1000 superframe ≒ 6秒くらいだったなら、1800は約10秒強相当。
const FP0_CANDIDATES_TO_SCAN: usize = 1800;

// 出力を途中で止めたいなら Some(n)
const LIMIT_OUTPUT_SUPERFRAMES: Option<u64> = None;

const BASE_INDEX: usize = 12;
const MAX_SHIFT: isize = 16;
const FAST_TEST_SUPERFRAMES: usize = 8;
const PAYLOAD_START: usize = 3; // 映像が出ていた方にする。4で出たなら4。

fn main() {
    let mut paths: Vec<PathBuf> = std::fs::read_dir("./in")
        .unwrap()
        .map(|p| p.unwrap().path())
        .collect();

    paths.sort();

    if paths.len() < 3 {
        panic!("最低3波分のTSファイルが必要です。./in/ に3ファイル置いてください");
    }

    eprintln!("input files:");
    for p in &paths {
        eprintln!("  {:?}", p);
    }

    eprintln!("scanning fp=0 candidates...");

    let mut candidate_lists: Vec<Vec<SyncCandidate>> = paths
        .iter()
        .map(|path| scan_fp0_candidates(path, FP0_CANDIDATES_TO_SCAN))
        .collect();

    for list in &candidate_lists {
        if list.is_empty() {
            panic!("fp=0 candidate が見つからないファイルがあります");
        }
    }

    // carrier_sequence順に並べる
    candidate_lists.sort_by_key(|list| list[0].carrier_sequence);

    eprintln!("candidate summary:");
    for list in &candidate_lists {
        let first = &list[0];
        eprintln!(
            "  {:?}: carrier={} candidates={} first_offset={} number_of_carriers={} number_of_frames={}",
            first.path,
            first.carrier_sequence,
            list.len(),
            first.offset,
            first.number_of_carriers,
            first.number_of_frames
        );
    }

    let best = find_best_sync(&candidate_lists);

    eprintln!("best sync:");
    eprintln!("  score={}", best.score);
    eprintln!("  payload_start={}", best.payload_start);
    eprintln!("  stats={:?}", best.stats);
    for (i, idx) in best.indices.iter().enumerate() {
        let c = &candidate_lists[i][*idx];
        eprintln!(
            "  stream{} carrier={} candidate_index={} offset={}",
            i, c.carrier_sequence, idx, c.offset
        );
    }

    eprintln!("writing output from best sync...");

    let mut streams: Vec<StreamState> = best
        .indices
        .iter()
        .enumerate()
        .map(|(stream_idx, candidate_idx)| {
            let c = &candidate_lists[stream_idx][*candidate_idx];
            open_stream_at_candidate(c)
        })
        .collect();

    streams.sort_by_key(|s| s.carrier_sequence);

    let mut out = TlvReassembler::new(best.payload_start, Some(File::create("out_rsn_01.mmts").unwrap()));

    let mut superframe_count = 0u64;

    loop {
        let ok = process_one_superframe(&mut streams, &mut out, TARGET_RSN);

        if !ok {
            break;
        }

        superframe_count += 1;

        if superframe_count % 100 == 0 {
            eprintln!(
                "output superframes={} stats={:?}",
                superframe_count,
                out.stats()
            );
        }

        if let Some(limit) = LIMIT_OUTPUT_SUPERFRAMES {
            if superframe_count >= limit {
                break;
            }
        }
    }

    out.finish();

    eprintln!("done");
    eprintln!("output superframes={}", superframe_count);
    eprintln!("output file: out_rsn_01.mmts");
}

#[derive(Debug, Clone)]
struct SyncCandidate {
    path: PathBuf,
    offset: u64,
    carrier_sequence: u8,
    number_of_carriers: u8,
    number_of_frames: u8,
}

#[derive(Debug, Clone)]
struct TrialResult {
    score: i128,
    payload_start: usize,
    indices: Vec<usize>,
    stats: TlvStats,
}

fn find_best_sync(candidate_lists: &[Vec<SyncCandidate>]) -> TrialResult {
    if candidate_lists.len() != 3 {
        panic!("この版は3波前提です");
    }

    for (i, list) in candidate_lists.iter().enumerate() {
        // if list.len() <= BASE_INDEX {
        //     panic!(
        //         "stream {} の候補数が足りません: candidates={} BASE_INDEX={}",
        //         i,
        //         list.len(),
        //         BASE_INDEX
        //     );
        // }
    }

    let mut results = Vec::new();

    eprintln!(
        "fast sync search: base_index={} carrier1 fixed, carrier2/3 shift={}..{} payload_start={}",
        BASE_INDEX,
        -MAX_SHIFT,
        MAX_SHIFT,
        PAYLOAD_START
    );

    for s2 in -MAX_SHIFT..=MAX_SHIFT {
        for s3 in -MAX_SHIFT..=MAX_SHIFT {
            let i0 = BASE_INDEX as isize;
            let i1 = BASE_INDEX as isize + s2;
            let i2 = BASE_INDEX as isize + s3;

            if i1 < 0 || i2 < 0 {
                continue;
            }

            let indices = vec![i0 as usize, i1 as usize, i2 as usize];

            if indices[1] >= candidate_lists[1].len()
                || indices[2] >= candidate_lists[2].len()
            {
                continue;
            }

            if let Some(result) = run_trial(
                candidate_lists,
                &indices,
                PAYLOAD_START,
                FAST_TEST_SUPERFRAMES,
            ) {
                results.push(result);
            }
        }
    }

    if results.is_empty() {
        panic!("有効な同期候補が見つかりませんでした");
    }

    results.sort_by_key(|r| r.score);

    eprintln!("fast search top:");
    for r in results.iter().take(20) {
        eprintln!(
            "  score={} indices={:?} stats={:?}",
            r.score, r.indices, r.stats
        );
    }

    results[0].clone()
}
fn run_trial(
    candidate_lists: &[Vec<SyncCandidate>],
    indices: &[usize],
    payload_start: usize,
    test_superframes: usize,
) -> Option<TrialResult> {
    let mut streams: Vec<StreamState> = indices
        .iter()
        .enumerate()
        .map(|(stream_idx, candidate_idx)| {
            let c = &candidate_lists[stream_idx][*candidate_idx];
            open_stream_at_candidate(c)
        })
        .collect();

    streams.sort_by_key(|s| s.carrier_sequence);

    let mut reassembler = TlvReassembler::new(payload_start, None);

    let mut ok_superframes = 0usize;

    for _ in 0..test_superframes {
        if !process_one_superframe(&mut streams, &mut reassembler, TARGET_RSN) {
            break;
        }

        ok_superframes += 1;

        let st = reassembler.stats();

        // 明らかなハズレは即打ち切り
        if st.bad > 20 || st.resync > 20 {
            break;
        }
    }

    if ok_superframes == 0 {
        return None;
    }

    let stats = reassembler.stats();
    let score = score_stats(&stats, ok_superframes);

    Some(TrialResult {
        score,
        payload_start,
        indices: indices.to_vec(),
        stats,
    })
}
fn score_stats(stats: &TlvStats, ok_superframes: usize) -> i128 {
    let mut score = 0i128;

    score += stats.bad as i128 * 1_000_000;
    score += stats.resync as i128 * 1_000_000;
    score += stats.remaining_buf as i128 * 10;
    score += stats.max_buf as i128;

    score -= stats.tlv_packets as i128 * 1_000;
    score -= stats.non_null_packets as i128 * 5_000;
    score -= stats.null_packets as i128 * 100;
    score -= ok_superframes as i128 * 100_000;

    if stats.tlv_packets == 0 {
        score += 1_000_000_000;
    }

    score
}

fn scan_fp0_candidates(path: &PathBuf, max_candidates: usize) -> Vec<SyncCandidate> {
    let mut file = File::open(path).unwrap();
    let mut buf = [0u8; TS_PACKET_SIZE];

    let mut offset = 0u64;
    let mut candidates = Vec::new();

    loop {
        if file.read_exact(&mut buf).is_err() {
            break;
        }

        let pid = packet_pid(&buf);

        if pid == PID_EXTENDED_TSMF_HEADER {
            let header = ExtendedTsmfHeader::parse(&buf);

            if header.frame_position == 0 {
                candidates.push(SyncCandidate {
                    path: path.clone(),
                    offset,
                    carrier_sequence: header.carrier_sequence,
                    number_of_carriers: header.number_of_carriers,
                    number_of_frames: header.number_of_frames,
                });

                if candidates.len() >= max_candidates {
                    break;
                }
            }
        }

        offset += TS_PACKET_SIZE as u64;
    }

    eprintln!("{:?}: fp0 candidates={}", path, candidates.len());

    candidates
}

fn open_stream_at_candidate(candidate: &SyncCandidate) -> StreamState {
    let mut file = File::open(&candidate.path).unwrap();
    file.seek(SeekFrom::Start(candidate.offset)).unwrap();

    StreamState {
        path: candidate.path.clone(),
        file,
        offset: candidate.offset,
        carrier_sequence: candidate.carrier_sequence,
        number_of_carriers: candidate.number_of_carriers,
        number_of_frames: candidate.number_of_frames,
    }
}

fn process_one_superframe(
    streams: &mut [StreamState],
    reassembler: &mut TlvReassembler,
    target_rsn: u8,
) -> bool {
    let n_carriers = streams.len();
    let n_frames = streams[0].number_of_frames as usize;

    if n_frames != EXPECTED_NUMBER_OF_FRAMES {
        eprintln!("unexpected number_of_frames={}", n_frames);
        return false;
    }

    let mut superframe: Vec<Vec<TsmfFrame>> = (0..n_frames)
        .map(|_| Vec::with_capacity(n_carriers))
        .collect();

    for stream in streams.iter_mut() {
        for _ in 0..n_frames {
            let frame = match read_one_tsmf_frame(stream) {
                Some(f) => f,
                None => return false,
            };

            let fp = frame.header.frame_position as usize;

            if fp >= n_frames {
                eprintln!(
                    "invalid frame_position={} carrier={}",
                    frame.header.frame_position,
                    frame.header.carrier_sequence
                );
                return false;
            }

            superframe[fp].push(frame);
        }
    }

    for fp in 0..n_frames {
        superframe[fp].sort_by_key(|f| f.header.carrier_sequence);

        if superframe[fp].len() != n_carriers {
            eprintln!(
                "missing carriers at fp={} got={} expected={}",
                fp,
                superframe[fp].len(),
                n_carriers
            );
            return false;
        }
    }

    let mut ordered_slots = Vec::new();

    for fp in 0..n_frames {
        for carrier_idx in 0..n_carriers {
            let frame = &superframe[fp][carrier_idx];

            for data_slot_idx in 0..DATA_SLOTS_PER_TSMF {
                let rsn = frame.header.relative_stream_number[data_slot_idx];

                if rsn != target_rsn {
                    continue;
                }

                let rsn_idx = (rsn - 1) as usize;

                if rsn_idx >= frame.header.stream_type.len() {
                    continue;
                }

                if frame.header.stream_type[rsn_idx] != StreamKind::Tlv {
                    continue;
                }

                /*
                 * 仕様の合成順:
                 *   subframe
                 *     slot_position
                 *       carrier_sequence
                 *
                 * 拡張TSMFは 53 slots。
                 * slot 0 はヘッダ。
                 * data_slot_idx 0 は slot 1 に相当。
                 */
                let slot_index_in_tsmf = data_slot_idx + 1;
                let full_slot_index = fp * 53 + slot_index_in_tsmf;

                let subframe = full_slot_index / n_frames;
                let slot_position = full_slot_index % n_frames;

                ordered_slots.push(OrderedSlot {
                    subframe,
                    slot_position,
                    carrier_sequence: frame.header.carrier_sequence,
                    packet: frame.slots[data_slot_idx],
                });
            }
        }
    }

    ordered_slots.sort_by_key(|s| {
        (
            s.subframe,
            s.slot_position,
            s.carrier_sequence,
        )
    });

    for slot in ordered_slots {
        reassembler.feed_split_tlv_packet(&slot.packet);
    }

    true
}

fn read_one_tsmf_frame(stream: &mut StreamState) -> Option<TsmfFrame> {
    let mut buf = [0u8; TS_PACKET_SIZE];

    let header = loop {
        let packet_offset = stream.offset;

        if stream.file.read_exact(&mut buf).is_err() {
            return None;
        }

        stream.offset += TS_PACKET_SIZE as u64;

        let pid = packet_pid(&buf);

        if pid == PID_EXTENDED_TSMF_HEADER {
            let h = ExtendedTsmfHeader::parse(&buf);

            if h.carrier_sequence != stream.carrier_sequence {
                eprintln!(
                    "carrier_sequence mismatch: stream={} header={} offset={}",
                    stream.carrier_sequence,
                    h.carrier_sequence,
                    packet_offset
                );
            }

            break h;
        }
    };

    let mut slots: Vec<[u8; 188]> = Vec::with_capacity(DATA_SLOTS_PER_TSMF);

    while slots.len() < DATA_SLOTS_PER_TSMF {
        let packet_offset = stream.offset;

        if stream.file.read_exact(&mut buf).is_err() {
            return None;
        }

        stream.offset += TS_PACKET_SIZE as u64;

        let pid = packet_pid(&buf);

        match pid {
            PID_SPLIT_TLV => {
                slots.push(buf);
            }

            PID_EXTENDED_TSMF_HEADER => {
                eprintln!(
                    "next 0x2F came before 52 slots: carrier={} frame_position={} slots={} offset={}",
                    stream.carrier_sequence,
                    header.frame_position,
                    slots.len(),
                    packet_offset
                );

                stream.file.seek(SeekFrom::Start(packet_offset)).unwrap();
                stream.offset = packet_offset;

                return None;
            }

            _ => {}
        }
    }

    Some(TsmfFrame { header, slots })
}

fn packet_pid(data: &[u8; 188]) -> u16 {
    if data[0] != 0x47 {
        eprintln!(
            "sync byte mismatch: {:02X} {:02X} {:02X} {:02X}",
            data[0], data[1], data[2], data[3]
        );
    }

    ((data[1] as u16 & 0x1f) << 8) | data[2] as u16
}

fn packet_start_indicator(data: &[u8; 188]) -> bool {
    (data[1] & 0x40) != 0
}

#[derive(Debug)]
struct StreamState {
    path: PathBuf,
    file: File,
    offset: u64,
    carrier_sequence: u8,
    number_of_carriers: u8,
    number_of_frames: u8,
}

#[derive(Debug)]
struct TsmfFrame {
    header: ExtendedTsmfHeader,
    slots: Vec<[u8; 188]>,
}

#[derive(Clone, Copy)]
struct OrderedSlot {
    subframe: usize,
    slot_position: usize,
    carrier_sequence: u8,
    packet: [u8; 188],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamKind {
    Tlv,
    TsOrNone,
}

#[derive(Debug, Clone)]
struct ExtendedTsmfHeader {
    frame_pid: u16,
    continuity_counter: u8,

    frame_sync: u16,
    version_number: u8,
    relative_stream_number_mode: bool,
    frame_type: u8,

    stream_status: [bool; 15],
    stream_id: [u16; 15],
    original_network_id: [u16; 15],
    receive_status: [u8; 15],
    emergency_indicator: bool,

    relative_stream_number: [u8; 52],
    stream_type: [StreamKind; 15],

    group_id: u8,
    number_of_carriers: u8,
    carrier_sequence: u8,
    number_of_frames: u8,
    frame_position: u8,

    crc: u32,
}

impl ExtendedTsmfHeader {
    fn parse(data: &[u8; 188]) -> Self {
        let mut r = BitReader::new(data);

        let sync_byte = r.read_u8(8);
        assert_eq!(sync_byte, 0x47, "Extended TSMF sync_byte != 0x47");

        let fixed_000 = r.read_u8(3);
        if fixed_000 != 0 {
            eprintln!("warning: TSMF header fixed '000' is {}", fixed_000);
        }

        let frame_pid = r.read_u16(13);

        let fixed_0001 = r.read_u8(4);
        if fixed_0001 != 0b0001 {
            eprintln!("warning: TSMF header fixed '0001' is {:04b}", fixed_0001);
        }

        let continuity_counter = r.read_u8(4);

        let _reserved_3 = r.read_u8(3);

        let frame_sync = r.read_u16(13);
        let version_number = r.read_u8(3);
        let relative_stream_number_mode = r.read_bool();
        let frame_type = r.read_u8(4);

        let mut stream_status = [false; 15];
        for i in 0..15 {
            stream_status[i] = r.read_bool();
        }

        let _reserved_1 = r.read_bool();

        let mut stream_id = [0u16; 15];
        let mut original_network_id = [0u16; 15];

        for i in 0..15 {
            stream_id[i] = r.read_u16(16);
            original_network_id[i] = r.read_u16(16);
        }

        let mut receive_status = [0u8; 15];

        for i in 0..15 {
            receive_status[i] = r.read_u8(2);
        }

        let _reserved_1b = r.read_bool();
        let emergency_indicator = r.read_bool();

        let mut relative_stream_number = [0u8; 52];

        for i in 0..52 {
            relative_stream_number[i] = r.read_u8(4);
        }

        let _earthquake_early_warning = r.read_bits_to_vec(204);

        let fixed_0000 = r.read_u8(4);
        if fixed_0000 != 0 {
            eprintln!("warning: TSMF fixed '0000' is {:04b}", fixed_0000);
        }

        let mut stream_type = [StreamKind::TsOrNone; 15];

        for i in 0..15 {
            let bit = r.read_bool();

            stream_type[i] = if bit {
                StreamKind::TsOrNone
            } else {
                StreamKind::Tlv
            };
        }

        let fixed_0 = r.read_bool();
        if fixed_0 {
            eprintln!("warning: TSMF fixed '0' is 1");
        }

        let group_id = r.read_u8(8);
        let number_of_carriers = r.read_u8(8);
        let carrier_sequence = r.read_u8(8);
        let number_of_frames = r.read_u8(4);
        let frame_position = r.read_u8(4);

        let _reserved_424 = r.read_bits_to_vec(424);

        let crc = r.read_u32(32);

        if frame_pid != PID_EXTENDED_TSMF_HEADER {
            eprintln!("warning: frame_pid != 0x2F: 0x{:04X}", frame_pid);
        }

        Self {
            frame_pid,
            continuity_counter,

            frame_sync,
            version_number,
            relative_stream_number_mode,
            frame_type,

            stream_status,
            stream_id,
            original_network_id,
            receive_status,
            emergency_indicator,

            relative_stream_number,
            stream_type,

            group_id,
            number_of_carriers,
            carrier_sequence,
            number_of_frames,
            frame_position,

            crc,
        }
    }
}

struct BitReader<'a> {
    data: &'a [u8],
    bit_pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, bit_pos: 0 }
    }

    fn read_bool(&mut self) -> bool {
        self.read_u8(1) != 0
    }

    fn read_u8(&mut self, bits: usize) -> u8 {
        assert!(bits <= 8);

        let mut v = 0u8;

        for _ in 0..bits {
            v <<= 1;
            v |= self.read_bit();
        }

        v
    }

    fn read_u16(&mut self, bits: usize) -> u16 {
        assert!(bits <= 16);

        let mut v = 0u16;

        for _ in 0..bits {
            v <<= 1;
            v |= self.read_bit() as u16;
        }

        v
    }

    fn read_u32(&mut self, bits: usize) -> u32 {
        assert!(bits <= 32);

        let mut v = 0u32;

        for _ in 0..bits {
            v <<= 1;
            v |= self.read_bit() as u32;
        }

        v
    }

    fn read_bits_to_vec(&mut self, bits: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity((bits + 7) / 8);

        let mut cur = 0u8;
        let mut n = 0usize;

        for _ in 0..bits {
            cur <<= 1;
            cur |= self.read_bit();
            n += 1;

            if n == 8 {
                out.push(cur);
                cur = 0;
                n = 0;
            }
        }

        if n != 0 {
            cur <<= 8 - n;
            out.push(cur);
        }

        out
    }

    fn read_bit(&mut self) -> u8 {
        let byte_pos = self.bit_pos / 8;
        let bit_in_byte = 7 - (self.bit_pos % 8);

        if byte_pos >= self.data.len() {
            panic!("BitReader overrun at bit_pos={}", self.bit_pos);
        }

        let bit = (self.data[byte_pos] >> bit_in_byte) & 1;
        self.bit_pos += 1;

        bit
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct TlvStats {
    split_packets: u64,
    starts: u64,
    tlv_packets: u64,
    null_packets: u64,
    non_null_packets: u64,
    bad: u64,
    resync: u64,
    remaining_buf: usize,
    max_buf: usize,
}

struct TlvReassembler {
    payload_start: usize,
    out: Option<File>,
    aligned: bool,
    buf: Vec<u8>,

    stats: TlvStats,
    type_counts: BTreeMap<u16, u64>,
}

impl TlvReassembler {
    fn new(payload_start: usize, out: Option<File>) -> Self {
        Self {
            payload_start,
            out,
            aligned: false,
            buf: Vec::with_capacity(1024 * 1024),

            stats: TlvStats::default(),
            type_counts: BTreeMap::new(),
        }
    }

    fn feed_split_tlv_packet(&mut self, packet: &[u8; 188]) {
        self.stats.split_packets += 1;

        if packet[0] != 0x47 {
            self.bad_and_resync();
            return;
        }

        if packet_pid(packet) != PID_SPLIT_TLV {
            return;
        }

        if self.payload_start >= 188 {
            self.bad_and_resync();
            return;
        }

        let start = packet_start_indicator(packet);
        let payload = &packet[self.payload_start..188];

        if start {
            self.stats.starts += 1;

            if payload.is_empty() {
                self.bad_and_resync();
                return;
            }

            let pointer = payload[0] as usize;

            if pointer > payload.len().saturating_sub(1) {
                self.bad_and_resync();
                return;
            }

            if !self.aligned {
                let begin = 1 + pointer;

                if begin > payload.len() {
                    self.bad_and_resync();
                    return;
                }

                self.buf.extend_from_slice(&payload[begin..]);
                self.aligned = true;
            } else {
                // pointer byteだけを除去する。
                // pointer前のデータは前TLVの残りなので捨てない。
                self.buf.extend_from_slice(&payload[1..]);
            }
        } else if self.aligned {
            self.buf.extend_from_slice(payload);
        }

        self.stats.max_buf = self.stats.max_buf.max(self.buf.len());

        self.flush_complete_tlv_packets();
    }

    fn flush_complete_tlv_packets(&mut self) {
        loop {
            if self.buf.len() < 4 {
                self.stats.remaining_buf = self.buf.len();
                return;
            }

            let packet_type = read_u16_be(&self.buf[0..2]);
            let length = read_u16_be(&self.buf[2..4]) as usize;
            let total_len = 4 + length;

            /*
             * typeを少しだけ制限する。
             *
             * 0x7FFF = Null TLV
             * 0x0001..0x0004 あたり = IPv4/IPv6/圧縮IP/制御系想定
             *
             * ここが厳しすぎる場合は looks_like_valid_tlv_type を広げる。
             */
            if !looks_like_valid_tlv_type(packet_type) {
                self.bad_and_resync();
                return;
            }

            if total_len > 65539 {
                self.bad_and_resync();
                return;
            }

            if self.buf.len() < total_len {
                self.stats.remaining_buf = self.buf.len();

                // ランダム長を掴んで巨大に待ち続ける候補を落とす
                if self.buf.len() > 128 * 1024 {
                    self.bad_and_resync();
                }

                return;
            }

            let packet = &self.buf[..total_len];

            if let Some(out) = self.out.as_mut() {
                out.write_all(packet).unwrap();
            }

            self.stats.tlv_packets += 1;

            *self.type_counts.entry(packet_type).or_insert(0) += 1;

            if packet_type == 0x7fff {
                self.stats.null_packets += 1;
            } else {
                self.stats.non_null_packets += 1;
            }

            self.buf.drain(..total_len);

            if self.buf.capacity() > 4 * 1024 * 1024 && self.buf.len() < 1024 {
                self.buf.shrink_to(1024 * 1024);
            }
        }
    }

    fn bad_and_resync(&mut self) {
        self.stats.bad += 1;
        self.stats.resync += 1;
        self.aligned = false;
        self.buf.clear();
        self.stats.remaining_buf = 0;
    }

    fn finish(&mut self) {
        self.flush_complete_tlv_packets();
        self.stats.remaining_buf = self.buf.len();

        eprintln!(
            "tlv reassembler: payload_start={} split_packets={} starts={} tlv_packets={} null_packets={} non_null_packets={} bad={} resync={} remaining_buf={} max_buf={} aligned={}",
            self.payload_start,
            self.stats.split_packets,
            self.stats.starts,
            self.stats.tlv_packets,
            self.stats.null_packets,
            self.stats.non_null_packets,
            self.stats.bad,
            self.stats.resync,
            self.stats.remaining_buf,
            self.stats.max_buf,
            self.aligned
        );

        if self.out.is_some() {
            eprintln!("TLV packet types:");
            for (packet_type, count) in &self.type_counts {
                eprintln!("  type=0x{:04X} count={}", packet_type, count);
            }
        }
    }

    fn stats(&self) -> TlvStats {
        let mut s = self.stats;
        s.remaining_buf = self.buf.len();
        s
    }
}

fn looks_like_valid_tlv_type(packet_type: u16) -> bool {
    packet_type == 0x7fff || packet_type <= 0x7ffe
}

fn read_u16_be(bytes: &[u8]) -> u16 {
    ((bytes[0] as u16) << 8) | bytes[1] as u16
}