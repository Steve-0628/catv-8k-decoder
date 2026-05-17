use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

const TS_PACKET_SIZE: usize = 188;
const DATA_SLOTS_PER_TSMF: usize = 52;
const MAX_RSN: usize = 16;

// 全部やるなら None
// const LIMIT_SUPER_FRAMES: Option<u64> = Some(1000);
const LIMIT_SUPER_FRAMES: Option<u64> = None;


const PID_EXTENDED_TSMF_HEADER: u16 = 0x002f;
const PID_SPLIT_TLV: u16 = 0x002d;

fn main() {
    let mut paths: Vec<PathBuf> = std::fs::read_dir("./in")
        .unwrap()
        .map(|p| p.unwrap().path())
        .collect();

    paths.sort();

    if paths.is_empty() {
        panic!("./in/ にTSファイルがありません");
    }

    eprintln!("input files:");
    for path in &paths {
        eprintln!("  {:?}", path);
    }

    let mut streams: Vec<StreamState> = paths
        .iter()
        .map(open_and_sync_to_frame0)
        .collect();

    streams.sort_by_key(|s| s.carrier_sequence);

    eprintln!("synced streams:");
    for s in &streams {
        eprintln!(
            "  {:?}: carrier_sequence={} number_of_carriers={} number_of_frames={} offset={}",
            s.path,
            s.carrier_sequence,
            s.number_of_carriers,
            s.number_of_frames,
            s.offset
        );
    }

    let n_carriers = streams.len();
    let n_frames = streams[0].number_of_frames as usize;

    if n_frames != 3 && n_frames != 4 {
        panic!("number_of_frames が仕様外です: {}", n_frames);
    }

    eprintln!("n_carriers={} n_frames={}", n_carriers, n_frames);

    let mut out_rsn_01 = TlvReassembler::new(File::create("out_rsn_01.mmts").unwrap());

    let mut super_frame_count: u64 = 0;

    'outer: loop {
        /*
         * superframe[frame_position][carrier_index]
         *
         * 1搬送波につき number_of_frames 個の拡張TSMFを読む。
         * 256QAMなら 4個。
         * 64QAMなら 3個。
         */
        let mut superframe: Vec<Vec<TsmfFrame>> = (0..n_frames)
            .map(|_| Vec::with_capacity(n_carriers))
            .collect();

        for stream in streams.iter_mut() {
            for _ in 0..n_frames {
                let frame = match read_one_tsmf_frame(stream) {
                    Some(f) => f,
                    None => break 'outer,
                };

                let fp = frame.header.frame_position as usize;

                if fp >= n_frames {
                    eprintln!(
                        "invalid frame_position={} carrier={}",
                        frame.header.frame_position,
                        frame.header.carrier_sequence
                    );
                    break 'outer;
                }

                superframe[fp].push(frame);
            }
        }

        for fp in 0..n_frames {
            superframe[fp].sort_by_key(|f| f.header.carrier_sequence);

            if superframe[fp].len() != n_carriers {
                eprintln!(
                    "frame_position={} has {} carriers, expected {}",
                    fp,
                    superframe[fp].len(),
                    n_carriers
                );
                break 'outer;
            }
        }

        if super_frame_count % 100 == 0 {
            eprintln!("super_frame_count={}", super_frame_count);

            for fp in 0..n_frames {
                eprintln!("  frame_position={}", fp);

                for f in &superframe[fp] {
                    eprintln!(
                        "    carrier={} group_id={} rsn[0..16]={:?} stream_type[0..15]={:?}",
                        f.header.carrier_sequence,
                        f.header.group_id,
                        &f.header.relative_stream_number[0..16],
                        &f.header.stream_type
                    );
                }
            }
        }

        /*
         * 総務省資料 4.7.2 の順序:
         *
         *   subframe
         *     slot_position
         *       carrier_sequence
         *
         * ここでは、各データスロットに対して、
         *
         *   full_slot_index = frame_position * 53 + slot_index
         *
         * とする。
         *
         * slot_index は:
         *   0      = 拡張TSMFヘッダスロット
         *   1..52  = データスロット
         *
         * なので、relative_stream_number[0] に対応するスロットは slot_index=1。
         */
        let mut ordered_slots: Vec<OrderedSlot> = Vec::new();

        for fp in 0..n_frames {
            for carrier_idx in 0..n_carriers {
                let frame = &superframe[fp][carrier_idx];

                for data_slot_idx in 0..DATA_SLOTS_PER_TSMF {
                    let slot_index_in_tsmf = data_slot_idx + 1;
                    let full_slot_index = fp * 53 + slot_index_in_tsmf;

                    let subframe = full_slot_index / n_frames;
                    let slot_position = full_slot_index % n_frames;

                    let rsn = frame.header.relative_stream_number[data_slot_idx];

                    if rsn == 0 {
                        continue;
                    }

                    let rsn_idx = (rsn - 1) as usize;

                    if rsn_idx >= frame.header.stream_type.len() {
                        continue;
                    }

                    let is_tlv = frame.header.stream_type[rsn_idx] == StreamKind::Tlv;

                    if !is_tlv {
                        continue;
                    }

                    ordered_slots.push(OrderedSlot {
                        subframe,
                        slot_position,
                        carrier_sequence: frame.header.carrier_sequence,
                        rsn,
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

        for s in ordered_slots {
            if super_frame_count < 2 {
                eprintln!(
                    "sf={} sp={} carrier={} rsn={} head={:02X} {:02X} {:02X} {:02X}",
                    s.subframe,
                    s.slot_position,
                    s.carrier_sequence,
                    s.rsn,
                    s.packet[0],
                    s.packet[1],
                    s.packet[2],
                    s.packet[3],
                );
            }

            if s.rsn == 1 {
                out_rsn_01.feed_split_tlv_packet(&s.packet);
            }
        }

        super_frame_count += 1;

        if let Some(limit) = LIMIT_SUPER_FRAMES
            && super_frame_count >= limit {
                break;
            }
    }

    out_rsn_01.finish();

    eprintln!("done: super_frame_count={}", super_frame_count);
    eprintln!("generated:");
    eprintln!("  out_tlv_all.mmts");
    eprintln!("  out_rsn_00.mmts ... out_rsn_15.mmts");
}

fn open_and_sync_to_frame0(path: &PathBuf) -> StreamState {
    let mut file = File::open(path).unwrap();
    let mut buf = [0u8; TS_PACKET_SIZE];

    let mut offset: u64 = 0;

    loop {
        file.seek(SeekFrom::Start(offset)).unwrap();

        if file.read_exact(&mut buf).is_err() {
            panic!("frame_position=0 が見つからなかった: {:?}", path);
        }

        let pid = packet_pid(&buf);

        if pid == PID_EXTENDED_TSMF_HEADER {
            let header = ExtendedTsmfHeader::parse(&buf);

            if header.frame_position == 0 {
                eprintln!(
                    "{:?}: synced carrier_sequence={} number_of_carriers={} number_of_frames={} frame_position={} offset={}",
                    path,
                    header.carrier_sequence,
                    header.number_of_carriers,
                    header.number_of_frames,
                    header.frame_position,
                    offset
                );

                file.seek(SeekFrom::Start(offset)).unwrap();

                return StreamState {
                    path: path.clone(),
                    file,
                    offset,
                    carrier_sequence: header.carrier_sequence,
                    number_of_carriers: header.number_of_carriers,
                    number_of_frames: header.number_of_frames,
                };
            }
        }

        offset += TS_PACKET_SIZE as u64;
    }
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

/*
 * 分割TLVパケット → 生TLVストリーム復元
 *
 * 分割TLVパケット構造:
 *
 *   byte 0      : 0x47
 *   byte 1..2   : TEI / TLV packet start indicator / '0' / PID
 *   byte 3..187 : payload 185B
 *
 * TLV packet start indicator が 1 のとき:
 *
 *   payload[0] が「先頭TLV指示」
 *   payload[1..] が実データ
 *
 * 以前の data[4..] 固定は、start indicator=0 のパケットで
 * payload先頭1バイトを毎回捨てるので壊れる。
 */
struct TlvReassembler {
    out: File,
    aligned: bool,
    packet_count: u64,
    start_count: u64,
}

impl TlvReassembler {
    fn new(out: File) -> Self {
        Self {
            out,
            aligned: false,
            packet_count: 0,
            start_count: 0,
        }
    }

    fn feed_split_tlv_packet(&mut self, packet: &[u8; 188]) {
        self.packet_count += 1;

        if packet[0] != 0x47 {
            eprintln!("split TLV sync mismatch: {:02X}", packet[0]);
            return;
        }

        let pid = packet_pid(packet);
        if pid != PID_SPLIT_TLV {
            return;
        }

        let payload = &packet[3..188];
        let start = packet_start_indicator(packet);

        if start {
            self.start_count += 1;

            let pointer = payload[0] as usize;

            if pointer > 184 {
                eprintln!("bad first_tlv_pointer={}", pointer);
                return;
            }

            if !self.aligned {
                /*
                 * 最初だけは、前のTLVパケットの尻尾を捨てる。
                 * pointerの位置から最初の完全なTLVが始まる。
                 */
                let begin = 1 + pointer;
                self.out.write_all(&payload[begin..]).unwrap();
                self.aligned = true;
            } else {
                /*
                 * すでに同期済みなら、pointer前のバイトも前TLVの続きなので捨てない。
                 * pointer byte 自体だけを除いて payload[1..] を全部出す。
                 */
                self.out.write_all(&payload[1..]).unwrap();
            }
        } else {
            if self.aligned {
                self.out.write_all(payload).unwrap();
            }
        }
    }

    fn finish(&mut self) {
        eprintln!(
            "tlv reassembler: packets={} starts={} aligned={}",
            self.packet_count,
            self.start_count,
            self.aligned
        );
    }
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
    rsn: u8,
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