#![feature(path_file_prefix)]
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

fn main() {
    let paths = std::fs::read_dir("./in").unwrap();
    let file_readers: Vec<std::fs::File> = paths
        .map(|p| {
            let path = p.unwrap().path();
            std::fs::File::open(path).unwrap()
        })
        .collect();
    dbg!(&file_readers);
    let mut streams: Vec<TsmfInfo> = file_readers
        .iter()
        .map(|mut reader| {
            let mut offset: u64 = 0;
            // let mut ts_packet_itr = vec![0, 188];
            // reader.read_exact(&mut ts_packet_itr).unwrap();
            // offset += *(ts_packet_itr
            //     .iter()
            //     .find(|byte| **byte == 0x47)
            //     .unwrap_or(&0)) as u64;

            let mut ts_packet = vec![0; 188];
            loop {
                reader.seek(SeekFrom::Start(offset)).unwrap();
                reader.read_exact(&mut ts_packet).unwrap();
                let packet: [u8; 188] = ts_packet[..].try_into().unwrap();
                let packet = TSPacket::from(packet);
                if packet.pid == 0x2f {
                    // TSMF header
                    let tsmf = TSMFHeaderPacket::from(packet.payload);
                    if tsmf.frame_location == 0 {
                        return TsmfInfo {
                            stream_id: tsmf.streams_order,
                            offset_bytes: offset,
                            file: reader,
                        };
                    }
                }
                offset += 188;
            }
        })
        .collect::<Vec<TsmfInfo>>();
    streams.sort_by(|a, b| a.stream_id.cmp(&b.stream_id));
    dbg!(&streams);

    let mut outfile = File::create("out.mmts").unwrap();

    let mut current_stream = 0;
    let mut ts_packet = vec![0; 188];
    while streams[current_stream]
        .file
        .read_exact(&mut ts_packet)
        .is_ok()
    {
        let packet: [u8; 188] = ts_packet[..].try_into().unwrap();
        let packet = TSPacket::from(packet);
        match packet.pid {
            0x2f => {
                // TSMF contorl packet
                let tsmf = TSMFHeaderPacket::from(packet.payload);
                // TODO
                current_stream += 1;
                current_stream /= streams.len();
                continue;
            }
            0x2d => {
                if packet.payload_unit_start_indicator {
                    let realmmt: [u8; 184] = packet.mmt[1..].try_into().unwrap();
                    let _ = outfile.write(&realmmt).unwrap();
                } else {
                    let _ = outfile.write(&packet.mmt).unwrap();
                }
            }
            _ => {
                // TODO: not implmented
            }
        }
        streams[current_stream].offset_bytes += 188;
    }
}

#[derive(Debug)]
struct TsmfInfo<'a> {
    stream_id: u8,
    offset_bytes: u64,
    file: &'a File,
}

struct TSPacket {
    sync_byte: u8,
    transport_error_indicator: bool,
    payload_unit_start_indicator: bool,
    transport_priority: bool,
    pid: u16,                         // 13 bits
    transport_scrambling_control: u8, // 2 bits
    adaptation_field_control: u8,     // 2 bits
    continuity_counter: u8,
    payload: [u8; 184], // TODO: maybe implment adaptation field?
    mmt: [u8; 185],
}

impl From<[u8; 188]> for TSPacket {
    fn from(data: [u8; 188]) -> Self {
        let sync_byte = data[0];
        let transport_error_indicator = ((data[1] & 0b1000_0000) >> 7) == 1;
        let payload_unit_start_indicator = ((data[1] & 0b0100_0000) >> 6) == 1;
        let transport_priority = ((data[1] & 0b0010_0000) >> 5) == 1;
        let pid = ((data[1] as u16 & 0b0001_1111) << 8) | data[2] as u16;
        let transport_scrambling_control = (data[3] & 0b1100_0000) >> 6;
        let adaptation_field_control = (data[3] & 0b0011_0000) >> 4;
        let continuity_counter = data[3] & 0b0000_1111;
        let payload = data[4..].try_into().unwrap();
        let mmt = data[3..].try_into().unwrap();
        Self {
            sync_byte,
            transport_error_indicator,
            payload_unit_start_indicator,
            transport_priority,
            pid,
            transport_scrambling_control,
            adaptation_field_control,
            continuity_counter,
            payload,
            mmt,
        }
    }
}

#[derive(Debug)]
struct TSMFHeaderPacket {
    sync_signal_unused: u8,                   // 3 bits
    sync_signal: u16,                         // 13 bits
    alter_indicator: u8,                      // 3bits;  1: static, 0: undefined
    slot_info_indicator: bool,                // 1 bit
    multi_frame_type: u8,                     // 4 bits;  0x1, 0x2, 0xF, undefined
    relative_stream_enabled: [bool; 15],      // 15 bits
    slot_info_undefined: u8,                  // 1 bit
    relative_stream_info: [u32; 15],          // 32*15=480 bits [TSID, NID]
    relative_stream_reception_info: [u8; 15], // 2*15=30 bits
    transmission_control_undefined: u8,       // 1 bit
    emergency_warning_info: u8,               // 1 bit
    relative_stream_correspondence: [u8; 52], // 4*52=208 bits stream 2 to 53
    // extended_information: [u8; 85],           // 680 bits todo: maybe parse?
    earthquake_warning_information: [u8; 26], // 204 + 4 bits
    stream_type: u16,                         // 7 bits
    // stream_type_unused_0: u8,                  // 1 bit
    streams_identification: u8, // 8 bits
    streams_count: u8,          // 8 bits
    streams_order: u8,          // 8 bits
    frame_count: u8,            // 4 bits
    frame_location: u8,         // 4 bits
    extension_field: [u8; 53],  // 424 bits
    crc: u32,                   // 32 bits
}

impl From<[u8; 184]> for TSMFHeaderPacket {
    fn from(data: [u8; 184]) -> Self {
        let sync_signal_unused = data[0] & 0b1110_0000;
        let sync_signal = ((data[0] as u16 & 0b0001_1111) << 8) | data[1] as u16;
        let alter_indicator = (data[2] & 0b1110_0000) >> 5;
        let slot_info_indicator = ((data[2] & 0b0001_0000) >> 4) == 1;
        let multi_frame_type = data[2] & 0b0000_1111;
        let relative_stream_enabled = [
            ((data[3] & 0b1000_0000) >> 7) == 1,
            ((data[3] & 0b0100_0000) >> 6) == 1,
            ((data[3] & 0b0010_0000) >> 5) == 1,
            ((data[3] & 0b0001_0000) >> 4) == 1,
            ((data[3] & 0b0000_1000) >> 3) == 1,
            ((data[3] & 0b0000_0100) >> 2) == 1,
            ((data[3] & 0b0000_0010) >> 1) == 1,
            ((data[3] & 0b0000_0001) >> 0) == 1,
            ((data[4] & 0b1000_0000) >> 7) == 1,
            ((data[4] & 0b0100_0000) >> 6) == 1,
            ((data[4] & 0b0010_0000) >> 5) == 1,
            ((data[4] & 0b0001_0000) >> 4) == 1,
            ((data[4] & 0b0000_1000) >> 3) == 1,
            ((data[4] & 0b0000_0100) >> 2) == 1,
            ((data[4] & 0b0000_0010) >> 1) == 1,
        ];
        let slot_info_undefined = data[4] & 0b0000_0001;

        let mut relative_stream_info = [0u32; 15];
        for i in 0..15 {
            relative_stream_info[i] =
                u32::from_be_bytes(data[5 + 4 * i..9 + 4 * i].try_into().unwrap());
        } // 64

        let mut relative_stream_reception_info = [0u8; 15];
        for i in 0..15 {
            relative_stream_reception_info[i] =
                (data[65 + i / 4] & (0b1100_0000 >> (2 * (i % 4)))) >> (6 - 2 * (i % 4)); // ?
        }

        let transmission_control_undefined = (data[68] & 0b0000_0010) >> 1;
        let emergency_warning_info = data[68] & 0b0000_0001;
        // 68

        let mut relative_stream_correspondence = [0u8; 52];
        for i in 0..26 {
            relative_stream_correspondence[i * 2] = (data[69 + i] & 0b1111_0000) >> 4;
            relative_stream_correspondence[i * 2 + 1] = data[69 + i] & 0b0000_1111;
        }

        // let extended_information: [u8; 85] = data[95..180].try_into().unwrap();

        let earthquake_warning_information: [u8; 26] = data[95..121].try_into().unwrap();
        let stream_type = ((data[121] as u16) << 7) & ((data[122] & 0b1111_1110) >> 1) as u16;
        let stream_type_unused_0 = data[122] & 0b0000_0001;
        let streams_identification = data[123];
        let streams_count = data[124];
        let streams_order = data[125];
        let frame_count = (data[126] & 0b1111_0000) >> 4;
        let frame_location = data[126] & 0b0000_1111;
        let extension_field: [u8; 53] = data[127..180].try_into().unwrap();

        let crc = ((data[180] as u32) << 24)
            | ((data[181] as u32) << 16)
            | ((data[182] as u32) << 8)
            | data[183] as u32;

        Self {
            sync_signal_unused,
            sync_signal,
            alter_indicator,
            slot_info_indicator,
            multi_frame_type,
            relative_stream_enabled,
            slot_info_undefined,
            relative_stream_info,
            relative_stream_reception_info,
            transmission_control_undefined,
            emergency_warning_info,
            relative_stream_correspondence,
            earthquake_warning_information,
            stream_type,
            streams_identification,
            streams_count,
            streams_order,
            frame_count,
            frame_location,
            extension_field,
            crc,
        }
    }
}
