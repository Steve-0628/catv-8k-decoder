use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

fn main() {
    // ./in/ 以下のTSファイルを全部読む
    let mut paths: Vec<_> = std::fs::read_dir("./in")
        .unwrap()
        .map(|p| p.unwrap().path())
        .collect();
    paths.sort(); // ファイル名順にソート（念のため）

    // まず各ファイルの frame_position=0 を探して位相を合わせる
    let mut streams: Vec<StreamState> = paths
        .iter()
        .map(|path| {
            let mut file = File::open(path).unwrap();
            let mut buf = [0u8; 188];
            let mut offset: u64 = 0;

            loop {
                file.seek(SeekFrom::Start(offset)).unwrap();
                if file.read_exact(&mut buf).is_err() {
                    panic!("frame_position=0 が見つからなかった: {:?}", path);
                }
                let packet = TSPacket::from(buf);
                if packet.pid == 0x2f {
                    let tsmf = TSMFHeaderPacket::from(packet.payload);
                    if tsmf.frame_position == 0 {
                        println!(
                            "{:?}: carrier_sequence={} number_of_carriers={} number_of_frames={} frame_position={}",
                            path,
                            tsmf.carrier_sequence,
                            tsmf.number_of_carriers,
                            tsmf.number_of_frames,
                            tsmf.frame_position
                        );
                        return StreamState {
                            carrier_sequence: tsmf.carrier_sequence,
                            number_of_frames: tsmf.number_of_frames,
                            offset,
                            file,
                        };
                    }
                }
                offset += 188;
            }
        })
        .collect();

    // carrier_sequence 順にソート
    streams.sort_by_key(|s| s.carrier_sequence);

    let n_carriers = streams.len();
    // number_of_frames は全波共通のはずだが、念のため最初のストリームから取る
    let number_of_frames = streams[0].number_of_frames as usize;

    println!(
        "搬送波数: {}, number_of_frames: {}",
        n_carriers, number_of_frames
    );

    let mut outfile = File::create("out.mmts").unwrap();

    // スーパーフレームは256QAMなので拡張TSMF×4
    // サブフレームは53個/スーパーフレーム
    // 各サブフレーム = number_of_frames 個のスロット
    //
    // 合成順: サブフレーム単位で、各搬送波の対応するスロット群を
    // carrier_sequence 順に並べる
    //
    // 実装方針:
    // 各ストリームから TSパケットを順番に読んでいき、
    // 0x2d (TLVペイロード) を number_of_frames 個ずつ取り出して
    // carrier_sequence 順に outfile に書く

    // 'outer: loop {
    //     // 各搬送波から number_of_frames 個の 0x2d パケットを集める
    //     // let mut subframes: Vec<Vec<Vec<u8>>> = vec![Vec::new(); n_carriers];

    //     // for (i, stream) in streams.iter_mut().enumerate() {
    //     let mut i = 1;
    //     loop {
    //         // let mut collected = 0;
    //         // let mut stream = streams[i];
    //         let mut buf = [0u8; 188];

    //         loop {
    //             streams[i].file.seek(SeekFrom::Start(streams[i].offset)).unwrap();
    //             if streams[i].file.read_exact(&mut buf).is_err() {
    //                 // EOF
    //                 break 'outer;
    //             }
    //             streams[i].offset += 188;

    //             let packet = TSPacket::from(buf);
    //             match packet.pid {
    //                 0x2f => {
    //                     // TSMFヘッダ: このサブフレームの区切りとして使う
    //                     // number_of_frames 個集まったら次の搬送波へ
    //                     // if collected >= number_of_frames {
    //                     //     break;
    //                     // }
    //                     // まだ集まっていなければ読み飛ばして続ける 
    //                     break;
    //                 }
    //                 0x2d => {
    //                     let payload = extract_payload(&packet);
    //                     // subframes[i].push(payload);
    //                     // collected += 1;
    //                     // if collected >= number_of_frames {
    //                     //     break;
    //                     // }
    //                     let _ = outfile.write(&payload).unwrap();
    //                 }
    //                 _ => {}
    //             }
    //         }
    //         i %= 3;
    //         i+=1;

    //     }

    //     // carrier_sequence 順 (= streams のソート済み順) に書き出す
    //     // for payloads in &subframes {
    //     //     for payload in payloads {
    //     //         outfile.write_all(payload).unwrap();
    //     //     }
    //     // }
    // }
    let mut super_frame_count = 0;
    'outer: loop {
        for stream in streams.iter_mut() {
            let mut buf = [0u8; 188];
            loop {
                if stream.file.read_exact(&mut buf).is_err() {
                    break 'outer;
                }
                let packet = TSPacket::from(buf);
                match packet.pid {
                    0x2f => {
                        let tsmf = TSMFHeaderPacket::from(packet.payload.clone());
                        eprintln!("stream {} frame_position={}", stream.carrier_sequence, tsmf.frame_position);
                        if tsmf.frame_position == 3 {
                            super_frame_count += 1;
                        }
                        break;
                    }
                    0x2d => {
                        outfile.write_all(&packet.payload).unwrap();
                    }
                    _ => {}
                }
            }
        }
        if super_frame_count >= 1 {
            break;
        }
    }

    // println!("{:?}", count);
    eprintln!("完了");
}

/// TSパケットからペイロードを取り出す (adaptation field を考慮)
// fn extract_payload(packet: &TSPacket) -> Vec<u8> {
//     if packet.payload_unit_start_indicator && !packet.payload.is_empty() {
//         packet.payload[1..].to_vec()
//     } else {
//         packet.payload.to_vec()
//     }
// }

struct StreamState {
    carrier_sequence: u8,
    number_of_frames: u8,
    offset: u64,
    file: File,
}

struct TSPacket {
    pid: u16,
    payload_unit_start_indicator: bool,
    payload: Vec<u8>,
}

impl From<[u8; 188]> for TSPacket {
    fn from(data: [u8; 188]) -> Self {
        let pid = ((data[1] as u16 & 0b0001_1111) << 8) | data[2] as u16;
        let payload_unit_start_indicator = ((data[1] & 0b0100_0000) >> 6) == 1;
        let adaptation_field_control = (data[3] & 0b0011_0000) >> 4;

        // adaptation field を考慮してペイロード開始位置を決める
        let payload_start = match adaptation_field_control {
            0b10 => {
                // adaptation fieldのみ、ペイロードなし
                188 // 空
            }
            0b11 => {
                // adaptation field + payload
                let af_len = data[4] as usize;
                5 + af_len // 4バイトヘッダ + 1バイト長さフィールド + adaptation field本体
            }
            _ => {
                // 0b01: ペイロードのみ
                4
            }
        };
        // let payload: Vec<u8> = if payload_unit_start_indicator { data[4..].to_vec() } else { data[3..].to_vec() };
        let payload = data[4..].to_vec();
        Self {
            pid,
            payload_unit_start_indicator,
            payload,
        }
    }
}

#[derive(Debug)]
struct TSMFHeaderPacket {
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
    earthquake_early_warning: [u8; 26],
    stream_type: [bool; 15],
    group_id: u8,
    number_of_carriers: u8,
    carrier_sequence: u8,
    number_of_frames: u8,
    frame_position: u8,
    crc: u32,
}

impl From<Vec<u8>> for TSMFHeaderPacket {
    fn from(data: Vec<u8>) -> Self {
        // ペイロードが短い場合のガード
        assert!(data.len() >= 184, "TSMFヘッダが短すぎる: {}バイト", data.len());

        let frame_sync = ((data[0] as u16 & 0b0001_1111) << 8) | data[1] as u16;
        let version_number = (data[2] & 0b1110_0000) >> 5;
        let relative_stream_number_mode = ((data[2] & 0b0001_0000) >> 4) == 1;
        let frame_type = data[2] & 0b0000_1111;

        // stream_status[15]: byte3の上位8bit + byte4の上位7bit
        let mut stream_status = [false; 15];
        for i in 0..8 {
            stream_status[i] = ((data[3] >> (7 - i)) & 1) == 1;
        }
        for i in 0..7 {
            stream_status[8 + i] = ((data[4] >> (7 - i)) & 1) == 1;
        }

        // stream_id[15] + original_network_id[15]: byte5-64 (60bytes)
        let mut stream_id = [0u16; 15];
        let mut original_network_id = [0u16; 15];
        for i in 0..15 {
            let base = 5 + i * 4;
            stream_id[i] = u16::from_be_bytes([data[base], data[base + 1]]);
            original_network_id[i] = u16::from_be_bytes([data[base + 2], data[base + 3]]);
        }

        // receive_status[15]: 2bits×15=30bits → byte65-68の途中
        let mut receive_status = [0u8; 15];
        for i in 0..15 {
            let byte_idx = 65 + i / 4;
            let shift = 6 - 2 * (i % 4);
            receive_status[i] = (data[byte_idx] >> shift) & 0b11;
        }

        // byte68: [...reserved(1)][emergency_indicator(1)][reserved(1)][reserved(1)]
        // 30bits消費後の残り: byte67の下位2bit + byte68
        // 30bits = 7bytes + 6bits なので byte65+7=72? → 要確認
        // とりあえず元のコードの位置を踏襲
        let emergency_indicator = ((data[68] & 0b0000_0010) >> 1) == 1;

        // relative_stream_number[52]: 4bits×52=208bits=26bytes → byte69-94
        let mut relative_stream_number = [0u8; 52];
        for i in 0..26 {
            relative_stream_number[i * 2] = (data[69 + i] & 0b1111_0000) >> 4;
            relative_stream_number[i * 2 + 1] = data[69 + i] & 0b0000_1111;
        }

        // earthquake_early_warning: 204bits → byte95-120 (上位4bitが最後の4bit)
        let earthquake_early_warning: [u8; 26] = data[95..121].try_into().unwrap();
        // byte120の下位4bit: '0000' padding

        // stream_type[15]: byte121-122
        // byte121: stream_type[0..7] (8bits)
        // byte122: stream_type[8..14] (7bits) + '0' padding (1bit)
        let mut stream_type = [false; 15];
        for i in 0..8 {
            stream_type[i] = ((data[121] >> (7 - i)) & 1) == 1;
        }
        for i in 0..7 {
            stream_type[8 + i] = ((data[122] >> (7 - i)) & 1) == 1;
        }

        let group_id = data[123];
        let number_of_carriers = data[124];
        let carrier_sequence = data[125];
        let number_of_frames = (data[126] & 0b1111_0000) >> 4;
        let frame_position = data[126] & 0b0000_1111;

        let crc = u32::from_be_bytes([data[180], data[181], data[182], data[183]]);

        Self {
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
            earthquake_early_warning,
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