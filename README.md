# catv-8k-decoder

`catv-8k-decoder` は、CATV 8K の 3 波 TS から Extended TSMF と Split TLV を読み取り、3 波を同期して RSN 1 の TLV パケット列を再構成する Rust ツールです。

このリポジトリは映像を直接デコードするものではなく、後段で扱える MMTS/TLV バイト列を取り出すための実験用デコーダです。

## できること

- Extended TSMF header (PID `0x002f`) から `frame_position = 0` の候補を集めて同期点を探索する
- `carrier_sequence` 順に 3 波をそろえてスーパーフレームを再構成する
- RSN 1 かつ TLV のデータスロットだけを抽出する
- Split TLV (PID `0x002d`) を再構成して MMTS/TLV ストリームとして出力する

## 前提

- Rust と Cargo が使えること
- 入力が 3 系統の TS であること
- 各 TS に Extended TSMF header と Split TLV packet が含まれていること

## ビルド

```bash
cargo build --release
```

## 使い方

### file mode

引数なしで実行すると file mode で動きます。`./in` ディレクトリ内の TS ファイルをファイル名順で読み込み、最良の同期位置を探して `out_rsn_01.mmts` を書き出します。

現状の実装は 3 波前提なので、`./in` には 3 つの TS ファイルを置いてください。

```bash
cargo run --release
```

入力例:

```text
in/
  135.ts
  141.ts
  147.ts
```

出力:

- `out_rsn_01.mmts`

### live mode

`--live` を付けると、3 つの入力を並行に読みながら同期して、再構成した MMTS/TLV を標準出力へ流します。入力はローカルファイルパスまたは `http://` / `https://` URL を使えます。

```bash
cargo run --release -- --live <ts1|url1> <ts2|url2> <ts3|url3> > out_rsn_01.mmts
```

例:

```bash
cargo run --release -- --live http://host/a.ts http://host/b.ts http://host/c.ts > out_rsn_01.mmts
```

補足:

- バイナリストリームは標準出力へ出ます
- ログは標準エラーへ出ます
- パケットドロップでフレームが欠けた場合は、TSMF ヘッダの continuity_counter の飛びと frame_position のズレで即座に検出し、自動で再同期します
- 検出をすり抜けた劣化も、エラー率が一定以上に増えるとフォールバックで再同期します

## いまの制約

- 3 波専用です
- `number_of_frames = 4` を前提にしています
- 出力対象の RSN は `1` に固定です
- TLV 再構成の `payload_start` は `3` に固定です
- file mode の出力先は `out_rsn_01.mmts` 固定です

## 実装メモ

- 本体コードは [src/main.rs](src/main.rs) にあります
- live mode では `ureq` を使って HTTP/HTTPS 入力を開きます
- `target/`、`*.ts`、`*.mmts` は `.gitignore` 対象です

# Information

<https://www.soumu.go.jp/main_content/000317625.pdf>
