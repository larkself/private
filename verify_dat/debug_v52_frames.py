#!/usr/bin/env python3
"""诊断 v52 解析器为什么只解出 1 条记录。"""
import sys, struct
sys.path.insert(0, 'stock-grabber/tools')

from pathlib import Path
from dat_to_dataframe import extract_blocks, u32, u24, is_trading_ts

# 一个成功和一个失败的文件
files = [
    ('OK',   '/Users/yishwu/Code/yishui/larkself/2025-02-14/raw/pankou/sh600052.dat'),
    ('FAIL', '/Users/yishwu/Code/yishui/larkself/2025-02-14/raw/pankou/sh600004.dat'),
    ('FAIL', '/Users/yishwu/Code/yishui/larkself/2025-02-14/raw/pankou/sh600007.dat'),
]

for label, path in files:
    raw = Path(path).read_bytes()
    version = raw[2]
    blocks = extract_blocks(raw)
    data = blocks[0]

    rec_count = u32(data, 0)
    print(f"\n=== {Path(path).name} ({label}) ===")
    print(f"  version: {version:#x}, block0: {len(data)}B")
    print(f"  rec_count (u32 @ 0): {rec_count}")

    # 手动解析前几帧
    pos = 4
    for frame_i in range(min(5, rec_count)):
        if pos + 5 > len(data):
            print(f"  frame {frame_i}: EOF at pos={pos}")
            break
        seq = data[pos]
        ts = u24(data, pos + 1)
        fc = data[pos + 4]
        print(f"  frame {frame_i}: pos={pos} seq={seq} ts={ts} fc={fc}", end="")

        if fc > 20:
            print(f" ❌ fc>20, break")
            break

        types = list(data[pos + 5:pos + 5 + fc])
        print(f" types={[hex(t) for t in types]}", end="")

        vpos = pos + 5 + fc
        values = {}
        for t in types:
            if vpos + 4 > len(data):
                print(f" ❌ value overflow")
                break
            values[t] = u32(data, vpos)
            vpos += 4

        # OB
        if vpos < len(data):
            ob_count = data[vpos]
            vpos += 1
            print(f" ob_count={ob_count}", end="")
            if ob_count <= 20:
                parsed = 0
                for _ in range(ob_count):
                    if vpos + 5 > len(data):
                        print(f" ❌ ob overflow")
                        break
                    price = u32(data, vpos)
                    side = data[vpos + 4]
                    if side > 3:
                        print(f" ❌ side={side}>3")
                        break
                    if side in (0, 1):
                        if vpos + 9 > len(data):
                            break
                        vol = u32(data, vpos + 5)
                        vpos += 9
                    else:
                        vpos += 5
                    parsed += 1
                if parsed != ob_count:
                    print(f" ❌ parsed={parsed}!=ob_count={ob_count}")
                else:
                    print(f" ✅ ob parsed ok", end="")
            else:
                print(f" ❌ ob_count>20")
        print(f" next_pos={vpos}")
        pos = vpos

    # 看看第 2 帧位置的原始字节
    if rec_count >= 2:
        print(f"  bytes at next frame pos={pos}: {data[pos:pos+20].hex(' ')}")
