#!/usr/bin/env python3
"""暴力扫描 v52 文件, 找到真正的帧边界。"""
import sys
sys.path.insert(0, 'stock-grabber/tools')

from pathlib import Path
from dat_to_dataframe import extract_blocks, u32, u24, is_trading_ts

path = '/Users/yishwu/Code/yishui/larkself/2025-02-14/raw/pankou/sh600004.dat'
raw = Path(path).read_bytes()
blocks = extract_blocks(raw)
data = blocks[0]

# 第1帧在 pos=15, 解析了 types=[0x01,0x0a,0x0c,0x0d,0x0e], ob_count=0
# v52 解析器认为下一帧在 pos=46, 但这里是垃圾
# 扫描: 从 pos=46 开始, 找下一个有效 trading ts
print("=== 从 pos=40 开始扫描有效帧 ===")
for p in range(40, min(250, len(data))):
    if p + 5 > len(data):
        break
    ts = u24(data, p + 1)
    fc = data[p + 4]
    if fc > 0 and fc <= 12 and is_trading_ts(ts):
        types = list(data[p + 5:p + 5 + fc])
        valid = any(t in {0x01,0x02,0x03,0x07,0x08,0x0a,0x0c,0x0f} for t in types)
        if valid:
            print(f"  pos={p} seq={data[p]} ts={ts} fc={fc} types={[hex(t) for t in types]}")
            # 如果找到, 算一下从 frame1 end(pos=45) 到这里的偏移
            gap = p - 45
            print(f"    gap from frame1 ob_count end (pos=45): {gap} bytes")
            break

# 仔细看 pos=15 frame 的 types 和后续原始数据
print(f"\n=== frame1 详细数据分析 ===")
frame1_pos = 15
fc = data[frame1_pos + 4]
print(f"fc={fc}")
types = list(data[frame1_pos + 5:frame1_pos + 5 + fc])
print(f"types={[hex(t) for t in types]}")

# 打印 pos 15 ~ 150 的原始 hex (每行 16B)
print(f"\n=== 原始数据 pos=15..150 ===")
for start in range(15, 150, 16):
    end = min(start + 16, 150)
    hex_str = ' '.join(f'{data[i]:02x}' for i in range(start, end))
    ascii_str = ''.join(chr(data[i]) if 32 <= data[i] < 127 else '.' for i in range(start, end))
    print(f"  {start:4d}: {hex_str:<48s}  {ascii_str}")

# 同样看 OK 文件
print(f"\n=== sh600052 (OK) 原始数据 pos=15..100 ===")
path2 = '/Users/yishwu/Code/yishui/larkself/2025-02-14/raw/pankou/sh600052.dat'
raw2 = Path(path2).read_bytes()
blocks2 = extract_blocks(raw2)
data2 = blocks2[0]
for start in range(15, 100, 16):
    end = min(start + 16, 100)
    hex_str = ' '.join(f'{data2[i]:02x}' for i in range(start, end))
    print(f"  {start:4d}: {hex_str}")
