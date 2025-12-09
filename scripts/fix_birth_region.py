#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fix_birth_region.py

遍历 data/raw/** 目录下所有 worker 子目录：
  - 读取 person_details.jsonl
  - 用改进版 _extract_region_from_place 重算 birth_region
  - 写出到同目录下的 person_details_fixed.jsonl

原文件不会被覆盖，方便你检查 OK 之后再替换。
"""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Optional


def _extract_region_from_place(place: str) -> Optional[str]:
    """
    尽量从出生地中抽取“国家/地区”这一段。

    处理形式示例：
      - "美国,新泽西州,纽瓦克"        -> "美国"
      - "英国 苏塞克斯 郡沃辛"        -> "英国"
      - "法国.巴黎"                  -> "法国"
      - "日本·东京"                  -> "日本"
      - "印度新德里"                 -> "印度新德里"  # 连写没法自动拆，只能保留原样
    """
    if not place:
        return None

    s = str(place).strip()

    # 1）统一常见分隔符为逗号
    #    中文逗号、顿号、斜杠、竖线、间隔点等
    for ch in ["，", "、", "／", "/", "|", "·"]:
        s = s.replace(ch, ",")

    # 2）把“汉字 . 汉字 / 汉字 。 汉字”这种用点当分隔符的情况处理掉
    #    例如 "英国.苏塞克斯 郡沃辛"、"法国.巴黎"
    s = re.sub(r"(?<=[\u4e00-\u9fff])\s*\.\s*(?=[\u4e00-\u9fff])", ",", s)
    s = re.sub(r"(?<=[\u4e00-\u9fff])\s*。\s*(?=[\u4e00-\u9fff])", ",", s)

    # 3）连续空白（包括全角空格）也视作分隔符
    s = re.sub(r"[\s\u3000]+", ",", s)

    # 4）合并重复的逗号
    s = re.sub(r",+", ",", s)

    # 5）按逗号切分后，取第一段作为 birth_region
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        return None

    return parts[0]


def process_person_file(src_path: str, dst_path: str) -> None:
    """读取一个 person_details.jsonl，生成修正后的 person_details_fixed.jsonl。"""
    total = 0
    updated = 0

    print(f"[process] {src_path} -> {dst_path}")

    with open(src_path, "r", encoding="utf-8") as fin, \
            open(dst_path, "w", encoding="utf-8") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue

            total += 1
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[warn] JSON decode 失败，跳过一行: {e}")
                continue

            place = obj.get("birth_place_raw")
            new_region = _extract_region_from_place(place) if place else None

            old_region = obj.get("birth_region")
            # 这里直接覆盖，无论旧值是什么
            obj["birth_region"] = new_region

            if new_region != old_region:
                updated += 1

            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

    print(f"[done] {src_path}: total={total}, updated={updated}")


def find_person_files(raw_root: str) -> list[tuple[str, str]]:
    """
    在 raw_root 下递归查找所有 person_details.jsonl，
    返回 (src_path, dst_path) 列表。
    """
    results: list[tuple[str, str]] = []

    for root, dirs, files in os.walk(raw_root):
        if "person_details.jsonl" in files:
            src = os.path.join(root, "person_details.jsonl")
            dst = os.path.join(root, "person_details_fixed.jsonl")
            results.append((src, dst))

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="修正 person_details.jsonl 中的 birth_region 字段"
    )
    parser.add_argument(
        "--raw-root",
        type=str,
        default="../data/raw",
        help="原始数据根目录（默认: data/raw）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_root = args.raw_root

    if not os.path.isdir(raw_root):
        print(f"[error] 原始目录不存在: {raw_root}")
        return

    pairs = find_person_files(raw_root)
    if not pairs:
        print(f"[info] 在 {raw_root} 下没有找到任何 person_details.jsonl")
        return

    print(f"[info] 共找到 {len(pairs)} 个 person_details.jsonl 文件")

    for src, dst in pairs:
        process_person_file(src, dst)


if __name__ == "__main__":
    main()
