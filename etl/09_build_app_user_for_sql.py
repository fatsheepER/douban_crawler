#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_app_users_for_sql.py

从 users.csv 生成可以直接 \copy INTO app_user 的 CSV：

源文件（../data/etl/users.csv）格式：
    id,user_hash,name,email

目标文件（../data/etl/app_users_for_sql.csv）格式：
    id,name,mail

其中：
    - id   -> 直接沿用 users.csv 里的 id
    - name -> 使用 user_hash 作为用户名（脱敏、稳定）
    - mail -> 使用 email 字段
"""

from __future__ import annotations

import csv
import os
from typing import Dict, Set

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_DIR = os.path.join(BASE_DIR, "..", "data", "etl")

USERS_CSV = os.path.join(ETL_DIR, "users.csv")
APP_USERS_FOR_SQL_CSV = os.path.join(ETL_DIR, "app_users_for_sql.csv")


def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def build_app_users_for_sql() -> None:
    print(f"[users] 读取源文件: {USERS_CSV}")
    print(f"[app_user] 输出文件: {APP_USERS_FOR_SQL_CSV}")

    if not os.path.exists(USERS_CSV):
        raise FileNotFoundError(f"找不到 users.csv: {USERS_CSV}")

    ensure_dir_for_file(APP_USERS_FOR_SQL_CSV)

    total = 0
    skipped = 0
    seen_hashes: Set[str] = set()
    seen_mails: Set[str] = set()

    with open(USERS_CSV, "r", encoding="utf-8", newline="") as f_in, \
            open(APP_USERS_FOR_SQL_CSV, "w", encoding="utf-8", newline="") as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=["id", "name", "mail"])
        writer.writeheader()

        for row in reader:
            total += 1

            user_id_raw = row.get("id")
            user_hash = (row.get("user_hash") or "").strip()
            email = (row.get("email") or row.get("mail") or "").strip()

            if not user_id_raw or not user_hash or not email:
                skipped += 1
                continue

            # 保证 id 是整数（\copy 进 serial 没问题）
            try:
                user_id_int = int(str(user_id_raw).strip())
            except (TypeError, ValueError):
                skipped += 1
                continue

            # 简单去重：同一个 hash 或 email 出现多次就只保留第一条
            if user_hash in seen_hashes or email in seen_mails:
                skipped += 1
                continue

            seen_hashes.add(user_hash)
            seen_mails.add(email)

            writer.writerow({
                "id": user_id_int,
                "name": user_hash,  # 注意：用 hash 作为用户名存入 app_user.name
                "mail": email,
            })

    print(f"[app_user] 总记录数: {total}")
    print(f"[app_user] 写入行数: {total - skipped}")
    print(f"[app_user] 跳过行数: {skipped}")


def main() -> None:
    build_app_users_for_sql()
    print("===> app_users_for_sql.csv 构建完成")


if __name__ == "__main__":
    main()
