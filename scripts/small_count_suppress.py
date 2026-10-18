#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""small_count_suppress.py — 类别 3:小样本抑制(公卫数据发布的披露控制)。

发布聚合表前抑制小计数单元,防止从合计反推出个体。两种抑制:

  * **主抑制**:计数落在 ``[1, threshold)`` 的单元直接抑制。
    (0 不抑制 —— 零计数不泄露任何个体。)
  * **二次抑制**:某一行/列只剩**一个**被抑制单元时,用该行/列的合计
    相减即可反推出它,所以必须在同一行/列再抑制一个最小单元。
    二次抑制会制造出新的"单抑制行/列",因此循环到稳定。

⚠️ 二次抑制的前提是「边际合计已公布」(公卫报表的常见情形)。
   若合计不公开,用 ``--no-secondary`` 只做主抑制。

⚠️ 本脚本处理的是**聚合表**,不是明细。明细级去标识请用
   ``hospital_cleaner.py`` 的 ``deidentify()``。

Usage:
    # 已聚合的表:指定计数列
    python small_count_suppress.py --input agg.csv --count-col n \\
        --group 科室 --out suppressed.csv

    # 原始明细:现算计数
    python small_count_suppress.py --input raw.csv --group 科室 --group 年龄段 \\
        --threshold 5 --marker "<5" --out suppressed.csv
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("small_count_suppress")


def _build_lines(work: pd.DataFrame, group_cols: list) -> list:
    """构造"可能被合计反推"的线。

    一维(频数表):整张表就是一条线。
    二维(列联表):每行一条 + 每列一条。
    """
    if len(group_cols) == 1:
        return [list(range(len(work)))]
    if len(group_cols) >= 2:
        lines = []
        for col in (group_cols[0], group_cols[1]):
            for _, idx in work.groupby(col, dropna=False).groups.items():
                lines.append(sorted(int(i) for i in idx))
        return lines
    return []


def suppress_small_counts(df: pd.DataFrame, group_cols=None, count_col=None,
                          threshold: int = 5, secondary: bool = True,
                          marker=None, log_entries=None):
    """对聚合表做主抑制 + 二次抑制。

    Args:
        df: 已聚合表(给 count_col)或原始明细(给 group_cols,内部先聚合计数)
        group_cols: 分组列。给 1 个 = 频数表;给 2 个 = 列联表(行/列都做二次抑制)
        count_col: 计数列名。不给则按 group_cols 现算
        threshold: 小于该值的非零计数被抑制(默认 5,即 n<5)
        secondary: 是否做二次抑制
        marker: 抑制后的占位值。默认 None(留空);可传 "<5" 之类

    Returns:
        (df_out, info):df_out 追加 ``suppressed``(bool)与 ``suppression_reason``
        (primary / secondary / 空)两列。
    """
    if threshold < 2:
        raise ValueError("threshold 必须 >= 2")

    group_cols = [group_cols] if isinstance(group_cols, str) else list(group_cols or [])
    log_entries = log_entries if log_entries is not None else []

    if count_col:
        if count_col not in df.columns:
            raise ValueError(f"count_col {count_col!r} 不在数据里")
        work = df.copy()
        if not group_cols:
            group_cols = [c for c in df.columns if c != count_col]
    else:
        if not group_cols:
            raise ValueError("必须给 count_col(已聚合表)或 group_cols(原始明细)之一")
        work = df.groupby(group_cols, dropna=False).size().reset_index(name="count")
        count_col = "count"

    work = work.reset_index(drop=True)
    counts = pd.to_numeric(work[count_col], errors="coerce").fillna(0).astype(int)

    suppressed, reason = set(), {}
    for i in range(len(work)):
        if 0 < counts.iloc[i] < threshold:
            suppressed.add(i)
            reason[i] = "primary"

    if secondary and suppressed:
        lines = _build_lines(work, group_cols)
        changed, guard = True, 0
        while changed and guard < 200:
            changed, guard = False, guard + 1
            for line in lines:
                sup = [i for i in line if i in suppressed]
                if len(sup) != 1:
                    continue
                cand = [i for i in line if i not in suppressed]
                if not cand:
                    continue
                pick = min(cand, key=lambda i: counts.iloc[i])
                suppressed.add(pick)
                reason[pick] = "secondary"
                changed = True

    out = work.copy()
    out["suppressed"] = [i in suppressed for i in range(len(work))]
    out["suppression_reason"] = [reason.get(i, "") for i in range(len(work))]
    if suppressed:
        if marker is None:
            # 留空:整型列放不下 NaN,先转可空整数(Int64),
            # 这样计数仍是整数语义,同时又支持缺失
            out[count_col] = out[count_col].astype("Int64")
            out.loc[out["suppressed"], count_col] = pd.NA
        else:
            # marker 可能是 "<5" 这类字符串。必须先转 object:
            # 整型 dtype 放不下字符串,pandas 3 会直接抛 TypeError。
            out[count_col] = out[count_col].astype(object)
            out.loc[out["suppressed"], count_col] = marker

    n_primary = sum(1 for v in reason.values() if v == "primary")
    n_secondary = sum(1 for v in reason.values() if v == "secondary")
    info = {"threshold": threshold, "rows": int(len(work)),
            "primary": n_primary, "secondary": n_secondary,
            "suppressed_rows": int(len(suppressed))}
    log.info(f"抑制完成:主抑制 {n_primary} 个,二次抑制 {n_secondary} 个"
             f"(共 {len(work)} 行,阈值 <{threshold})")
    log_entries.append({"category": "suppression", "action": "small_count_suppressed",
                        **info})
    if marker is None:
        log.info("抑制单元已留空;如需显式占位请用 --marker '<5'")
    return out, info


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    ap = argparse.ArgumentParser(description="小计数抑制(公卫披露控制)")
    ap.add_argument("--input", required=True, help="输入 CSV")
    ap.add_argument("--out", required=True, help="输出 CSV")
    ap.add_argument("--group", action="append", default=[],
                    help="分组列,可重复给(1 个=频数表,2 个=列联表)")
    ap.add_argument("--count-col", default=None,
                    help="已聚合表的计数列名;不给则按 --group 现算")
    ap.add_argument("--threshold", type=int, default=5)
    ap.add_argument("--marker", default=None, help="抑制后的占位值,如 '<5'(默认留空)")
    ap.add_argument("--no-secondary", action="store_true",
                    help="只做主抑制(边际合计不公开时用)")
    ap.add_argument("--log", default=None, help="另存抑制日志 CSV")
    args = ap.parse_args(argv)

    df = pd.read_csv(args.input, low_memory=False)
    log_entries = []
    out, info = suppress_small_counts(df, group_cols=args.group,
                                      count_col=args.count_col,
                                      threshold=args.threshold,
                                      secondary=not args.no_secondary,
                                      marker=args.marker, log_entries=log_entries)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(json.dumps(info, ensure_ascii=False, indent=2))
    if args.log:
        pd.DataFrame(log_entries).to_csv(args.log, index=False, encoding="utf-8-sig")

    if info["suppressed_rows"] == 0:
        log.info("没有任何单元需要抑制")
    return 0


if __name__ == "__main__":
    sys.exit(main())
