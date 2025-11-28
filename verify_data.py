#!/usr/bin/env python3
import csv
import glob
import re
from collections import defaultdict


def parse_filename(filename):
    pattern = r"(raft|zab)_write_prob_(\d+)_fault_enabled_kill_prob_(\d+)_nodes_(\d+)_summary\.csv"
    match = re.search(pattern, filename)
    if match:
        return {
            "protocol": match.group(1),
            "write_prob": int(match.group(2)),
            "kill_prob": int(match.group(3)),
            "nodes": int(match.group(4)),
        }
    return None


def read_summary_csv(filepath):
    metrics = {}
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            metrics[row["metric"]] = float(row["value"])
    return metrics


for protocol in ["raft", "zab"]:
    pattern = f"{protocol}/data/*_summary.csv"
    files = sorted(glob.glob(pattern))

    print(f"\n{'=' * 80}")
    print(f"Checking {protocol.upper()} files (showing first 5)")
    print(f"{'=' * 80}")

    for filepath in files[:5]:
        info = parse_filename(filepath)
        if info:
            metrics = read_summary_csv(filepath)
            print(f"\nFile: {filepath}")
            print(f"  Parsed protocol: {info['protocol']}")
            print(
                f"  Write prob: {info['write_prob']}%, Kill prob: {info['kill_prob']}%, Nodes: {info['nodes']}"
            )
            print(
                f"  TPS: {metrics['avg_tps']:.2f}, Success rate: {metrics['avg_success_rate_percent']:.2f}%"
            )
