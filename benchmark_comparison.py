import csv
import glob
import re
from collections import defaultdict
import statistics
import matplotlib.pyplot as plt
import numpy as np


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


def collect_data():
    data = defaultdict(lambda: defaultdict(list))

    for protocol in ["raft", "zab"]:
        pattern = f"{protocol}/data/*_summary.csv"
        for filepath in glob.glob(pattern):
            info = parse_filename(filepath)
            if not info:
                continue

            metrics = read_summary_csv(filepath)
            key = (info["protocol"], info["nodes"], info["kill_prob"])
            data[key]["avg_tps"].append(metrics["avg_tps"])
            data[key]["avg_success_rate_percent"].append(
                metrics["avg_success_rate_percent"]
            )
            data[key]["p95_latency_ms"].append(metrics["p95_latency_ms"])

    return data


def aggregate_data(data):
    aggregated = {}
    for key, values in data.items():
        protocol, nodes, kill_prob = key
        aggregated[key] = {
            "avg_tps": statistics.mean(values["avg_tps"]),
            "avg_success_rate": statistics.mean(values["avg_success_rate_percent"]),
            "p95_latency": statistics.mean(values["p95_latency_ms"]),
        }
    return aggregated


def compare_cluster_size_vs_tps(aggregated):
    print("\n" + "=" * 80)
    print("BENCHMARK 1: Cluster Size vs Average TPS")
    print("=" * 80)

    cluster_sizes = sorted(set(nodes for _, nodes, _ in aggregated.keys()))
    kill_probs = sorted(set(kill_prob for _, _, kill_prob in aggregated.keys()))

    for kill_prob in kill_probs:
        print(f"\nLeader Kill Probability: {kill_prob}%")
        print("-" * 80)
        print(
            f"{'Cluster Size':<15} {'Raft TPS':<15} {'Zab TPS':<15} {'Difference':<15} {'Winner':<10}"
        )
        print("-" * 80)

        for nodes in cluster_sizes:
            raft_key = ("raft", nodes, kill_prob)
            zab_key = ("zab", nodes, kill_prob)

            if raft_key in aggregated and zab_key in aggregated:
                raft_tps = aggregated[raft_key]["avg_tps"]
                zab_tps = aggregated[zab_key]["avg_tps"]
                diff = raft_tps - zab_tps
                winner = "Raft" if diff > 0 else "Zab"

                print(
                    f"{nodes:<15} {raft_tps:<15.2f} {zab_tps:<15.2f} {diff:<15.2f} {winner:<10}"
                )


def compare_kill_prob_vs_tps(aggregated):
    print("\n" + "=" * 80)
    print("BENCHMARK 2: Leader Kill Percentage vs Average TPS")
    print("=" * 80)

    cluster_sizes = sorted(set(nodes for _, nodes, _ in aggregated.keys()))
    kill_probs = sorted(set(kill_prob for _, _, kill_prob in aggregated.keys()))

    for nodes in cluster_sizes:
        print(f"\nCluster Size: {nodes} nodes")
        print("-" * 80)
        print(
            f"{'Kill %':<15} {'Raft TPS':<20} {'Zab TPS':<20} {'Difference':<15} {'Winner':<10}"
        )
        print("-" * 80)

        for kill_prob in kill_probs:
            raft_key = ("raft", nodes, kill_prob)
            zab_key = ("zab", nodes, kill_prob)

            if raft_key in aggregated and zab_key in aggregated:
                raft_tps = aggregated[raft_key]["avg_tps"]
                zab_tps = aggregated[zab_key]["avg_tps"]
                diff = raft_tps - zab_tps
                winner = "Raft" if diff > 0 else "Zab"

                print(
                    f"{kill_prob:<15} {raft_tps:<20.2f} {zab_tps:<20.2f} {diff:<15.2f} {winner:<10}"
                )


def compare_kill_prob_vs_success_rate(aggregated):
    print("\n" + "=" * 80)
    print("BENCHMARK 3: Leader Kill Percentage vs Average Success Rate")
    print("=" * 80)

    cluster_sizes = sorted(set(nodes for _, nodes, _ in aggregated.keys()))
    kill_probs = sorted(set(kill_prob for _, _, kill_prob in aggregated.keys()))

    for nodes in cluster_sizes:
        print(f"\nCluster Size: {nodes} nodes")
        print("-" * 80)
        print(
            f"{'Kill %':<15} {'Raft Success %':<20} {'Zab Success %':<20} {'Difference':<15} {'Winner':<10}"
        )
        print("-" * 80)

        for kill_prob in kill_probs:
            raft_key = ("raft", nodes, kill_prob)
            zab_key = ("zab", nodes, kill_prob)

            if raft_key in aggregated and zab_key in aggregated:
                raft_success = aggregated[raft_key]["avg_success_rate"]
                zab_success = aggregated[zab_key]["avg_success_rate"]
                diff = raft_success - zab_success
                winner = "Raft" if diff > 0 else "Zab"

                print(
                    f"{kill_prob:<15} {raft_success:<20.2f} {zab_success:<20.2f} {diff:<15.2f} {winner:<10}"
                )


def compare_p95_latency_by_cluster_size(aggregated):
    print("\n" + "=" * 80)
    print("BENCHMARK 4: P95 Latency Percentile by Cluster Size")
    print("=" * 80)

    cluster_sizes = sorted(set(nodes for _, nodes, _ in aggregated.keys()))
    kill_probs = sorted(set(kill_prob for _, _, kill_prob in aggregated.keys()))

    for kill_prob in kill_probs:
        print(f"\nLeader Kill Probability: {kill_prob}%")
        print("-" * 80)
        print(
            f"{'Cluster Size':<15} {'Raft P95 (ms)':<20} {'Zab P95 (ms)':<20} {'Difference':<15} {'Winner':<10}"
        )
        print("-" * 80)

        for nodes in cluster_sizes:
            raft_key = ("raft", nodes, kill_prob)
            zab_key = ("zab", nodes, kill_prob)

            if raft_key in aggregated and zab_key in aggregated:
                raft_p95 = aggregated[raft_key]["p95_latency"]
                zab_p95 = aggregated[zab_key]["p95_latency"]
                diff = raft_p95 - zab_p95
                winner = "Zab" if diff > 0 else "Raft"

                print(
                    f"{nodes:<15} {raft_p95:<20.3f} {zab_p95:<20.3f} {diff:<15.3f} {winner:<10}"
                )


def generate_summary(aggregated):
    print("\n" + "=" * 80)
    print("OVERALL SUMMARY")
    print("=" * 80)

    raft_wins_tps = 0
    zab_wins_tps = 0
    raft_wins_success = 0
    zab_wins_success = 0
    raft_wins_latency = 0
    zab_wins_latency = 0

    cluster_sizes = sorted(set(nodes for _, nodes, _ in aggregated.keys()))
    kill_probs = sorted(set(kill_prob for _, _, kill_prob in aggregated.keys()))

    for nodes in cluster_sizes:
        for kill_prob in kill_probs:
            raft_key = ("raft", nodes, kill_prob)
            zab_key = ("zab", nodes, kill_prob)

            if raft_key in aggregated and zab_key in aggregated:
                if aggregated[raft_key]["avg_tps"] > aggregated[zab_key]["avg_tps"]:
                    raft_wins_tps += 1
                else:
                    zab_wins_tps += 1

                if (
                    aggregated[raft_key]["avg_success_rate"]
                    > aggregated[zab_key]["avg_success_rate"]
                ):
                    raft_wins_success += 1
                else:
                    zab_wins_success += 1

                if (
                    aggregated[raft_key]["p95_latency"]
                    < aggregated[zab_key]["p95_latency"]
                ):
                    raft_wins_latency += 1
                else:
                    zab_wins_latency += 1

    print(f"\nThroughput (TPS) - Raft: {raft_wins_tps} wins, Zab: {zab_wins_tps} wins")
    print(
        f"Success Rate - Raft: {raft_wins_success} wins, Zab: {zab_wins_success} wins"
    )
    print(
        f"Latency (P95) - Raft: {raft_wins_latency} wins, Zab: {zab_wins_latency} wins"
    )
    print()


def plot_cluster_size_vs_tps(aggregated):
    cluster_sizes = sorted(set(nodes for _, nodes, _ in aggregated.keys()))
    kill_probs = sorted(set(kill_prob for _, _, kill_prob in aggregated.keys()))

    fig, axes = plt.subplots(1, len(kill_probs), figsize=(18, 5))
    if len(kill_probs) == 1:
        axes = [axes]

    for idx, kill_prob in enumerate(kill_probs):
        raft_tps = []
        zab_tps = []

        for nodes in cluster_sizes:
            raft_key = ("raft", nodes, kill_prob)
            zab_key = ("zab", nodes, kill_prob)

            if raft_key in aggregated and zab_key in aggregated:
                raft_tps.append(aggregated[raft_key]["avg_tps"])
                zab_tps.append(aggregated[zab_key]["avg_tps"])
            else:
                raft_tps.append(0)
                zab_tps.append(0)

        x = np.arange(len(cluster_sizes))
        width = 0.35

        axes[idx].bar(x - width / 2, raft_tps, width, label="Raft", color="#2E86AB")
        axes[idx].bar(x + width / 2, zab_tps, width, label="Zab", color="#A23B72")

        axes[idx].set_xlabel("Cluster Size (nodes)", fontsize=11)
        axes[idx].set_ylabel("Average TPS", fontsize=11)
        axes[idx].set_title(
            f"Kill Probability: {kill_prob}%", fontsize=12, fontweight="bold"
        )
        axes[idx].set_xticks(x)
        axes[idx].set_xticklabels(cluster_sizes)
        axes[idx].legend()
        axes[idx].grid(axis="y", alpha=0.3)

    plt.suptitle(
        "Cluster Size vs Average TPS (Raft vs Zab)",
        fontsize=14,
        fontweight="bold",
        y=1.02,
    )
    plt.tight_layout()
    plt.savefig("cluster_size_vs_tps.png", dpi=300, bbox_inches="tight")
    print("Saved: cluster_size_vs_tps.png")
    plt.close()


def plot_kill_prob_vs_success_rate(aggregated):
    cluster_sizes = sorted(set(nodes for _, nodes, _ in aggregated.keys()))
    kill_probs = sorted(set(kill_prob for _, _, kill_prob in aggregated.keys()))

    fig, axes = plt.subplots(1, len(cluster_sizes), figsize=(18, 5))
    if len(cluster_sizes) == 1:
        axes = [axes]

    for idx, nodes in enumerate(cluster_sizes):
        raft_success = []
        zab_success = []

        for kill_prob in kill_probs:
            raft_key = ("raft", nodes, kill_prob)
            zab_key = ("zab", nodes, kill_prob)

            if raft_key in aggregated and zab_key in aggregated:
                raft_success.append(aggregated[raft_key]["avg_success_rate"])
                zab_success.append(aggregated[zab_key]["avg_success_rate"])
            else:
                raft_success.append(0)
                zab_success.append(0)

        axes[idx].plot(
            kill_probs,
            raft_success,
            marker="o",
            linewidth=2,
            markersize=8,
            label="Raft",
            color="#2E86AB",
        )
        axes[idx].plot(
            kill_probs,
            zab_success,
            marker="s",
            linewidth=2,
            markersize=8,
            label="Zab",
            color="#A23B72",
        )

        axes[idx].set_xlabel("Leader Kill Probability (%)", fontsize=11)
        axes[idx].set_ylabel("Success Rate (%)", fontsize=11)
        axes[idx].set_title(
            f"Cluster Size: {nodes} nodes", fontsize=12, fontweight="bold"
        )
        axes[idx].legend()
        axes[idx].grid(True, alpha=0.3)
        axes[idx].set_ylim([95, 100.5])

    plt.suptitle(
        "Leader Kill Probability vs Success Rate (Raft vs Zab)",
        fontsize=14,
        fontweight="bold",
        y=1.02,
    )
    plt.tight_layout()
    plt.savefig("kill_prob_vs_success_rate.png", dpi=300, bbox_inches="tight")
    print("Saved: kill_prob_vs_success_rate.png")
    plt.close()


def plot_kill_prob_vs_tps(aggregated):
    cluster_sizes = sorted(set(nodes for _, nodes, _ in aggregated.keys()))
    kill_probs = sorted(set(kill_prob for _, _, kill_prob in aggregated.keys()))

    fig, axes = plt.subplots(1, len(cluster_sizes), figsize=(18, 5))
    if len(cluster_sizes) == 1:
        axes = [axes]

    for idx, nodes in enumerate(cluster_sizes):
        raft_tps = []
        zab_tps = []

        for kill_prob in kill_probs:
            raft_key = ("raft", nodes, kill_prob)
            zab_key = ("zab", nodes, kill_prob)

            if raft_key in aggregated and zab_key in aggregated:
                raft_tps.append(aggregated[raft_key]["avg_tps"])
                zab_tps.append(aggregated[zab_key]["avg_tps"])
            else:
                raft_tps.append(0)
                zab_tps.append(0)

        axes[idx].plot(
            kill_probs,
            raft_tps,
            marker="o",
            linewidth=2,
            markersize=8,
            label="Raft",
            color="#2E86AB",
        )
        axes[idx].plot(
            kill_probs,
            zab_tps,
            marker="s",
            linewidth=2,
            markersize=8,
            label="Zab",
            color="#A23B72",
        )

        axes[idx].set_xlabel("Leader Kill Probability (%)", fontsize=11)
        axes[idx].set_ylabel("Average TPS", fontsize=11)
        axes[idx].set_title(
            f"Cluster Size: {nodes} nodes", fontsize=12, fontweight="bold"
        )
        axes[idx].legend()
        axes[idx].grid(True, alpha=0.3)

    plt.suptitle(
        "Leader Kill Probability vs TPS (Raft vs Zab)",
        fontsize=14,
        fontweight="bold",
        y=1.02,
    )
    plt.tight_layout()
    plt.savefig("kill_prob_vs_tps.png", dpi=300, bbox_inches="tight")
    print("Saved: kill_prob_vs_tps.png")
    plt.close()


def plot_p95_latency_by_cluster_size(aggregated):
    cluster_sizes = sorted(set(nodes for _, nodes, _ in aggregated.keys()))
    kill_probs = sorted(set(kill_prob for _, _, kill_prob in aggregated.keys()))

    fig, axes = plt.subplots(1, len(kill_probs), figsize=(18, 5))
    if len(kill_probs) == 1:
        axes = [axes]

    for idx, kill_prob in enumerate(kill_probs):
        raft_p95 = []
        zab_p95 = []

        for nodes in cluster_sizes:
            raft_key = ("raft", nodes, kill_prob)
            zab_key = ("zab", nodes, kill_prob)

            if raft_key in aggregated and zab_key in aggregated:
                raft_p95.append(aggregated[raft_key]["p95_latency"])
                zab_p95.append(aggregated[zab_key]["p95_latency"])
            else:
                raft_p95.append(0)
                zab_p95.append(0)

        x = np.arange(len(cluster_sizes))
        width = 0.35

        axes[idx].bar(x - width / 2, raft_p95, width, label="Raft", color="#2E86AB")
        axes[idx].bar(x + width / 2, zab_p95, width, label="Zab", color="#A23B72")

        axes[idx].set_xlabel("Cluster Size (nodes)", fontsize=11)
        axes[idx].set_ylabel("P95 Latency (ms)", fontsize=11)
        axes[idx].set_title(
            f"Kill Probability: {kill_prob}%", fontsize=12, fontweight="bold"
        )
        axes[idx].set_xticks(x)
        axes[idx].set_xticklabels(cluster_sizes)
        axes[idx].legend()
        axes[idx].grid(axis="y", alpha=0.3)

    plt.suptitle(
        "P95 Latency by Cluster Size (Raft vs Zab)",
        fontsize=14,
        fontweight="bold",
        y=1.02,
    )
    plt.tight_layout()
    plt.savefig("p95_latency_by_cluster_size.png", dpi=300, bbox_inches="tight")
    print("Saved: p95_latency_by_cluster_size.png")
    plt.close()


if __name__ == "__main__":
    print("Collecting benchmark data from CSV files...")
    data = collect_data()

    print("Aggregating data...")
    aggregated = aggregate_data(data)

    compare_cluster_size_vs_tps(aggregated)
    compare_kill_prob_vs_tps(aggregated)
    compare_kill_prob_vs_success_rate(aggregated)
    compare_p95_latency_by_cluster_size(aggregated)
    generate_summary(aggregated)

    print("\n" + "=" * 80)
    print("Generating visualizations...")
    print("=" * 80)
    plot_cluster_size_vs_tps(aggregated)
    plot_kill_prob_vs_tps(aggregated)
    plot_kill_prob_vs_success_rate(aggregated)
    plot_p95_latency_by_cluster_size(aggregated)

    print("\nBenchmark comparison complete!")
