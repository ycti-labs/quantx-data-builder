#!/usr/bin/env python3
"""
QuantX Data Builder - Dataset Size Visualizer

Analyzes and visualizes the size and structure of the QuantX data lake,
including prices, ESG data, results, and metadata across all tickers.
"""

import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Set visualization style
plt.style.use("seaborn-v0_8-darkgrid")
sns.set_palette("husl")

# ---------- CONFIGURATION ----------
DATA_ROOT = Path("data/curated")


def scan_dataset_structure():
    """
    Scan the entire data lake and collect statistics about:
    - Number of files
    - Total size
    - Data types (prices, ESG, results, metadata)
    - Tickers covered
    - Date ranges
    """
    stats = {
        "tickers": defaultdict(
            lambda: {"prices": 0, "esg": 0, "results": 0, "size_mb": 0}
        ),
        "references": defaultdict(lambda: {"prices": 0, "size_mb": 0}),
        "metadata": {"files": 0, "size_mb": 0},
        "membership": {"files": 0, "size_mb": 0},
        "results": defaultdict(lambda: {"files": 0, "size_mb": 0}),
    }

    total_files = 0
    total_size = 0

    if not DATA_ROOT.exists():
        print(f"❌ Data directory not found: {DATA_ROOT}")
        return stats, total_files, total_size

    print("🔍 Scanning dataset structure...")

    # Scan tickers
    tickers_path = DATA_ROOT / "tickers"
    if tickers_path.exists():
        for exchange_dir in tickers_path.glob("exchange=*"):
            for ticker_dir in exchange_dir.glob("ticker=*"):
                ticker = ticker_dir.name.split("=")[1]

                # Scan prices
                prices_path = ticker_dir / "prices"
                if prices_path.exists():
                    for f in prices_path.rglob("*.parquet"):
                        size = f.stat().st_size / (1024**2)
                        stats["tickers"][ticker]["prices"] += 1
                        stats["tickers"][ticker]["size_mb"] += size
                        total_files += 1
                        total_size += size

                # Scan ESG
                esg_path = ticker_dir / "esg"
                if esg_path.exists():
                    for f in esg_path.rglob("*.parquet"):
                        size = f.stat().st_size / (1024**2)
                        stats["tickers"][ticker]["esg"] += 1
                        stats["tickers"][ticker]["size_mb"] += size
                        total_files += 1
                        total_size += size

                # Scan results
                results_path = ticker_dir / "results"
                if results_path.exists():
                    for f in results_path.rglob("*.parquet"):
                        size = f.stat().st_size / (1024**2)
                        stats["tickers"][ticker]["results"] += 1
                        stats["tickers"][ticker]["size_mb"] += size
                        total_files += 1
                        total_size += size

    # Scan references
    ref_path = DATA_ROOT / "references"
    if ref_path.exists():
        for ticker_dir in ref_path.glob("ticker=*"):
            ticker = ticker_dir.name.split("=")[1]
            for f in ticker_dir.rglob("*.parquet"):
                size = f.stat().st_size / (1024**2)
                stats["references"][ticker]["prices"] += 1
                stats["references"][ticker]["size_mb"] += size
                total_files += 1
                total_size += size

    # Scan metadata
    metadata_path = DATA_ROOT / "metadata"
    if metadata_path.exists():
        for f in metadata_path.rglob("*.parquet"):
            size = f.stat().st_size / (1024**2)
            stats["metadata"]["files"] += 1
            stats["metadata"]["size_mb"] += size
            total_files += 1
            total_size += size

    # Scan membership
    membership_path = DATA_ROOT / "membership"
    if membership_path.exists():
        for f in membership_path.rglob("*.parquet"):
            size = f.stat().st_size / (1024**2)
            stats["membership"]["files"] += 1
            stats["membership"]["size_mb"] += size
            total_files += 1
            total_size += size

    # Scan curated results
    results_path = DATA_ROOT.parent / "results"
    if results_path.exists():
        for result_dir in results_path.iterdir():
            if result_dir.is_dir():
                result_type = result_dir.name
                for f in result_dir.rglob("*.parquet"):
                    size = f.stat().st_size / (1024**2)
                    stats["results"][result_type]["files"] += 1
                    stats["results"][result_type]["size_mb"] += size
                    total_files += 1
                    total_size += size

    print(f"✅ Scan complete: {total_files:,} files, {total_size:.2f} MB total")

    return stats, total_files, total_size


def create_summary_report(stats, total_files, total_size):
    """Generate text summary report"""
    print("\n" + "=" * 70)
    print("QUANTX DATA LAKE SUMMARY")
    print("=" * 70)
    print(f"\nTotal Files: {total_files:,}")
    print(f"Total Size:  {total_size:,.2f} MB ({total_size/1024:.2f} GB)")

    # Tickers summary
    num_tickers = len(stats["tickers"])
    print(f"\n📊 TICKERS: {num_tickers:,} tickers")
    if num_tickers > 0:
        total_ticker_files = sum(
            t["prices"] + t["esg"] + t["results"] for t in stats["tickers"].values()
        )
        total_ticker_size = sum(t["size_mb"] for t in stats["tickers"].values())
        print(f"   Files: {total_ticker_files:,}")
        print(f"   Size:  {total_ticker_size:,.2f} MB")

        # Count by data type
        tickers_with_prices = sum(
            1 for t in stats["tickers"].values() if t["prices"] > 0
        )
        tickers_with_esg = sum(1 for t in stats["tickers"].values() if t["esg"] > 0)
        tickers_with_results = sum(
            1 for t in stats["tickers"].values() if t["results"] > 0
        )
        print(f"   - With prices: {tickers_with_prices:,}")
        print(f"   - With ESG:    {tickers_with_esg:,}")
        print(f"   - With results: {tickers_with_results:,}")

    # References summary
    num_refs = len(stats["references"])
    if num_refs > 0:
        print(f"\n📈 REFERENCES: {num_refs} reference tickers")
        total_ref_size = sum(r["size_mb"] for r in stats["references"].values())
        print(f"   Size: {total_ref_size:.2f} MB")

    # Metadata
    if stats["metadata"]["files"] > 0:
        print(f"\n📋 METADATA: {stats['metadata']['files']} files")
        print(f"   Size: {stats['metadata']['size_mb']:.2f} MB")

    # Membership
    if stats["membership"]["files"] > 0:
        print(f"\n👥 MEMBERSHIP: {stats['membership']['files']} files")
        print(f"   Size: {stats['membership']['size_mb']:.2f} MB")

    # Results
    if stats["results"]:
        print(f"\n📊 RESULTS: {len(stats['results'])} result types")
        for result_type, data in sorted(stats["results"].items()):
            print(
                f"   {result_type:25s}: {data['files']:3d} files, {data['size_mb']:8.2f} MB"
            )

    print("\n" + "=" * 70)


def visualize_dataset_overview(stats, total_files, total_size):
    """Create comprehensive visualization dashboard"""
    fig = plt.figure(figsize=(18, 12))
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)

    # 1. Overall size breakdown by category
    ax1 = fig.add_subplot(gs[0, :2])
    categories = []
    sizes = []

    if stats["tickers"]:
        categories.append("Tickers")
        sizes.append(sum(t["size_mb"] for t in stats["tickers"].values()))

    if stats["references"]:
        categories.append("References")
        sizes.append(sum(r["size_mb"] for r in stats["references"].values()))

    if stats["metadata"]["size_mb"] > 0:
        categories.append("Metadata")
        sizes.append(stats["metadata"]["size_mb"])

    if stats["membership"]["size_mb"] > 0:
        categories.append("Membership")
        sizes.append(stats["membership"]["size_mb"])

    if stats["results"]:
        categories.append("Results")
        sizes.append(sum(r["size_mb"] for r in stats["results"].values()))

    colors = plt.cm.Set3(np.linspace(0, 1, len(categories)))
    ax1.pie(sizes, labels=categories, autopct="%1.1f%%", startangle=90, colors=colors)
    ax1.set_title(
        f"Data Lake Size Distribution ({total_size:.0f} MB total)",
        fontsize=14,
        fontweight="bold",
    )

    # 2. File count by category
    ax2 = fig.add_subplot(gs[0, 2])
    file_counts = []
    if stats["tickers"]:
        file_counts.append(
            sum(
                t["prices"] + t["esg"] + t["results"] for t in stats["tickers"].values()
            )
        )
    else:
        file_counts.append(0)

    if stats["references"]:
        file_counts.append(sum(r["prices"] for r in stats["references"].values()))
    else:
        file_counts.append(0)

    file_counts.append(stats["metadata"]["files"])
    file_counts.append(stats["membership"]["files"])

    if stats["results"]:
        file_counts.append(sum(r["files"] for r in stats["results"].values()))
    else:
        file_counts.append(0)

    cat_labels = ["Tickers", "References", "Metadata", "Membership", "Results"]
    ax2.barh(cat_labels, file_counts, color=colors)
    ax2.set_xlabel("File Count")
    ax2.set_title("Files by Category", fontsize=12, fontweight="bold")
    ax2.grid(alpha=0.3, axis="x")

    # 3. Top 20 tickers by size
    if stats["tickers"]:
        ax3 = fig.add_subplot(gs[1, :])
        ticker_sizes = [
            (ticker, data["size_mb"]) for ticker, data in stats["tickers"].items()
        ]
        ticker_sizes.sort(key=lambda x: x[1], reverse=True)
        top_20 = ticker_sizes[:20]

        tickers, sizes = zip(*top_20) if top_20 else ([], [])
        ax3.barh(
            range(len(tickers)), sizes, color="steelblue", edgecolor="black", alpha=0.7
        )
        ax3.set_yticks(range(len(tickers)))
        ax3.set_yticklabels(tickers)
        ax3.set_xlabel("Size (MB)", fontsize=11)
        ax3.set_title("Top 20 Tickers by Size", fontsize=13, fontweight="bold")
        ax3.invert_yaxis()
        ax3.grid(alpha=0.3, axis="x")

    # 4. Data type distribution for tickers
    if stats["tickers"]:
        ax4 = fig.add_subplot(gs[2, 0])
        data_types = ["Prices", "ESG", "Results"]
        type_counts = [
            sum(1 for t in stats["tickers"].values() if t["prices"] > 0),
            sum(1 for t in stats["tickers"].values() if t["esg"] > 0),
            sum(1 for t in stats["tickers"].values() if t["results"] > 0),
        ]
        ax4.bar(
            data_types,
            type_counts,
            color=["#1f77b4", "#2ca02c", "#ff7f0e"],
            edgecolor="black",
            alpha=0.7,
        )
        ax4.set_ylabel("Ticker Count")
        ax4.set_title("Data Type Coverage", fontsize=12, fontweight="bold")
        ax4.grid(alpha=0.3, axis="y")

    # 5. Size distribution histogram
    if stats["tickers"]:
        ax5 = fig.add_subplot(gs[2, 1])
        ticker_sizes_list = [
            data["size_mb"] for data in stats["tickers"].values() if data["size_mb"] > 0
        ]
        ax5.hist(
            ticker_sizes_list, bins=30, edgecolor="black", alpha=0.7, color="coral"
        )
        ax5.set_xlabel("Size (MB)")
        ax5.set_ylabel("Ticker Count")
        ax5.set_title("Ticker Size Distribution", fontsize=12, fontweight="bold")
        ax5.grid(alpha=0.3, axis="y")

    # 6. Results breakdown
    if stats["results"]:
        ax6 = fig.add_subplot(gs[2, 2])
        result_names = list(stats["results"].keys())
        result_sizes = [stats["results"][r]["size_mb"] for r in result_names]
        ax6.barh(
            result_names, result_sizes, color="purple", edgecolor="black", alpha=0.7
        )
        ax6.set_xlabel("Size (MB)")
        ax6.set_title("Results by Type", fontsize=12, fontweight="bold")
        ax6.invert_yaxis()
        ax6.grid(alpha=0.3, axis="x")

    fig.suptitle(
        "QuantX Data Lake - Dataset Size Analysis",
        fontsize=18,
        fontweight="bold",
        y=0.995,
    )

    plt.tight_layout()
    plt.show()

    print("✅ Visualization complete")


def analyze_data_completeness(stats):
    """Analyze data completeness across tickers"""
    if not stats["tickers"]:
        print("\n⚠️  No ticker data found for completeness analysis")
        return

    print("\n" + "=" * 70)
    print("DATA COMPLETENESS ANALYSIS")
    print("=" * 70)

    # Create completeness matrix
    tickers_list = sorted(stats["tickers"].keys())
    has_prices = [1 if stats["tickers"][t]["prices"] > 0 else 0 for t in tickers_list]
    has_esg = [1 if stats["tickers"][t]["esg"] > 0 else 0 for t in tickers_list]
    has_results = [1 if stats["tickers"][t]["results"] > 0 else 0 for t in tickers_list]

    # Calculate completeness categories
    complete = sum(
        1
        for i in range(len(tickers_list))
        if has_prices[i] and has_esg[i] and has_results[i]
    )
    partial = sum(
        1
        for i in range(len(tickers_list))
        if (has_prices[i] + has_esg[i] + has_results[i]) in [1, 2]
    )

    print(f"\nCompleteness Categories:")
    print(
        f"  Complete (all 3 types):  {complete:4d} tickers ({complete/len(tickers_list)*100:.1f}%)"
    )
    print(
        f"  Partial (1-2 types):     {partial:4d} tickers ({partial/len(tickers_list)*100:.1f}%)"
    )

    # Show common patterns
    print(f"\nCommon Data Patterns:")
    patterns = defaultdict(int)
    for i in range(len(tickers_list)):
        pattern = []
        if has_prices[i]:
            pattern.append("P")
        if has_esg[i]:
            pattern.append("E")
        if has_results[i]:
            pattern.append("R")
        patterns["+".join(pattern) if pattern else "None"] += 1

    for pattern, count in sorted(patterns.items(), key=lambda x: x[1], reverse=True)[
        :10
    ]:
        print(f"  {pattern:15s}: {count:4d} tickers")


def main():
    """Main execution function"""
    print("\n" + "🚀 QuantX Dataset Size Visualizer")
    print("=" * 70 + "\n")

    # Scan dataset
    stats, total_files, total_size = scan_dataset_structure()

    # Generate reports
    create_summary_report(stats, total_files, total_size)

    # Analyze completeness
    analyze_data_completeness(stats)

    # Create visualizations
    print("\n📊 Generating visualizations...")
    visualize_dataset_overview(stats, total_files, total_size)

    print("\n✅ Analysis complete!\n")


if __name__ == "__main__":
    main()
