from universe.sp500_universe import SP500UniverseManager


def main():
    """Build S&P 500 membership datasets from raw historical data."""
    # Initialize builder with default paths
    builder = SP500UniverseManager(
        data_root="data/curated",
        raw_data_root="data/raw"
    )

    # Build membership data
    stats = builder.build_sp500_membership(min_date="2000-01-01")

    return stats

if __name__ == "__main__":
    main()
