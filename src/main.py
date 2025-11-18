from universe.sp500_universe import SP500UniverseManager


def main():
    print("Please select: 1. Build S&P 500 membership datasets from raw historical data.")
    print("e: Exit")
    choice = input("Enter choice number: ")

    if choice == '1':
        """Build S&P 500 membership datasets from raw historical data."""
        # Initialize builder with default paths
        builder = SP500UniverseManager(
            data_root="data/curated",
            raw_data_root="data/raw"
        )

        # Build membership data
        stats = builder.build_sp500_membership(min_date="2000-01-01")
        return stats



def build_sp500_membership():
    """Build S&P 500 membership datasets from raw historical data."""
    # Initialize SP500Universe with default paths
    builder = SP500UniverseManager(
        data_root="data/curated",
        raw_data_root="data/raw"
    )

    # Build membership data
    stats = builder.build_sp500_membership(min_date="2000-01-01")
    return stats


if __name__ == "__main__":
    main()



if __name__ == "__main__":
    main()
