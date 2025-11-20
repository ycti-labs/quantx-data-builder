import sys
from core.config import Config
from universe.sp500_universe import SP500Universe
from programs import sp500

config = Config("config/settings.yaml")

def main():
    menu_options = {
        '1': build_sp500_membership,
        '2': sp500.get_spy_historical_data,
        '3': sp500.build_sp500_database,
        # '4': fetch_missing_data.main,
        # '5': fetch_universe_missing_data.demo_fetch_universe_with_corrections,
    }
    
    print("Please select: ")
    print("1. Build S&P 500 membership datasets from raw historical data.")
    print("2. Fetch S&P 500 Index (SPY) Historical Data")
    print("3. Build Price database")
    print("4. Fetch Price missing data")
    print("5. Fetch Price missing data with corrections")
    print("e: Exit")


    choice = input("Enter your choice: ").strip().lower()
    if choice == 'e':
        sys.exit(0)
    if choice in menu_options:
        menu_options[choice]()
    else:
        print("Invalid choice. Please try again.")


def build_sp500_membership():
    """Build S&P 500 membership datasets from raw historical data."""
    sp500 = SP500Universe(
        data_root=config.get("storage.local.root_path"),
    )
    stats = sp500.build_membership(min_date=config.get('universes.sp500.start_date'))
    return stats

if __name__ == "__main__":
    main()
