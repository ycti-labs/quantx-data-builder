import sys

from core.config import Config
from programs import sp500
from universe.sp500_universe import SP500Universe

config = Config("config/settings.yaml")

def main():
    menu_options = {
        '1': sp500.build_membership,
        '2': lambda: sp500.get_spy_historical_data(frequency="daily"),
        '3': lambda: sp500.get_spy_historical_data(frequency="weekly"),
        '4': lambda: sp500.get_spy_historical_data(frequency="monthly"),
        '5': lambda: sp500.build_historic_database(frequency="daily"),
        '6': lambda: sp500.build_historic_database(frequency="weekly"),
        '7': lambda: sp500.build_historic_database(frequency="monthly"),
        # '8': sp500.check_missing_data,
        # '9': fetch_universe_missing_data.demo_fetch_universe_with_corrections,
    }

    print("Please select: ")
    print("1. Build S&P 500 membership datasets from raw historical data.")
    print("2. Fetch S&P 500 Index (SPY) Historical Data (Daily)")
    print("3. Fetch S&P 500 Index (SPY) Historical Data (Weekly)")
    print("4. Fetch S&P 500 Index (SPY) Historical Data (Monthly)")
    print("5. Build S&P 500 Price database (Daily)")
    print("6. Build S&P 500 Price database (Weekly)")
    print("7. Build S&P 500 Price database (Monthly)")
    # print("8. Check Price missing data")
    # print("9. Fetch Price missing data with corrections")
    print("e: Exit")


    choice = input("Enter your choice: ").strip().lower()
    if choice == 'e':
        sys.exit(0)
    if choice in menu_options:
        menu_options[choice]()
    else:
        print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
