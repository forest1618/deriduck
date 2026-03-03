import argparse
import sys
from src.schema import setup_database
from src.ingestion import update_db_data
from src.aggregates import make_aggregates

def main():
    parser = argparse.ArgumentParser(description="Deriduck!")
    subparsers = parser.add_subparsers(dest="command", help="Commands:")

    subparsers.add_parser("setup", help="Init DB, tables, views and macros")
    subparsers.add_parser("update", help="Download data and update aggregates")
    subparsers.add_parser("aggregate", help="Only update aggregates of available data")

    args = parser.parse_args()

    if args.command == "setup":
        setup_database()

    elif args.command == "update":
        update_db_data()
        make_aggregates()

    elif args.command == "aggregate":
        make_aggregates()

    else:
        parser.print_help()

if __name__ == "__main__":
    main()

