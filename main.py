import argparse
from etl import init_db, full_load, incremental_load, validate_data

def main():
    parser = argparse.ArgumentParser(description="Sakila Data Sync CLI")
    
    # define subcommand
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    subparsers.add_parser('init', help='Initialize analytics database')
    subparsers.add_parser('full-load', help='Load all data from Source')
    subparsers.add_parser('incremental', help='Load changed data only')
    subparsers.add_parser('validate', help='Validate data consistency')
    
    args = parser.parse_args()
    
    if args.command == 'init':
        init_db()
    elif args.command == 'full-load':
        full_load()
    elif args.command == 'incremental':
        incremental_load()
    elif args.command == 'validate':
        validate_data()
    else:
        parser.print_help()

if __name__ == '__main__':
    main()