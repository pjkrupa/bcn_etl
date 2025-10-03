import argparse

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", 
        "--packages",
        nargs='+',
        required=True, 
        help="Name of the Open Data BCN package listing the resources available."
        )
    parser.add_argument(
        '-d',
        '--directory',
        default='.',
        help='Root directory where you want to save the CSV files',
    )
    parser.add_argument(
        "--to_db",
        action="store_true",
        help="Set this flag if you want to automatically ingest the CSV files into a database after download"
    )
    return parser