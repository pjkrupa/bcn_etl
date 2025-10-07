import argparse

def get_parser():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "-p", 
        "--packages",
        nargs='+',
        help="Name of the Open Data BCN package listing the resources available."
        )
    group.add_argument(
        "-t", 
        "--tags",
        nargs='+',
        help="Tags to be searched for in the BCN catelog. All packages with a tag will be downloaded."
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