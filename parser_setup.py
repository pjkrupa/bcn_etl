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
        required=False,
        help='Directory where you want to save the CSV files',
    )
    return parser