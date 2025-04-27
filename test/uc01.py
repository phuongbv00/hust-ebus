import argparse

from dotenv import load_dotenv

from utils import test_job

load_dotenv()

parser = argparse.ArgumentParser(description="UC01")
parser.add_argument('--student-count', type=int, help='Student count')
parser.add_argument('--walk-max-distance', type=int, help='Walk max distance')
parser.add_argument('--coverage-ratio', type=float, help='Coverage ratio')

if __name__ == '__main__':
    args = parser.parse_args()
    filtered_args = {k: v for k, v in vars(args).items() if v is not None}
    test_job("uc01", **filtered_args)
