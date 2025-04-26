import argparse

from dotenv import load_dotenv

from utils import test_job

load_dotenv()

parser = argparse.ArgumentParser(description="Bootstrap")
parser.add_argument('--student-count', type=int, help='Student count')
parser.add_argument('--bus-count', type=int, help='Bus count')

if __name__ == '__main__':
    args = parser.parse_args()
    test_job("bootstrap", **vars(args))
