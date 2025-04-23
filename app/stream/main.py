from dotenv import load_dotenv

from stream.pipeline import uc02

load_dotenv()

if __name__ == '__main__':
    uc02.run()
