import sys

from src import pipereg

if __name__ == "__main__":
    pipereg.get_step(sys.argv[1]).run()
