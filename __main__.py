import sys

assert sys.version_info >= (3, 7), "Python version 3.7 or newer is required"

if __name__ == "__main__":
    import bead_cli.main
    bead_cli.main.main()
