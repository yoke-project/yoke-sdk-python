"""The Python plugin library's harness."""

from yoke_sdk import plugin


def declaration():
    return plugin.Declaration(id="")


def main():
    raise SystemExit(1)


if __name__ == "__main__":
    main()
