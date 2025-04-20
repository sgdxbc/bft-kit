#!/usr/bin/env python
from sys import argv
import list_artifacts


def run():
    from invoke import run

    artifacts = " ".join(list_artifacts.run(hide=True))
    run(f"cp {artifacts} {argv[1]}", echo=True)


if __name__ == "__main__":
    import build_artifacts

    build_artifacts.run()
    run()
