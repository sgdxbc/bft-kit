#!/usr/bin/env python
def run(hide=False):
    from invoke import run

    return run(
        "find target/release/ -maxdepth 1 -type f -executable", echo=True, hide=hide
    ).stdout.splitlines()


if __name__ == "__main__":
    run()
