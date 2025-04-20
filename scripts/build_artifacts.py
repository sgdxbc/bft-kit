def run():
    from invoke import run

    run(f"cargo build --release", echo=True)


if __name__ == "__main__":
    run()
