from fabric import *
from invoke import *


BUILD_DIR = "/tmp/bk"
DST_DIR = "/homes/cowsay"


def run_task(c):
    run("cargo build -r --bin bk", echo=True)  # sanity check
    run("tar -czf target/archive.tar.gz src/ benches/ Cargo.toml Cargo.lock", echo=True)
    c.run(f"mkdir -p {BUILD_DIR}", echo=True)
    c.put("target/archive.tar.gz", f"{BUILD_DIR}/archive.tar.gz")
    c.run(f"tar -xzf {BUILD_DIR}/archive.tar.gz -C {BUILD_DIR}", echo=True)
    c.run(f"cd {BUILD_DIR} && /bin/bash -l -c 'cargo build -r --bin bk'", echo=True)
    c.run(f"cp {BUILD_DIR}/target/release/bk {DST_DIR}/", echo=True)


if __name__ == "__main__":
    with Connection("nsl-node7") as c:
        run_task(c)
