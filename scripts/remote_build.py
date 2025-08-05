from common import *


def task(host):
    local("cargo build -r --bin bftk")  # sanity check
    ssh(host, f"mkdir -p {build_dir}")
    local(f"rsync -aR src/ benches/ Cargo.toml Cargo.lock {host}:{build_dir}/")
    ssh(host, f"cd {build_dir} && /bin/bash -l -c 'cargo build -r --bin bftk'")


if __name__ == "__main__":
    import clusters

    task(clusters.service[0]["host"])
