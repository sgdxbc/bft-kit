from common import *


def task(hosts, log_path):
    # local("rm -r logs; mkdir logs")
    local("mkdir -p logs")
    with open("logs/.gitignore", "w") as f:
        f.write("*")
    for host in hosts:
        try:
            local(f"rsync {host}:{log_path} logs/{host}.log")
        except RuntimeError:
            print(f"Failed to download logs from {host}")


if __name__ == "__main__":
    import clusters
    from sys import argv

    task([item["host"] for item in clusters.workers + clusters.service], argv[1])
