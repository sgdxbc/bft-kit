from common import *


def task(hosts):
    for host in hosts:
        try:
            ssh(host, f"pgrep bftk && pkill bftk")
        except RuntimeError:
            pass
        try:
            ssh(host, f"rm -r /tmp/big-storage*")
        except RuntimeError:
            pass


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.workers + clusters.service])
