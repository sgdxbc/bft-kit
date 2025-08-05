from common import *
from time import sleep


def task(hosts):
    for host in hosts:
        try:
            ssh(host, f"pkill -INT bftk")
        except RuntimeError:
            pass
    sleep(1)
    for host in hosts:
        try:
            ssh(host, f"pkill bftk")
        except RuntimeError:
            pass


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.service])
