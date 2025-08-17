from common import *
import service_start
import service_stop
from time import sleep


def task(service_hosts):
    processes = service_start.task(service_hosts)
    for host, proc in processes:
        if proc.wait() != 0:
            print(f"Service on {host} failed")
            service_stop.task(service_hosts)
            break


if __name__ == "__main__":
    import clusters

    service_hosts = [item["host"] for item in clusters.service]
    task(service_hosts)
