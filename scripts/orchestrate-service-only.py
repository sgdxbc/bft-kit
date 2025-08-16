from common import *
import service_start
import service_stop
from time import sleep


def task(service_hosts):
    try:
        service_start.task(service_hosts)
        sleep(10)
    finally:
        service_stop.task(service_hosts)


if __name__ == "__main__":
    import clusters

    service_hosts = [item["host"] for item in clusters.service]
    task(service_hosts)
