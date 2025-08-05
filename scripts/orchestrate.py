from common import *
import service_start
import service_stop
import workload
from time import sleep


def task(service_hosts, workload_hosts):
    try:
        service_start.task(service_hosts)
        sleep(1)
        workload.task(workload_hosts)
    finally:
        service_stop.task(service_hosts)


if __name__ == "__main__":
    import clusters

    service_hosts = [item["host"] for item in clusters.service]
    workload_hosts = [item["host"] for item in clusters.workload]
    task(service_hosts, workload_hosts)
