from common import *
import load_config
import service_start
import service_stop
import workers
from time import sleep


def task(service_hosts, workload_hosts):
    load_config.task(service_hosts + workload_hosts)
    try:
        service_start.task(service_hosts)
        sleep(1)
        workers.task(workload_hosts)
    finally:
        service_stop.task(service_hosts)


if __name__ == "__main__":
    import clusters

    service_hosts = [item["host"] for item in clusters.service]
    workers_hosts = [item["host"] for item in clusters.workers]
    task(service_hosts, workers_hosts)
