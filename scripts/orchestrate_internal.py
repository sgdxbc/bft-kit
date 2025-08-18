from common import *
import load_config
import service_start
import service_stop
import download_logs


def task(service_hosts):
    load_config.task(service_hosts)
    processes = service_start.task(service_hosts)
    for host, proc in processes:
        if proc.wait() != 0:
            print(f"Service on {host} failed")
            service_stop.task(service_hosts)
            break
    download_logs.task(service_hosts, "/tmp/bftk-log")


if __name__ == "__main__":
    import clusters

    service_hosts = [item["host"] for item in clusters.service]
    task(service_hosts)
