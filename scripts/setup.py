from common import *
from concurrent.futures import *
import clusters


def task(hosts):
    addr_conf = "\n".join(
        f"addr {item['ip']}:{service_port}" for item in clusters.service
    )

    for host in hosts:
        ssh(host, f"mkdir -p {deploy_dir}/bftk-configs")
        write_file(host, f"{deploy_dir}/bftk-configs/addr.conf", addr_conf)


if __name__ == "__main__":
    items = clusters.service if not nfs else [clusters.service[0]]
    task([item["host"] for item in items])
