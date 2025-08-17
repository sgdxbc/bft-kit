from common import *


def task(hosts):
    for host in hosts:
        local(f"rsync -a configs/*.conf {host}:{deploy_dir}/bftk-configs/")
        if nfs:
            break


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.service + clusters.workload])
