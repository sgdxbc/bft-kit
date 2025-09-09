from common import *


def task(hosts):
    processes = []
    for host in hosts:
        processes.append(local(f"rsync -a configs/*.conf {host}:{deploy_dir}/bftk-configs/", detach=True))
        if nfs:
            break
    for proc in processes:
        if proc.wait() != 0:
            raise RuntimeError("rsync failed")


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.service + clusters.workers])
