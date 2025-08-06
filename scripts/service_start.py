from common import *


def task(hosts):
    for index, host in enumerate(hosts):
        if not nfs or index == 0:
            local(f"rsync -a configs/*.conf {host}:{deploy_dir}/bftk-configs/")
        ssh(host, f"cd {deploy_dir} && TOKIO_WORKER_THREADS=6 ./bftk service {index}", detach=True)


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.service])
