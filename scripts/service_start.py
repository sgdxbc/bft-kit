from common import *


def task(hosts):
    tasks = []
    for index, host in enumerate(hosts):
        if not nfs or index == 0:
            local(f"rsync -a configs/*.conf {host}:{deploy_dir}/bftk-configs/")
        tasks.append(
            ssh(
                host,
                f"cd {deploy_dir} && TOKIO_WORKER_THREADS=6 ./bftk service {index}",
                detach=True,
            )
        )
    return tasks


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.service])
