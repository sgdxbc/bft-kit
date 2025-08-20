from time import sleep
from common import *


def task(hosts):
    processes = []
    for host in hosts:
        p = ssh(host, f"cd {deploy_dir} && ./bftk workers", detach=True)
        processes.append(p)
    running = True
    while running:
        running = False
        for p in processes:
            if p.poll() is None:
                running = True
            elif p.returncode != 0:
                raise RuntimeError()
        sleep(0.1)


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.workers])
