from common import *
import load_config


def task(hosts):
    assert not nfs
    load_config.task(hosts)
    processes = []
    for index, host in enumerate(hosts):
        processes.append(
            (host, ssh(host, f"cd {deploy_dir} && ./bftk preload {index}", detach=True))
        )
    for host, process in processes:
        if process.wait() != 0:
            print("Preload failed on", host)


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.service])
