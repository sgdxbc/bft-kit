from common import *


def task(hosts):
    tasks = []
    for index, host in enumerate(hosts):
        proc = ssh(host, f"cd {deploy_dir} && ./bftk replica {index}", detach=True)
        tasks.append((host, proc))
    return tasks


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.service])
