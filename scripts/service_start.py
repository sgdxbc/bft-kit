from common import *


def task(hosts):
    for index, host in enumerate(hosts):
        ssh(host, f"cd {deploy_dir} && ./bftk service {index}", detach=True)


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.service])
