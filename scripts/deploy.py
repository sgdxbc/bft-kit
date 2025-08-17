from common import *
from concurrent.futures import *
import remote_build
import clusters


def task(build_host, sync_hosts):
    remote_build.task(build_host)
    ssh(build_host, f"cp {build_dir}/target/release/bftk {deploy_dir}/")
    if not nfs:
        # TODO sync to other hosts
        pass


if __name__ == "__main__":
    task(clusters.service[0]["host"], [item["host"] for item in clusters.service[1:]])
