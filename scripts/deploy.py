from common import *
from concurrent.futures import *
import remote_build
import clusters


def task(build_host, sync_hosts):
    remote_build.task(build_host)
    ssh(build_host, f"cp {build_dir}/target/release/bftk {deploy_dir}/")
    ssh(build_host, f"cp -rT {build_dir}/configs {deploy_dir}/bftk-configs")

    addr_conf = "\n".join(
        f"addr {item['ip']}:{service_port}" for item in clusters.service
    )
    write_file(
        build_host,
        f"{deploy_dir}/bftk-configs/addr.conf",
        addr_conf,
    )
    # TODO sync to other hosts


if __name__ == "__main__":
    task(clusters.service[0]["host"], [])
