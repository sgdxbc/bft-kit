from subprocess import Popen, PIPE


service_port = 5000
build_dir = "/tmp/bftk"
deploy_dir = "/app"


def local(cmd):
    print(f"[local] {cmd}")
    process = Popen(cmd, shell=True)
    if process.wait() != 0:
        raise RuntimeError()


def ssh(host, cmd, detach=False):
    print(f"[ssh {host}] {cmd}")
    process = Popen(["ssh", host, cmd])
    if detach:
        return process
    if process.wait() != 0:
        raise RuntimeError()


def write_file(host, path, content):
    print(f"[write_file {host} {path}]")
    process = Popen(["ssh", host, "cat", ">", path], text=True, stdin=PIPE)
    process.communicate(input=content)
    if process.returncode != 0:
        raise RuntimeError()


try:
    from common_override import *
except ImportError:
    pass
