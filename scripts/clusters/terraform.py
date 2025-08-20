from subprocess import run
from json import loads


def run_terraform(name):
    proc = run(
        f"terraform -chdir=scripts/terraform output -json {name}_instances",
        shell=True,
        capture_output=True,
        text=True,
        check=True,
    )
    instances = loads(proc.stdout)
    return [
        {
            "host": instance["public_dns"],
            "ip": instance["private_ip"],
        }
        for instance in instances
    ]


service = run_terraform("service")
workers = run_terraform("workload")


if __name__ == "__main__":
    from pprint import pprint as print

    print(service)
    print(workers)
