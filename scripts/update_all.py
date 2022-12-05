# Updates every VM with a git pull. If the repo has been deleted, it will reclone
# Assumes the git repo has been cloned in the VM before (no popups for authentication)
# Run this in the scripts directory!

import argparse
import os
import selectors
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor


NUM_VM = 10


# 0 indexed


def run_wrapper(args: list):
    # https://stackoverflow.com/a/4417735
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    sel = selectors.DefaultSelector()
    sel.register(p.stdout, selectors.EVENT_READ)
    sel.register(p.stderr, selectors.EVENT_READ)

    while True:
        for key, _ in sel.select():
            data = key.fileobj.read1().decode()
            if not data:
                exit()
            if key.fileobj is p.stdout:
                print("STDOUT: " + data, end="")
            else:
                print("STDERR: " + data, end="", file=sys.stderr)
    # def execute(cmd):
    #     popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    #     for stdout_line in iter(popen.stdout.readline, ""):
    #         yield stdout_line
    #     popen.stdout.close()
    #     return_code = popen.wait()
    #     if return_code:
    #         raise subprocess.CalledProcessError(return_code, cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--netID",
        type=str,
        required=True,
        help="netID, will be checked for correctness",
    )
    parser.add_argument("-i", "--pub", type=str, default=None, help="Path to publickey")

    args = parser.parse_args()

    if args.netID != "tvitkin2" and args.netID != "zhuxuan2":
        print("Invalid netID specified! Terminating!")
        return 1

    curdir = os.getcwd()
    if not curdir.endswith("ece428_mp4/scripts"):
        print(
            f"curdir is: {curdir} . It is not in a directory with path ending with 'ece428_mp3/scripts'!"
        )
        return 1

    pubkey_arg = ""
    if args.pub is not None:
        pubkey_arg = f"-i ~/.ssh/{args.pub}"

    # Update all VMs
    with ThreadPoolExecutor(max_workers=NUM_VM) as executor:
        for i in range(1, NUM_VM + 1):
            vmurl = f"{args.netID}@fa22-cs425-25{i:02}.cs.illinois.edu"
            command = f"ssh {pubkey_arg} {vmurl} 'bash -s' < {curdir}/vm_ssh_update.sh {args.netID} {i}"
            executor.submit(run_wrapper, command)
            print("Submitted command: " + command)


if __name__ == "__main__":
    main()
