#!/usr/bin/python3
import subprocess

status = subprocess.call(["cargo", "fmt", "--check", "--quiet"])
if status != 0:
    subprocess.check_call(["cargo", "fmt"])
    print("\x1b[102;1m Formatted. Please run git add\x1b[0m")
    exit(status)

subprocess.check_call(["cargo", "test"])
