#!/usr/bin/env python3
import subprocess
import os

os.chdir(r'd:\work\snyu\watchlog_py')

# Stage all files
result = subprocess.run(['git', 'add', '-A'], capture_output=True, text=True)
print("git add -A:", result.returncode)
print(result.stdout)
print(result.stderr)

# Commit
result = subprocess.run(['git', 'commit', '-m', 'Initial commit'], capture_output=True, text=True)
print("\ngit commit:", result.returncode)
print(result.stdout)
print(result.stderr)

# Push
result = subprocess.run(['git', 'push', '-u', 'origin', 'master'], capture_output=True, text=True)
print("\ngit push:", result.returncode)
print(result.stdout)
print(result.stderr)
