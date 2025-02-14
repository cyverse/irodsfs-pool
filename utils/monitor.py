#!/usr/bin/env python3

import subprocess
import sys
import time
import os
import re
from datetime import datetime, timedelta
from collections import deque

def follow(filename):
    current_inode = os.stat(filename).st_ino
    with open(filename, 'r') as file:
        file.seek(0, os.SEEK_END)
        while True:
            try:
                if os.stat(filename).st_ino != current_inode:
                    # File has been recreated, reopen it
                    file.close()
                    file = open(filename, 'r')
                    current_inode = os.stat(filename).st_ino
                    file.seek(0, os.SEEK_END)
                line = file.readline()
                if not line:
                    time.sleep(0.1)
                    continue
                yield line
            except FileNotFoundError:
                # File has been deleted, wait for it to be recreated
                time.sleep(1)
                continue

def parse_time(line):
    match = re.search(r'time="([\d-]+ [\d:.]+)"', line)
    if match:
        time_str = match.group(1)
        return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
    return None

def restart_irodsfs_pool():
    try:
        # Run the service restart command
        result = subprocess.run(['service', 'irodsfs-pool', 'restart'], 
                                check=True, 
                                stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE,
                                text=True)
        
        # Print the command output
        print("Service restart output:")
        print(result.stdout)
        
        # Check if the restart was successful
        if result.returncode == 0:
            print("irodsfs-pool service restarted successfully.")
        else:
            print("Failed to restart irodsfs-pool service.")
            print("Error:", result.stderr)
        
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while trying to restart the service: {e}")
        print("Error output:", e.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():
    log_file = "/irodsfs_pool_data/irodsfs_pool_svc.log.child"
    
    print(f"Monitoring {log_file} for login requests and responses...")
    
    requests = deque()
    
    for line in follow(log_file):
        parsed_time = parse_time(line)
        if not parsed_time:
            continue

        if "Login request" in line:
            requests.append(parsed_time)
            print(f"Request at {parsed_time}: {line.strip()}")
        
        elif "Login response" in line:
            requests.popleft()
            print(f"Response at {parsed_time}: {line.strip()}")

        # Check for requests without responses
        while requests and (parsed_time - requests[0]) > timedelta(seconds=10):
            alert_time = requests.popleft()
            print(f"ALERT: No login response received within 3 seconds of request at {alert_time}")
            print("Restarting irodsfs-pool service...")
            restart_irodsfs_pool()

if __name__ == "__main__":
    main()
