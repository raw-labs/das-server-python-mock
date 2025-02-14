#!/usr/bin/env python3
import sys
import os

def ensure_inits(directory):
    for root, dirs, files in os.walk(directory):
        init_path = os.path.join(root, '__init__.py')
        if not os.path.exists(init_path):
            # Create an empty __init__.py file
            open(init_path, 'a').close()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python add_init_files.py <directory>")
        sys.exit(1)
    target_dir = sys.argv[1]
    ensure_inits(target_dir)