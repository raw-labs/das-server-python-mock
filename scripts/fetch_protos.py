#!/usr/bin/env python3
import sys
import os
import yaml
import requests

def fetch_protos(config_yaml, directory):
    """
    Downloads .proto files from remote URLs into the subdirectory structure
    that begins at 'com/rawlabs/...'.
    """
    with open(config_yaml, 'r') as f:
        config = yaml.safe_load(f)

    proto_urls = config.get('proto_urls', [])
    if not proto_urls:
        print("No proto_urls found in config.yaml")
        return

    for url in proto_urls:
        # We only care about the path starting from 'com/rawlabs/...'
        marker = "com/rawlabs/"
        idx = url.find(marker)
        if idx == -1:
            print(f"ERROR: URL does not contain '{marker}': {url}")
            continue

        relative_path = url[idx:]  # e.g. "com/rawlabs/protocol/das/v1/types/types.proto"
        output_path = os.path.join(directory, relative_path)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        print(f"Downloading {url} -> {output_path}")
        resp = requests.get(url)
        resp.raise_for_status()

        with open(output_path, 'wb') as file:
            file.write(resp.content)

    print("All .proto files downloaded successfully.")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python fetch_protos.py <config.yaml> <directory>")
        sys.exit(1)
    config_yaml = sys.argv[1]
    target_dir = sys.argv[2]
    fetch_protos(config_yaml, target_dir)