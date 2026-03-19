#!/usr/bin/env python3

# Imports
import subprocess

# Clear out data directory
subprocess.run(["rm -rf ../output/*"], shell=True)
subprocess.run(["mkdir ../output/root"], shell=True)

# Variables
root_collection_pid_path = "root/fsu:digital_library"

def get_collection_pid_from_collection_pid_path(collection_pid_path):
  return collection_pid_path.split("/")[-1]

def get_collection_children_pids(collection_pid):
  children_result = subprocess.run(["drush -u 1 islandora_datastream_crud_fetch_pids --collection={}".format(collection_pid)], shell=True, capture_output=True, text=True)
  return children_result.stdout.splitlines()

def process_collection(collection_pid_path):
  collection_pid = get_collection_pid_from_collection_pid_path(collection_pid_path)
  children_pids = get_collection_children_pids(collection_pid)
  print(children_pids)

process_collection(root_collection_pid_path)
