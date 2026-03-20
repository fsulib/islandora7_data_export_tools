#!/usr/bin/env python3

# Imports
import datetime
import os
import subprocess

# Clear out data directory
subprocess.run(["rm -rf ../output/*"], shell=True)
subprocess.run(["mkdir ../output/root"], shell=True)
# Variables
logtime = datetime.datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
output_path = os.getcwd() + "/../output/root/"
collections_with_noncollection_children = []
collections_to_process = ["fsu:digital_library"]

# Functions
def log(message):
  formatted_message = "{} {}\n".format(datetime.datetime.now().strftime("%Y.%m.%d_%H.%M.%S"), message)
  print(formatted_message)
  logfile = open("{}{}.log".format(output_path, logtime), "a")
  logfile.write(formatted_message)

def get_collection_pid_from_path(collection_pid_path):
  return collection_pid_path.split("/")[-1]
  
def get_collection_directory_path(collection_pid_path):
  return output_path + collection_pid_path.replace(':', '_')

def get_collection_file_prefix(collection_pid_path):
  return get_collection_pid_from_path(collection_pid_path).replace(':', '_')

def write_file_to_collection_directory(collection_pid_path, filename, data):
  collection_directory = get_collection_directory_path(collection_pid_path)
  log("Writing file {} to {}.".format(filename, collection_directory))
  if not os.path.isdir(collection_directory):
    os.mkdir(collection_directory)
  file = open("{}/{}".format(collection_directory, filename), "a")
  file.write(data)

def get_collection_children_pids(collection_pid_path):
  child_data = {}
  collection_pid = collection_pid_path.split("/")[-1]
  child_data['collections'] = subprocess.run(["drush -u 1 islandora_datastream_crud_fetch_pids --content_model=islandora:collectionCModel --collection={}".format(collection_pid)], shell=True, capture_output=True, text=True).stdout.splitlines()
  child_data['noncollections'] = subprocess.run(["drush -u 1 islandora_datastream_crud_fetch_pids --without_cmodel=islandora:collectionCModel --collection={}".format(collection_pid)], shell=True, capture_output=True, text=True).stdout.splitlines()
  if child_data['noncollections']:
    log("{} contains noncollection children.".format(collection_pid_path))
    collections_with_noncollection_children.append(collection_pid_path)
  return child_data


def export_collection_data(collection_pid_path):
  collection_directory = get_collection_directory_path(collection_pid_path)
  collection_file_prefix = get_collection_file_prefix(collection_pid_path)
  collection_pid = get_collection_pid_from_path(collection_pid_path)

  log("Beginning export of {}.".format(collection_pid_path))
  write_file_to_collection_directory(collection_pid_path, "{}.pid".format(collection_file_prefix), collection_pid)

  collection_child_pids = get_collection_children_pids(collection_pid_path)
  if collection_child_pids['collections']:
    write_file_to_collection_directory(collection_pid_path, "{}.child-collections.pids".format(collection_file_prefix), "\n".join(collection_child_pids['collections']))
    for child_collection in collection_child_pids['collections']:
      collections_to_process.append("{}/{}".format(collection_pid_path, child_collection))
  if collection_child_pids['noncollections']:
    write_file_to_collection_directory(collection_pid_path, "{}.child-noncollections.pids".format(collection_file_prefix), "\n".join(collection_child_pids['noncollections']))

  subprocess.run(["drush -u 1 -y islandora_datastream_crud_fetch_datastreams --pid_file={0}/{1}.pid --dsid=RELS-EXT --datastreams_directory={0}".format(collection_directory, collection_file_prefix)], shell=True, capture_output=True, text=True).stdout.splitlines()

  subprocess.run(["drush -u 1 -y islandora_datastream_crud_fetch_datastreams --pid_file={0}/{1}.pid --dsid=DC --datastreams_directory={0}".format(collection_directory, collection_file_prefix)], shell=True, capture_output=True, text=True).stdout.splitlines()

  subprocess.run(["drush -u 1 -y islandora_datastream_crud_fetch_datastreams --pid_file={0}/{1}.pid --dsid=MODS --datastreams_directory={0}".format(collection_directory, collection_file_prefix)], shell=True, capture_output=True, text=True).stdout.splitlines()

  collections_to_process.remove(collection_pid_path)
  log("Finished export of {}.".format(collection_pid_path))
  if collections_to_process:
    export_collection_data(collections_to_process[0])


def write_collections_with_noncollection_children_list():
  if collections_with_noncollection_children:
    formatted_data =  "\n".join(collections_with_noncollection_children)
    log("The following collections with noncollection children were detected and saved to root/collections_with_noncollection_children.txt:\n{}".format(formatted_data))
    file = open("{}/collections_with_noncollection_children.txt".format(output_path), "a")
    file.write(formatted_data)
  else:
    log("No collections with noncollection children were detected.")


# Main
log("Beginning collection data export.")

export_collection_data('fsu:digital_library')


write_collections_with_noncollection_children_list()
log("Finished collection data export.")
