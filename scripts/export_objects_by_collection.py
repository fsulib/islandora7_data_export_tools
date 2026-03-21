#!/usr/bin/env python3


# Imports
import datetime
import os
import subprocess
import sys


# Variables
collection_pid_path = sys.argv[1]
output_path = os.getcwd() + "/../output/root/"
logtime = datetime.datetime.now().strftime("%Y.%m.%d_%H.%M.%S")


# Functions
def log(message, collection_pid_path):
    formatted_message = "{} {}\n{}\n".format(
        datetime.datetime.now().strftime("%Y.%m.%d_%H.%M.%S"),
        collection_pid_path,
        message,
    )
    print(formatted_message)
    collection_directory_path = get_collection_directory_path(collection_pid_path)
    collection_file_prefix = get_collection_file_prefix(collection_pid_path)
    logfile = open(
        "{}/{}_{}.log".format(
            collection_directory_path, collection_file_prefix, logtime
        ),
        "a",
    )
    logfile.write(formatted_message)


def get_collection_pid_from_path(collection_pid_path):
    return collection_pid_path.split("/")[-1]


def get_collection_directory_path(collection_pid_path):
    return output_path + collection_pid_path.replace(":", "_")


def get_collection_file_prefix(collection_pid_path):
    return get_collection_pid_from_path(collection_pid_path).replace(":", "_")


def read_noncollection_pidfile(collection_pid_path):
    collection_directory_path = get_collection_directory_path(collection_pid_path)
    collection_file_prefix = get_collection_file_prefix(collection_pid_path)
    noncollection_pids = []
    with open(
        "{}/{}.child-noncollections.pids".format(
            collection_directory_path, collection_file_prefix
        )
    ) as child_noncollection_pids_file:
        for line in child_noncollection_pids_file:
            noncollection_pids.append(line)
    return noncollection_pids


def fetch_noncollection_object_datastreams(noncollection_pid):
    cmodel = subprocess.run(
        [
            "drush -u 1 php-eval '$obj = islandora_object_load(\"{}\"); print_r($obj->models[0]);'".format(
                noncollection_pid
            )
        ],
        shell=True,
        capture_output=True,
        text=True,
    ).stdout
    hierarchichal_cmodels = [
        "islandora:compoundCModel",
        "islandora:bookCModel",
        "islandora:pageCModel",
        "islandora:newspaperCModel",
        "islandora:newspaperIssueCModel",
        "islandora:newspaperPageCModel",
    ]
    if object_data["cmodel"] in hierarchichal_cmodels:
        print(
            'hierarchichal cmodel "{}" detected on {}.'.format(
                object_data["cmodel"], noncollection_pid
            )
        )
    universal_datastreams = ["MODS", "DC", "RELS-EXT"]
    cmodel_asset_datastreams = {
        "islandora:sp_pdf": "OBJ",
        "ir:thesisCModel": "PDF",
        "ir:citationCModel": "PDF",
        "islandora:sp_basic_image": "OBJ",
        "islandora:sp_large_image_cmodel": "OBJ",
        "islandora:sp-audioCModel": "OBJ",
        "islandora:sp_videoCModel": "OBJ",
        "islandora:pageCModel": "OBJ",
        "islandora:newspaperPageCModel": "OBJ",
        "islandora:binaryObjectCModel": "OBJ",
        "islandora:3dModelCModel": "DSMAP",  # Plus MF*
    }
    return ""


# Main
noncollection_pids = read_noncollection_pidfile(collection_pid_path)
print(noncollection_pids)
for pid in noncollection_pids:
    object_data = get_noncollection_object_data(pid)
    log("{} is a {}".format(pid, object_data["cmodel"]), collection_pid_path)
