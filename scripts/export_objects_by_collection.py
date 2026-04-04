#!/usr/bin/env python3

# Imports
import datetime
import json
import os
import subprocess
import sys


if len(sys.argv) < 2:
    sys.exit("No collection argument provided. Exiting.")


# Variables
collection_pid_path = sys.argv[1]
output_path = os.getcwd() + "/../output/root/"
logtime = datetime.datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
objects_with_embargoes = {}


# Functions
def log(message, collection_pid_path):
    formatted_message = "{} {}\n".format(
        datetime.datetime.now().strftime("%Y.%m.%d_%H.%M.%S"),
        message,
    )
    print(formatted_message)
    collection_directory_path = get_pid_directory_path(collection_pid_path)
    collection_file_prefix = get_pid_file_prefix(collection_pid_path)
    logfile = open(
        "{}/{}_{}.log".format(
            collection_directory_path, collection_file_prefix, logtime
        ),
        "a",
    )
    logfile.write(formatted_message)


def get_pid_from_path(pid_path):
    return pid_path.split("/")[-1]


def get_pid_directory_path(pid_path):
    return output_path + pid_path.replace(":", "_")


def get_pid_file_prefix(pid_path):
    return get_pid_from_path(pid_path).replace(":", "_")


def read_noncollection_pidfile(collection_pid_path):
    collection_directory_path = get_pid_directory_path(collection_pid_path)
    collection_file_prefix = get_pid_file_prefix(collection_pid_path)
    noncollection_pids = []
    with open(
        "{}/../{}.child-noncollections.pids".format(
            collection_directory_path, collection_file_prefix
        )
    ) as child_noncollection_pids_file:
        for line in child_noncollection_pids_file:
            noncollection_pids.append(line.strip())
    return noncollection_pids


def get_noncollection_object_datastreams(pid_path):
    datastreams = ["RELS-EXT", "MODS", "DC", "POLICY", "OBJ", "PDF"]
    # metadata_datastreams = ["RELS-EXT", "MODS", "DC", "POLICY"]
    # asset_datastreams = ["OBJ", "PDF"]
    log(
        "Beginning retrieval of the following datastreams for children of {}: {}.".format(
            pid_path, ", ".join(datastreams)
        ),
        collection_pid_path,
    )
    collection_directory = get_pid_directory_path(pid_path)
    collection_file_prefix = get_pid_file_prefix(pid_path)
    for datastream in datastreams:
        subprocess.run(
            [
                "drush -u 1 -y islandora_datastream_crud_fetch_datastreams --pid_file={0}/../{1}.child-noncollections.pids --dsid={2} --datastreams_directory={0}/".format(
                    collection_directory, collection_file_prefix, datastream
                )
            ],
            shell=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
    log(
        "Retrieval of datastreams for children of {} complete.".format(pid_path),
        collection_pid_path,
    )


def get_noncollection_object_embargoes(noncollection_pid):
    log(
        "Beginning retrieval of embargo data for {}".format(noncollection_pid),
        collection_pid_path,
    )
    embargoes = []

    # IP embargo check
    ip_embargo_check_result = subprocess.run(
        [
            "drush -u 1 sqlq \"select pid from islandora_ip_embargo_embargoes where pid = '{}'\"".format(
                noncollection_pid
            )
        ],
        shell=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    if ip_embargo_check_result[0] == noncollection_pid:
        log("IP embargo detected on {}.".format(noncollection_pid), collection_pid_path)
        ip_embargo_data = {"type": "IP", "expiry": "indefinite"}
        embargoes.append(ip_embargo_data)

    # Scholar embargo check
    scholar_embargo_check_result = subprocess.run(
        [
            "drush -u 1 php-eval \"\$object = islandora_object_load('{}'); \$embargoes = islandora_scholar_embargo_get_embargoed(\$object); echo json_encode(\$embargoes);\"".format(
                noncollection_pid
            )
        ],
        shell=True,
        capture_output=True,
        text=True,
    ).stdout
    scholar_embargo_raw_data = json.loads(scholar_embargo_check_result)
    if scholar_embargo_raw_data:
        log(
            "Scholar embargo detected on {}.".format(noncollection_pid),
            collection_pid_path,
        )
        for scholar_embargo in scholar_embargo_raw_data:
            scholar_embargo_slice = scholar_embargo["obj"]["value"].split("/")[-1]
            if scholar_embargo_slice == noncollection_pid:
                scholar_embargo_type = "object"
            else:
                scholar_embargo_type = "{} datastream".format(scholar_embargo_slice)
            scholar_embargo_expiry = scholar_embargo["date"]["value"].split("T")[0]
            scholar_embargo_data = {
                "type": scholar_embargo_type,
                "expiry": scholar_embargo_expiry,
            }
            embargoes.append(scholar_embargo_data)

    if embargoes:
        objects_with_embargoes[noncollection_pid] = embargoes
    log(
        "Retrieval of embargo data for {} complete.".format(noncollection_pid),
        collection_pid_path,
    )
    return embargoes


def get_noncollection_object_data(noncollection_pid):
    log(
        "Beginning extraction of data for {}".format(noncollection_pid),
        collection_pid_path,
    )
    object_data = {}
    object_data["pid"] = noncollection_pid
    object_data["embargoes"] = get_noncollection_object_embargoes(noncollection_pid)
    object_data["cmodel"] = subprocess.run(
        [
            "drush -u 1 php-eval '$obj = islandora_object_load(\"{}\"); print_r($obj->models[0]);'".format(
                noncollection_pid
            )
        ],
        shell=True,
        capture_output=True,
        text=True,
    ).stdout
    log(
        "Extraction of data for {} complete.".format(noncollection_pid),
        collection_pid_path,
    )
    return object_data


def write_file_to_pid_directory(pid_path, filename, data):
    pid_directory = get_pid_directory_path(pid_path)
    log("Writing file {} to {}".format(filename, pid_directory), collection_pid_path)
    file = open("{}/{}".format(pid_directory, filename), "a")
    file.write(data)


def write_collection_embargo_report(collection_pid_path, objects_with_embargoes):
    collection_pid = get_pid_from_path(collection_pid_path)
    collection_filename_prefix = get_pid_file_prefix(collection_pid)
    objects_with_embargoes_list = []
    for pid in objects_with_embargoes:
        for embargo in objects_with_embargoes[pid]:
            if embargo["type"] == "IP":
                csv_formatted_embargo = "{}, {}, {}".format(pid, "IP", "indefinite")
            else:
                csv_formatted_embargo = "{}, {}, {}".format(
                    pid, embargo["type"], embargo["expiry"]
                )
            objects_with_embargoes_list.append(csv_formatted_embargo)
    objects_with_embargoes_csv = "\n".join(objects_with_embargoes_list)
    write_file_to_pid_directory(
        collection_pid_path + "/..",
        collection_filename_prefix + ".child-embargoes.csv",
        objects_with_embargoes_csv,
    )


def write_object_embargo_report(object_pid_path, embargoes):
    object_pid = get_pid_from_path(object_pid_path)
    object_filename_prefix = get_pid_file_prefix(object_pid)
    embargoes_list = []
    for embargo in embargoes:
        if embargo["type"] == "IP":
            csv_formatted_embargo = "{}, {}".format("IP", "indefinite")
        else:
            csv_formatted_embargo = "{}, {}".format(embargo["type"], embargo["expiry"])
        embargoes_list.append(csv_formatted_embargo)
    embargoes_csv = "\n".join(embargoes_list)
    write_file_to_pid_directory(
        collection_pid_path, object_filename_prefix + ".embargoes.csv", embargoes_csv
    )


def process_hierarchichal_object(collection_pid_path, object_data):
    if object_data["cmodel"] == "islandora:compoundCModel":
        process_compound_object(collection_pid_path, object_data)
    elif object_data["cmodel"] == "islandora:bookCModel":
        process_book_object(collection_pid_path, object_data)
    elif object_data["cmodel"] == "islandora:newspaperCModel":
        process_newspaper_object(collection_pid_path, object_data)
    else:
        log(
            "Error processing hierarchichal object {} with cmodel {}".format(
                object_data["pid"], object_data["cmodel"]
            ),
            collection_pid_path,
        )


def process_compound_object(collection_pid_path, object_data):
    log(
        "Compound object detected, beginning processing of {}.".format(
            object_data["pid"]
        ),
        collection_pid_path,
    )
    compound_directory = "{}/{}/{}".format(
        output_path, collection_pid_path, object_data["pid"]
    ).replace(":", "_")
    if not os.path.isdir(compound_directory):
        os.mkdir(compound_directory)
    compound_children = subprocess.run(
        [
            "drush -u 1 -y islandora_datastream_crud_fetch_pids --solr_query='RELS_EXT_isConstituentOf_uri_s:info\:fedora/{}'".format(
                object_data["pid"].replace(":", "\\:")
            )
        ],
        shell=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    write_file_to_pid_directory(
        "{}".format(collection_pid_path),
        "{}.child-noncollections.pids".format(object_data["pid"].replace(":", "_")),
        "\n".join(compound_children),
    )
    collection_files = os.listdir(compound_directory + "/..")
    for pid in compound_children:
        for file in collection_files:
            if file.startswith(pid.replace(":", "_")):
                os.replace(
                    compound_directory + "/../" + file, compound_directory + "/" + file
                )
    log(
        "Compound object processing of {} complete.".format(object_data["pid"]),
        collection_pid_path,
    )


def process_book_object(collection_pid_path, object_data):
    log(
        "Book object detected, beginning processing of {}.".format(object_data["pid"]),
        collection_pid_path,
    )
    book_directory = "{}/{}/{}".format(
        output_path, collection_pid_path, object_data["pid"]
    ).replace(":", "_")
    if not os.path.isdir(book_directory):
        os.mkdir(book_directory)
    book_children = subprocess.run(
        [
            "drush -u 1 -y islandora_datastream_crud_fetch_pids --is_member_of={}".format(
                object_data["pid"]
            )
        ],
        shell=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    write_file_to_pid_directory(
        "{}".format(collection_pid_path),
        "{}.child-noncollections.pids".format(object_data["pid"].replace(":", "_")),
        "\n".join(book_children),
    )
    get_noncollection_object_datastreams(collection_pid_path + "/" + object_data["pid"])
    log(
        "Book object processing of {} complete.".format(object_data["pid"]),
        collection_pid_path,
    )


def process_newspaper_object(collection_pid_path, object_data):
    log(
        "Newspaper object detected, beginning processing of {}.".format(
            object_data["pid"]
        ),
        collection_pid_path,
    )
    newspaper_directory = "{}/{}/{}".format(
        output_path, collection_pid_path, object_data["pid"]
    ).replace(":", "_")
    if not os.path.isdir(newspaper_directory):
        os.mkdir(newspaper_directory)
    newspaper_children = subprocess.run(
        [
            "drush -u 1 -y islandora_datastream_crud_fetch_pids --is_member_of={}".format(
                object_data["pid"]
            )
        ],
        shell=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    write_file_to_pid_directory(
        "{}".format(collection_pid_path),
        "{}.child-noncollections.pids".format(object_data["pid"].replace(":", "_")),
        "\n".join(newspaper_children),
    )
    get_noncollection_object_datastreams(collection_pid_path + "/" + object_data["pid"])
    for issue in newspaper_children:
        issue_data = get_noncollection_object_data(issue)
        process_newspaper_issue_object(collection_pid_path, object_data, issue_data)
    log(
        "Newspaper object processing of {} complete.".format(object_data["pid"]),
        collection_pid_path,
    )


def process_newspaper_issue_object(collection_pid_path, newspaper_data, issue_data):
    log(
        "Newspaper issue object detected, beginning processing of {}.".format(
            issue_data["pid"]
        ),
        collection_pid_path,
    )
    newspaper_issue_directory = "{}/{}/{}/{}".format(
        output_path, collection_pid_path, newspaper_data["pid"], issue_data["pid"]
    ).replace(":", "_")
    if not os.path.isdir(newspaper_issue_directory):
        os.mkdir(newspaper_issue_directory)
    newspaper_issue_children = subprocess.run(
        [
            "drush -u 1 -y islandora_datastream_crud_fetch_pids --is_member_of={}".format(
                issue_data["pid"]
            )
        ],
        shell=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    write_file_to_pid_directory(
        "{}/{}".format(collection_pid_path, newspaper_data["pid"]),
        "{}.child-noncollections.pids".format(issue_data["pid"].replace(":", "_")),
        "\n".join(newspaper_issue_children),
    )
    get_noncollection_object_datastreams(
        "{}/{}/{}".format(collection_pid_path, newspaper_data["pid"], issue_data["pid"])
    )
    log(
        "Newspaper issue object processing of {} complete.".format(object_data["pid"]),
        collection_pid_path,
    )


# Main
collection_pid = get_pid_from_path(collection_pid_path)
log("Beginning export of {}.".format(collection_pid), collection_pid_path)
get_noncollection_object_datastreams(collection_pid_path)
noncollection_pids = read_noncollection_pidfile(collection_pid_path)
for pid in noncollection_pids:
    object_data = get_noncollection_object_data(pid)
    object_filename_prefix = get_pid_file_prefix(object_data["pid"])
    if object_data["embargoes"]:
        write_object_embargo_report(
            collection_pid_path + "/" + pid, object_data["embargoes"]
        )

    if object_data["cmodel"] in [
        "islandora:compoundCModel",
        "islandora:bookCModel",
        "islandora:newspaperCModel",
    ]:
        process_hierarchichal_object(collection_pid_path, object_data)
write_collection_embargo_report(collection_pid_path, objects_with_embargoes)
log("Finished with export of {}.".format(collection_pid), collection_pid_path)
