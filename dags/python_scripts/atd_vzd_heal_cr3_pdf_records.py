#!/usr/bin/env python

import os
import boto3
from typing import Optional, List
from string import Template
import datetime

PDF_MIME_COMMAND = Template("/usr/bin/file -b --mime $PDF_FILE")

S3_CLIENT = boto3.client('s3')


def is_crash_id(crash_id: int) -> bool:
    """
    Returns True if it is a valid integer
    :param int crash_id: The id to be tested
    :return bool:
    """
    return str(crash_id).isdigit()


def is_valid_metadata(metadata: dict) -> bool:
    if metadata.get("last_update", None) is not None \
       and metadata.get("file_size", None) is not None \
       and metadata.get("mime_type", None) is not None \
       and metadata.get("encoding", None) is not None:
        return True
    return False


def file_exists(crash_id: int) -> bool:
    """
    Returns True if the file exists, False otherwise
    :param int crash_id:
    :return bool:
    """
    try:
        if is_crash_id(crash_id):
            return os.path.isfile(f"./{crash_id}.pdf")
        else:
            return False
    except:
        return False


def download_file(crash_id: int) -> bool:
    """
    Downloads a CR3 PDF file into disk
    :param int crash_id: The crash id of the record to be downloaded
    :returns bool: True if it succeeds, false otherwise.
    """
    try:
        if is_crash_id(crash_id):
            S3_CLIENT.download_file('atd-vision-zero-editor', f"production/cris-cr3-files/{crash_id}.pdf", f"{crash_id}.pdf")
            return file_exists(crash_id)
        else:
            return False
    except:
        return False


def delete_file(crash_id: int) -> bool:
    """
    Deletes a CR3 PDF file from disk
    :param int crash_id: The crash id of the file to be deleted
    :returns bool:
    """
    if is_crash_id(crash_id) and file_exists(crash_id):
        os.remove(f"./{crash_id}.pdf")
        return file_exists(crash_id) is False
    else:
        return False


def get_mime_attributes(crash_id: int) -> dict:
    """
    Runs a shell command and returns a dictionary with mime attributes
    :returns dict:
    """
    # Make sure the id is an integer and that the file exists...
    if not is_crash_id(crash_id) or not file_exists(crash_id):
        return {}

    # Execute command to get mime attributes as accurately as possible:
    try:
        command = PDF_MIME_COMMAND.substitute(PDF_FILE=f"./{crash_id}.pdf")
        mime_attr_str = os.popen(command).read()
        mime_attr = mime_attr_str.replace(" charset=", "").strip().split(";", 2)

        return {
            "mime_type": mime_attr[0],
            "encoding": mime_attr[1]
        }
    except IndexError:
        return {}


def get_file_size(crash_id: int) -> int:
    """
    Gets the file size for crash_id
    :param int crash_id: The crash id of the file to be tested
    :return int:
    """
    if not is_crash_id(crash_id) or not file_exists(crash_id):
        return 0

    try:
        return os.path.getsize(f"./{crash_id}.pdf")
    except FileNotFoundError:
        return 0


def get_timestamp() -> str:
    """
    Returns a string containing the date and time the file was tested
    :return str:
    """
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_file_metadata(crash_id: int) -> dict:
    """
    Returns a dictionary containing the metadata for a file. It assumes the file already exists in disk.
    :param int crash_id:
    :return dict:
    """

    if not is_crash_id(crash_id) or not file_exists(crash_id):
        return {}

    timestamp = get_timestamp()
    file_size = get_file_size(crash_id)
    mime_attr = get_mime_attributes(crash_id)

    return {
        "last_update": timestamp,
        "file_size": file_size,
        "mime_type": mime_attr.get("mime_type", None),
        "encoding": mime_attr.get("encoding", None)
    }


def process_record(crash_id: int) -> bool:
    """
    Controls the process for a single file
    """
    if not is_crash_id(crash_id):
        return False

    # 1. Download file to disk
    if not download_file(crash_id):
        return False

    # 2. Generate file metadata
    metadata = get_file_metadata(crash_id)
    if not is_valid_metadata(metadata):
        return False

    # 3. Execute GraphQL with new metadata

    # 4. Delete the file from disk
    delete_file(crash_id)
    return True


def get_records(limit: int = 100) -> List[int]:
    """
    Returns a list of all CR3 crashes without CR3 PDF metadata
    :return List[int]:
    """
    # Executes GraphQL to fetch any crashes without CR3 PDF metadata, with a limit as indicated
    return []


def main():
    """
    Main Loop
    """
    # Get list of all records, with a limit
    # Processes each one of these crashes using process_record function
    pass
