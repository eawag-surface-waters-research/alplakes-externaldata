import os
import sys
import pysftp
import argparse
from datetime import datetime, timedelta


def delete_old_files_sftp(hostname, username, password, file_types, root_path='/', days=7):
    print("Deleting files older than {} days in {} on {} with filetypes: {}"
          .format(days, root_path, hostname, ", ".join(file_types)))
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        delete_files_in_directory(sftp, root_path, file_types, days)


def delete_files_in_directory(sftp, path, file_types, days):
    for entry in sftp.listdir_attr(path):
        remote_filepath = f"{path}/{entry.filename}"
        if sftp.isdir(remote_filepath):
            delete_files_in_directory(sftp, remote_filepath, file_types, days)
        elif any(entry.filename.endswith(file_type) for file_type in file_types):
            if is_old_file(entry, days):
                print(f"Deleting: {remote_filepath}")
                sftp.remove(remote_filepath)


def is_old_file(file, days):
    file_age_days = (datetime.now() - datetime.fromtimestamp(file.st_mtime)).days
    return file_age_days > days


if __name__ == "__main__":
    if sys.version_info[0:2] != (3, 9):
        raise Exception('Requires python 3.9')
    parser = argparse.ArgumentParser()
    parser.add_argument('--hostname', '-s', help="Server hostname", type=str)
    parser.add_argument('--username', '-u', help="Server username", type=str)
    parser.add_argument('--password', '-p', help="Server password", type=str)
    parser.add_argument('--file_types', '-f', help="Comma separated string of filetypes e.g. .nc,.zip", type=str)
    parser.add_argument('--root_path', '-r', help="Root path on server", type=str, default="/")
    parser.add_argument('--days', '-d', help="Delete files older than this number of days", type=str, default=7)
    args = vars(parser.parse_args())
    file_types = args["file_types"].replace(" ", "").split(",")
    delete_old_files_sftp(args["hostname"], args["username"], args["password"], file_types, root_path=args["root_path"], days=args["days"])
