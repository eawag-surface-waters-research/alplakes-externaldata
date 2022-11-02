import os
import time
import shutil
import pysftp
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from functions import logger, list_nested_dir


def csv_process(path, folder):
    name = os.path.basename(path)
    parts = name.split(".")[0].split("_")
    out = os.path.join(folder, parts[1], parts[2])
    if not os.path.exists(out):
        os.makedirs(out)
    df = pd.read_csv(path)
    df.index = pd.to_datetime(df["Time"])
    days = df.groupby([df.index.date]).sum(numeric_only=True).index
    start = "{}T00:00:00+00:00"
    end = "{}T23:59:00+00:00"
    for i in range(1, len(days)):
        df_d = df.loc[(df.index >= pd.Timestamp(start.format(str(days[i]))))
                      & (df.index < pd.Timestamp(end.format(str(days[i]))))]
        out_name = "{}_{}.csv".format(name.split(".")[0], str(days[i]))
        df_d.to_csv(os.path.join(out, out_name), index=False)


def totalinflowlakes_process(path, folder):
    types = ["C1E", "C1E_Ctrl", "C1E_Med", "C2E", "C2E_Ctrl", "C2E_Med", "ECMWF", "IFSENS", "NORAIN"]
    name = os.path.basename(path)
    out = False
    for t in types:
        if t in name:
            out = os.path.join(folder, name.split("_{}".format(t))[0], t)
    if out == False:
        print("Didn't recognise filename: {}".format(name))
        return
    if not os.path.exists(out):
        os.makedirs(out)
    skiprows = -1
    with open(path, 'rb') as f:
        for line in f:
            skiprows += 1
            if "dd mm yyyy hh" in str(line):
                break
    df = pd.read_csv(path, skiprows=skiprows, delim_whitespace=True)
    df.index = pd.to_datetime(dict(year=df.yyyy, month=df.mm, day=df.dd, hour=df.hh))
    days = df.groupby([df.index.date]).sum(numeric_only=True).index
    start = "{}T00:00:00"
    end = "{}T23:59:00"
    for i in range(1, len(days)):
        df_d = df.loc[(df.index >= pd.Timestamp(start.format(str(days[i]))))
                      & (df.index < pd.Timestamp(end.format(str(days[i]))))]
        out_name = "{}_{}.csv".format(name.split(".")[0], str(days[i]))
        df_d.to_csv(os.path.join(out, out_name), index=False)


def hydrodata(data_folder, ssh_key, ftp_host="ftp.hydrodata.ch", ftp_user="eawag"):
    """
    Download Bafu data from Bafu sftp server.
    """
    folders = [{"name": "CSV", "operation": "merge", "process": csv_process},
               {"name": "TotalInflowLakes", "operation": "merge", "process": totalinflowlakes_process},
               {"name": "pqprevi-official", "operation": "overwrite"},
               {"name": "pqprevi-unofficial", "operation": "overwrite"}]
    log = logger("cosmo", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download Hydrodata from Bafu sftp server")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "bafu/hydrodata")
    if not os.path.exists(parent):
        os.makedirs(parent)

    log.info("Connecting to {}".format(ftp_host))
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    conn = pysftp.Connection(host=ftp_host, username=ftp_user, private_key=ssh_key, cnopts=cnopts)
    log.info("Successfully connected to {}".format(ftp_host), indent=1)

    temp = os.path.join(parent, "temp")
    log.info("Downloading data to temporary directory: {}".format(temp), indent=1)
    if os.path.exists(temp):
        log.info("Temporary directory already exists, remove it.", indent=2)
        shutil.rmtree(temp)
    os.makedirs(temp)

    for folder in folders:
        log.info("Downloading {} to {}".format(folder["name"], temp), indent=2)
        start = time.process_time()
        conn.get_r(remotedir=folder["name"], localdir=temp)
        log.info("Downloaded {} in {} seconds.".format(folder["name"], round(time.process_time() - start)), indent=2)

    conn.close()
    log.info("Closing the connection to {}".format(ftp_host))

    log.info("Processing downloaded data.")
    failed = []
    for folder in folders:
        log.info("Processing data from {}".format(folder["name"]), indent=1)
        if folder["operation"] == "overwrite":
            log.info("Overwriting {} with new data.".format(os.path.join(parent, folder["name"])), indent=2)
            if os.path.exists(os.path.join(parent, folder["name"])):
                shutil.rmtree(os.path.join(parent, folder["name"]))
            shutil.move(os.path.join(temp, folder["name"]), os.path.join(parent, folder["name"]))
        elif folder["operation"] == "merge":
            files = list_nested_dir(os.path.join(temp, folder["name"]))
            log.info("Merging {} new files from {}.".format(len(files), folder["name"]), indent=2)
            for file in files:
                try:
                    folder["process"](file, os.path.join(parent, folder["name"]))
                except Exception as e:
                    failed.append(file)
                    log.error("Failed to process file: {}".format(file), e, indent=3)

    log.info("Removing temporary data.")
    shutil.rmtree(temp)

    if len(failed) > 0:
        raise ValueError("Failed to merge: {}".format(", ".join(failed)))
