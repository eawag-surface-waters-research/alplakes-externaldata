import os
import pysftp
import fnmatch
from .functions import logger, unzip_combine


def cosmo(data_folder, ftp_password, ftp_host="sftp.eawag.ch", ftp_port=22, ftp_user="cosmo"):
    """
    Download COSMO data from Eawag sftp server.
    Available files:
    - VNXQ94.%Y%m%d0000.nc (forecast): Cosmo-1e 33 hour ensemble forecast
    - VNXZ32.%Y%m%d0000.zip (forecast): Cosmo-2e 5 day ensemble forecast
    - VNXQ34.%Y%m%d0000.nc (reanalysis): Cosmo-1e 1 day deterministic (data from previous day from name)
    - VNJK21.%Y%m%d0000.nc (reanalysis): Cosmo-1e 1 day ensemble forecast (data from previous day from name)
    """
    files = [{"name": "VNXQ94.*0000.nc", "parent": "data/forecast", "folder": "VNXQ94"},
             {"name": "VNXZ32.*0000.zip", "parent": "data/forecast", "folder": "VNXZ32"},
             {"name": "VNXQ34.*0000.nc", "parent": "data/reanalysis", "folder": "VNXQ34"},
             {"name": "VNJK21.*0000.nc", "parent": "data/reanalysis", "folder": "VNJK21"}]

    failed = []

    log = logger("cosmo", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download COSMO data from Eawag sftp server")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "meteoswiss/cosmo")
    if not os.path.exists(parent):
        os.makedirs(parent)

    log.info("Connecting to {}".format(ftp_host))
    conn = pysftp.Connection(host=ftp_host, port=ftp_port, username=ftp_user, password=ftp_password)
    log.info("Successfully connected to {}".format(ftp_host), indent=1)

    for file in files:
        log.info("Downloading {} files".format(file["folder"]))
        folder = os.path.join(data_folder, "meteoswiss/cosmo", file["folder"])
        if not os.path.exists(folder):
            os.makedirs(folder)
        server_files = [f for f in conn.listdir(file["parent"]) if fnmatch.fnmatch(f, file["name"])]
        log.info("Found {} files matching file name pattern {}".format(len(server_files), file["name"]), indent=1)
        for server_file in server_files:
            if os.path.isfile(os.path.join(parent, file["folder"], server_file.replace(".zip", ".nc"))):
                log.info("File {} already downloaded, skipping.".format(server_file), indent=2)
            elif os.path.isfile(os.path.join(parent, file["folder"], server_file.replace(".nc", ".zip"))):
                log.info("File {} already downloaded, unzipping.".format(server_file), indent=2)
                unzip_combine(os.path.join(parent, file["folder"], server_file))
            else:
                log.info("Downloading file {}.".format(server_file), indent=2)
                try:
                    conn.get(os.path.join(file["parent"], server_file), os.path.join(parent, file["folder"], server_file))
                    if ".zip" in server_file:
                        unzip_combine(os.path.join(parent, file["folder"], server_file))
                except:
                    log.error("Failed to download {}.".format(server_file))
                    if os.path.exists(os.path.join(parent, file["folder"], server_file)):
                        os.unlink(os.path.join(parent, file["folder"], server_file))
                    failed.append(server_file)

    log.info("Closing connection to {}".format(ftp_host))
    conn.close()

    if len(failed) > 0:
        raise ValueError("Failed to download: {}".format(failed))
