import os
from .functions import logger


def cosmo_1(data_folder, ftp_password, ftp_host="sftp.eawag.ch", ftp_user="cosmo"):
    log = logger("cosmo_1", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Processing COSMO 1 data")
    log.info("Ensure data folder exists.")
    folder = os.path.join(data_folder, "meteoswiss/cosmo1")
    if not os.path.exists(folder):
        os.makedirs(folder)
    log.info("Downloading data file...")
    with open(os.path.join(folder, "cosmo_file.txt"), "a") as file:
        file.write("COSMO data" + "\n")
