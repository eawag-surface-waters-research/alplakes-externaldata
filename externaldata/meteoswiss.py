import os
from .functions import logger


def cosmo_1(folder):
    log = logger("cosmo_1", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Downloading COSMO 1 data")
    with open(os.path.join(folder, "cosmo_file.txt"), "a") as file:
        file.write("COSMO data" + "\n")
