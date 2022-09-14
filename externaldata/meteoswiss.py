from .functions import logger


def cosmo_1():
    log = logger("cosmo_1")
    log.initialise("Downloading COSMO 1 data")
    with open("data/cosmo_file.txt", "a") as file:
        file.write("COSMO data" + "\n")
