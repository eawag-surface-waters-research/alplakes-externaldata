# -*- coding: utf-8 -*-
import sys
import argparse
from meteoswiss import cosmo, icon, meteodata
from bafu import hydrodata
from arso import arso_meteodata
from geosphere import geosphere_meteodata


def main(params):
    setups = ["meteoswiss_cosmo", "bafu_hydrodata", "meteoswiss_meteodata", "meteoswiss_icon", "arso_meteodata"]
    if params["source"] == "meteoswiss_cosmo":
        cosmo(params["filesystem"], params["password"])
    elif params["source"] == "meteoswiss_icon":
        icon(params["filesystem"], params["password"])
    elif params["source"] == "meteoswiss_meteodata":
        meteodata(params["filesystem"], params["password"])
    elif params["source"] == "bafu_hydrodata":
        hydrodata(params["filesystem"], params["key"])
    elif params["source"] == "arso_meteodata":
        arso_meteodata(params["filesystem"])
    elif params["source"] == "geosphere_meteodata":
        geosphere_meteodata(params["filesystem"])
    else:
        raise Exception("Currently only the following sources are supported: {}".format(setups))


if __name__ == "__main__":
    if sys.version_info[0:2] != (3, 9):
        raise Exception('Requires python 3.9')
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', '-s', help="Data source [meteoswiss_cosmo, bafu_hydrodata, meteoswiss_meteodata, meteoswiss_icon]", type=str)
    parser.add_argument('--filesystem', '-f', help="Path to local storage filesystem", type=str,)
    parser.add_argument('--password', '-p', help="FTP password", type=str, default=False)
    parser.add_argument('--key', '-k', help="Path to ssh key file", type=str, default=False)
    args = parser.parse_args()
    main(vars(args))
