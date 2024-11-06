import os
import time
import json
import netCDF4
import tempfile
import requests
import numpy as np
import pandas as pd
from functools import reduce
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from functions import logger, merge_dfs

def thredds_meteodata(data_folder):
    """
    Download Meteodata from Thredds
    https://thredds-su.ipsl.fr/thredds/catalog/aeris_thredds/actrisfr_data/665029c8-82b8-4754-9ff4-d558e640b0ba/catalog.html
    """
    stations = [
        {"id": "73329001", "name": "CHAMBERY-AIX", "parameters": ["time","ta","rh","wd","ws","cumul_precip","glo"]},
        {"id": "74182001", "name": "MEYTHET", "parameters": ["time","ta","rh","wd","ws","cumul_precip"]},
    ]
    failed = []

    log = logger("meteodata", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download Meteodata from Thredds")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "thredds/meteodata")
    if not os.path.exists(parent):
        os.makedirs(parent)

    current_date = datetime.now()
    last_update = current_date - timedelta(weeks=4)

    url = "https://thredds-su.ipsl.fr/thredds/fileServer/aeris_thredds/actrisfr_data/665029c8-82b8-4754-9ff4-d558e640b0ba/{}/{}_{}_MTO_1H_{}.nc"

    for station in stations:
        log.info("Downloading data for station {}".format(station["id"]))
        for year in range(last_update.year, current_date.year + 1):
            with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as temp_file:
                response = requests.get(url.format(year, station["id"], station["name"], year), stream=True)
                if response.status_code == 200:
                    for chunk in response.iter_content(chunk_size=8192):
                        temp_file.write(chunk)
                else:
                    print("{} ({})".format(station["id"], year))
                    failed.append("{} ({})".format(station["id"], year))
                    continue
            try:
                data = {}
                with netCDF4.Dataset(temp_file.name) as nc:
                    for parameter in station["parameters"]:
                        data[parameter] = nc.variables[parameter][:]
                df = pd.DataFrame(data)
                df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
                df = df.sort_values(by='time')
                station_year_file = os.path.join(parent, station["id"], "{}.csv".format(year))
                if not os.path.exists(station_year_file):
                    log.info("Saving file new file {}.".format(station_year_file), indent=1)
                    os.makedirs(os.path.dirname(station_year_file), exist_ok=True)
                    df.to_csv(station_year_file, index=False)
                else:
                    df_existing = pd.read_csv(station_year_file)
                    df_existing['time'] = pd.to_datetime(df_existing['time'])
                    combined = pd.concat([df_existing, df])
                    combined = combined.drop_duplicates(subset=['time'], keep='last')
                    combined = combined.sort_values(by='time')
                    combined.to_csv(station_year_file, index=False)
            except:
                failed.append("{} ({})".format(station["id"], year))
    if len(failed) > 0:
        raise ValueError("Failed to download and process: {}".format(", ".join(failed)))
