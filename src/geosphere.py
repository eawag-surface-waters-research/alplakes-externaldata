import os
import time
import json
import requests
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from functions import logger, parse_dict_string

def geosphere_meteodata(data_folder):
    """
    Download Meteodata from Geosphere
    https://dataset.api.hub.geosphere.at/v1/docs/#
    """
    stations = [
        {"id": "6512", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"]}
    ]
    failed = []

    log = logger("meteodata", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download Meteodata from Geosphere")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "arso/meteodata")
    if not os.path.exists(parent):
        os.makedirs(parent)

    current_date = datetime.now()
    last_update = current_date - timedelta(weeks=2)
    url = "https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-10min?{}&start={}&end={}&station_ids={}"

    for station in stations:
        log.info("Downloading data for station {}".format(station["id"]))
        u = url.format("&".join(["parameters={}".format(p) for p in station["parameters"]]), last_update.isoformat(), current_date.isoformat(), station["id"])
        response = requests.get(u)
        if response.status_code == 200:
            raw_data = response.json()
            dict = {"time": raw_data["timestamps"]}
            for p in station["parameters"]:
                dict[p] = raw_data["features"][0]["properties"]["parameters"][p]["data"]
            df = pd.DataFrame(dict)
            df['time'] = pd.to_datetime(df['time'])
            for year in range(df['time'].min().year, df['time'].max().year + 1):
                station_year_file = os.path.join(parent, station["id"], "{}.csv".format(year))
                station_year_data = df[df['time'].dt.year == year]
                if not os.path.exists(station_year_file):
                    log.info("Saving file new file {}.".format(station_year_file), indent=1)
                    os.makedirs(os.path.dirname(station_year_file), exist_ok=True)
                    station_year_data.to_csv(station_year_file, index=False)
                else:
                    df_existing = pd.read_csv(station_year_file)
                    df_existing['time'] = pd.to_datetime(df_existing['time'])
                    combined = pd.concat([df_existing, station_year_data])
                    combined = combined.drop_duplicates(subset=['time'], keep='last')
                    combined = combined.sort_values(by='time')
                    combined.to_csv(station_year_file, index=False)
        else:
            failed.append(station["id"])

    if len(failed) > 0:
        raise ValueError("Failed to download and process: {}".format(", ".join(failed)))


