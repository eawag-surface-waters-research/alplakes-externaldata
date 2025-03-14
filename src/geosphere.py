import os
import time
import json
import requests
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from functions import logger, parse_dict_string, split_date_range

def geosphere_meteodata(data_folder):
    """
    Download Meteodata from Geosphere
    https://dataset.api.hub.geosphere.at/v1/docs/#

    Add new stations
    1. Find closest station https://alplakes-eawag.s3.eu-central-1.amazonaws.com/static/geosphere/geosphere_stations.json
    2. Add station to stations list
    3. Edit last_updated to an old date
    4. Upload data to API
    5. Edit FastAPI list of stations
    """

    stations = [
        {"id": "6512", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "2010-05-27T00:00"},
        {"id": "4821", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "2008-09-25T00:00"},
        {"id": "20123", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "1994-06-15T00:00"},
        {"id": "9618", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "2007-07-25T00:00"},
        {"id": "6415", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "1999-07-27T00:00"},
        {"id": "9643", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "2013-02-18T00:00"},
        {"id": "18225", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "1997-09-01T00:00"},
        {"id": "6621", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "2014-11-24T00:00"},
        {"id": "20220", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "1996-12-10T00:00"},
        {"id": "12311", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "1992-08-29T00:00"},
        {"id": "20212", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "1996-09-01T00:00"},
        {"id": "11505", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "1994-12-06T00:00"},
        {"id": "4515", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "2008-06-25T00:00"},
        {"id": "9406", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "2008-09-30T00:00"},
        {"id": "9016", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "1994-05-25T00:00"},
        {"id": "8806", "parameters": ["cglo", "dd", "p", "rf", "rr", "tl", "ffam"], "start": "2016-10-05T00:00"}
    ]
    failed = []

    log = logger("meteodata", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download Meteodata from Geosphere")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "geosphere/meteodata")
    if not os.path.exists(parent):
        os.makedirs(parent)

    current_date = datetime.now()
    last_update = current_date - timedelta(weeks=2)
    url = "https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-10min?{}&start={}&end={}&station_ids={}"

    for station in stations:
        log.info("Downloading data for station {}".format(station["id"]))
        start_date = max(last_update, datetime.fromisoformat(station["start"]))
        for chunk in split_date_range(start_date, current_date, 1, unit="years"):
            log.info("Accessing data from {} to {}".format(chunk[0], chunk[1]), indent=1)
            u = url.format("&".join(["parameters={}".format(p) for p in station["parameters"]]), chunk[0].isoformat(), chunk[1].isoformat(), station["id"])
            response = requests.get(u)
            if response.status_code == 200:
                try:
                    raw_data = response.json()
                    dict = {"time": raw_data["timestamps"]}
                    for p in station["parameters"]:
                        dict[p] = raw_data["features"][0]["properties"]["parameters"][p]["data"]
                    df = pd.DataFrame(dict)
                    df['time'] = pd.to_datetime(df['time'])
                    df = df.dropna(how='all', subset=df.columns.difference(['time']))
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
                except:
                    log.info("FAILED", indent=1)
                    if station["id"] not in failed:
                        failed.append(station["id"])
            else:
                log.info("FAILED", indent=1)
                if station["id"] not in failed:
                    failed.append(station["id"])

    if len(failed) > 0:
        raise ValueError("Failed to download at least one time period from: {}".format(", ".join(failed)))


