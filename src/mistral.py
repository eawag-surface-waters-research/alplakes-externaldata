import os
import time
import json
import requests
import numpy as np
import pandas as pd
from functools import reduce
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from functions import logger, merge_dfs

def mistral_meteodata(data_folder, user, password):
    """
    Download Meteodata from Mistral
    https://meteohub.mistralportal.it:7777/
    """
    stations = [
        {"id": "trn196", "parameters": ['B14198', 'B12101', 'B13003', 'B11001', 'B11002'], "lat": 46.06192, "lng": 11.12041, "network": "mnw"},
        {"id": "vnt387", "parameters": ['B14198', 'B12101', 'B13003', 'B11001', 'B11002'], "lat": 45.64268, "lng": 10.73399, "network": "mnw"},
        {"id": "tignale_oldesio", "parameters": ['B11002', 'B12101', 'B13011', 'B11001', 'B14198', 'B13003'], "lat": 45.73262, "lng": 10.72092, "network": "dpcn-lombardia"},
        {"id": "Tavernola Bergamasca Gallinarga", "parameters": ['B12101', 'B13003', 'B11001', 'B11002', 'B13011'], "lat": 45.69633, "lng": 10.05422, "network": "dpcn-lombardia"},
        {"id": "Costa Volpino v.Nazionale", "parameters": ['B14198', 'B12101', 'B13003', 'B11001', 'B11002', 'B13011'], "lat": 45.82716, "lng": 10.09706, "network": "dpcn-lombardia"},
        {"id": "lmb341", "parameters": ['B14198', 'B12101', 'B13003', 'B11001', 'B11002'], "lat": 45.60308, "lng": 9.8966, "network": "mnw"},
        {"id": "Dervio v.S.Cecilia", "parameters": ['B12101', 'B13003', 'B11001', 'B11002', 'B13011'], "lat": 46.06896, "lng": 9.30539, "network": "dpcn-lombardia"},
        {"id": "Porlezza torrente", "parameters": ['B14198', 'B12101', 'B13003', 'B11001', 'B11002', 'B13011'], "lat": 46.03777, "lng": 9.1408, "network": "dpcn-lombardia"}
    ]
    failed = []

    log = logger("meteodata", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download Meteodata from Mistral")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "mistral/meteodata")
    if not os.path.exists(parent):
        os.makedirs(parent)

    log.info("Collecting authentication token.")
    response = requests.post("https://meteohub.mistralportal.it/auth/login", json={
        'username': user,
        'password': password
    }, headers={
        'accept': 'application/json',
        'Content-Type': 'application/json'
    })
    if response.status_code != 200:
        raise ValueError("Failed to authenticate with Mistral server")
    token = response.json()

    current_date = datetime.now()
    last_update = current_date - timedelta(weeks=6)
    url = "https://meteohub.mistralportal.it/api/observations?q=reftime:%20%3E={}%2000:00,%3C={}%2023:59;license:CCBY_COMPLIANT;timerange:254,0,0&allStationProducts=true&networks={}&latmin={}&lonmin={}&latmax={}&lonmax={}"

    for station in stations:
        log.info("Downloading data for station {}".format(station["id"]))
        u = url.format(last_update.strftime("%Y-%m-%d"), current_date.strftime("%Y-%m-%d"), station["network"],
                       station["lat"] - 0.001, station["lng"] - 0.001, station["lat"] + 0.001, station["lng"] + 0.001)
        response = requests.get(u, headers={
            'accept': 'application/json',
            'Authorization': f'Bearer {token}'
        })
        if response.status_code == 200:
            try:
                data = response.json()["data"][0]["prod"]
                dfs = []
                for p in data:
                    if p["var"] in station["parameters"]:
                        d = {"time": [], p["var"]: []}
                        for v in p["val"]:
                            d["time"].append(v["ref"])
                            d[p["var"]].append(v["val"])
                        df = pd.DataFrame(d)
                        df["time"] = pd.to_datetime(df['time'])
                        df = df.sort_values(by='time').reset_index(drop=True)
                        dfs.append(df)
                df = reduce(merge_dfs, dfs)
                df = df.sort_values(by='time').reset_index(drop=True)
                for p in station["parameters"]:
                    if p not in df.columns:
                        df[p] = None
                for year in range(df['time'].min().year, df['time'].max().year + 1):
                    station_year_file = os.path.join(parent, station["id"].lower().replace(" ", "_").replace(".", "_"), "{}.csv".format(year))
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
            except Exception as e:
                print(e)
                failed.append(station["id"])
        else:
            print(response)
            failed.append(station["id"])

    requests.get("https://meteohub.mistralportal.it/auth/logout", headers={
        'accept': 'application/json',
        'Authorization': f'Bearer {token}'
    })

    if len(failed) > 0:
        raise ValueError("Failed to download and process: {}".format(", ".join(failed)))
