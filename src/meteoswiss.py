import os
import shutil
import pysftp
import fnmatch
import pandas as pd
from datetime import datetime, timezone
from functions import (logger, unzip_combine, progressbar, create_stac_session, stac_time, list_stac_forecast,
                       download_stac_asset, read_icon_grid, download_stac_forecast, process_icon_members,
                       delete_old_runs)


def cosmo(data_folder, ftp_password, ftp_host="sftp.eawag.ch", ftp_port=22, ftp_user="cosmo", progress=False):
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
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    conn = pysftp.Connection(host=ftp_host, port=ftp_port, username=ftp_user, password=ftp_password, cnopts=cnopts)
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
                    if progress:
                        conn.get(os.path.join(file["parent"], server_file),
                                 os.path.join(parent, file["folder"], server_file),
                                 callback=lambda x, y: progressbar(x, y))
                    else:
                        conn.get(os.path.join(file["parent"], server_file),
                                 os.path.join(parent, file["folder"], server_file))
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
        raise ValueError("Failed to download: {}".format(", ".join(failed)))


def icon(data_folder, ftp_password, ftp_host="sftp.eawag.ch", ftp_port=22, ftp_user="cosmo", progress=False):
    """
    Download ICON data from Eawag sftp server.
    Available files:
    - %Y_%m_%D_%H_icon-ch2-eps_eawag_lakes.zip (forecast):  ICON-CH2-EPS 5 day ensemble forecast
    - %Y_%m_%D_%H_icon-ch1-eps_eawag_lakes.zip (forecast):  ICON-CH1-EPS 33 hour ensemble forecast
    - %Y_%m_%D_%H_kenda-ch1_eawag_lakes.nc (reanalysis):  KENDA-CH1 1 day deterministic (data from previous day from name)
    - %Y_%m_%D_%H_kenda-ch1_eawag_lake_geneva_ensemble.nc (reanalysis):  KENDA-CH1 1 day ensemble forecast (data from previous day from name)
    """
    files = [{"name": "*_00_icon-ch2-eps_eawag_lakes.zip", "parent": "data/icon-ch2-eps", "folder": "icon-ch2-eps"},
             {"name": "*_00_icon-ch1-eps_eawag_lakes.zip", "parent": "data/icon-ch1-eps", "folder": "icon-ch1-eps"},
             {"name": "*_00_kenda-ch1_eawag_lakes.nc", "parent": "data/kenda-ch1", "folder": "kenda-ch1"},
             {"name": "*_00_kenda-ch1_eawag_lake_geneva_ensemble.nc", "parent": "data/kenda-ch1", "folder": "kenda-ch1-e"}]

    failed = []

    log = logger("icon", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download ICON data from Eawag sftp server")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "meteoswiss/icon")
    if not os.path.exists(parent):
        os.makedirs(parent)

    log.info("Connecting to {}".format(ftp_host))
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    conn = pysftp.Connection(host=ftp_host, port=ftp_port, username=ftp_user, password=ftp_password, cnopts=cnopts)
    log.info("Successfully connected to {}".format(ftp_host), indent=1)

    for file in files:
        log.info("Downloading {} files".format(file["folder"]))
        folder = os.path.join(data_folder, "meteoswiss/icon", file["folder"])
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
                    if progress:
                        conn.get(os.path.join(file["parent"], server_file),
                                 os.path.join(parent, file["folder"], server_file),
                                 callback=lambda x, y: progressbar(x, y))
                    else:
                        conn.get(os.path.join(file["parent"], server_file),
                                 os.path.join(parent, file["folder"], server_file))
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
        raise ValueError("Failed to download: {}".format(", ".join(failed)))


def icon_members(data_folder, retention_days=2, workers=8):
    """
    Download the 00 UTC ICON-CH2-EPS run from the MeteoSwiss open data STAC API and store every member as netCDF.
    https://opendatadocs.meteoswiss.ch/e-forecast-data/e2-e3-numerical-weather-forecasting-model
    Available for 24 hours after publication, which is typically 3.5 hours after the run starts.
    Output on the native grid: %Y_%m_%d_00_icon-ch2-eps_members.nc (forecast): ICON-CH2-EPS 5 day ensemble forecast
    """
    stac_url = "https://data.geo.admin.ch/api/stac/v1"
    collection = "ch.meteoschweiz.ogd-forecasting-icon-ch2"
    constants = "horizontal_constants_icon-ch2-eps.grib2"
    variables = ["T_2M", "TD_2M", "U_10M", "V_10M", "ASWDIR_S", "ASWDIFD_S", "CLCT", "TOT_PREC", "PMSL"]
    # GRIB typeOfStatisticalProcessing of the variables that are not instantaneous: 0 is average, 1 is accumulation
    statistics = {"ASWDIR_S": 0, "ASWDIFD_S": 0, "TOT_PREC": 1}
    leads = list(range(121))
    members = list(range(21))  # 0 is the control, 1 to 20 are perturbed
    # Names, units and long names follow the existing ICON products
    outputs = {"T_2M": {"units": "K", "long_name": "Temperature at 2m above ground"},
               "RELHUM_2M": {"units": "%", "long_name": "2m relative humidity (with respect to water)"},
               "TOT_PREC": {"units": "kg m-2", "long_name": "Total precipitation"},
               "U": {"units": "m s-1", "long_name": "Zonal wind", "vector_basis": "geographic"},
               "V": {"units": "m s-1", "long_name": "Meridional wind", "vector_basis": "geographic"},
               "PMSL": {"units": "Pa", "long_name": "Surface pressure reduced to msl"},
               "CLCT": {"units": "%", "long_name": "Total cloud cover"},
               "GLOB": {"units": "W m-2", "long_name": "Downward shortwave radiation flux at surface"}}

    log = logger("icon_members", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download ICON-CH2-EPS members from the MeteoSwiss open data STAC API")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "meteoswiss/icon/icon-ch2-eps-members")
    if not os.path.exists(parent):
        os.makedirs(parent)

    now = datetime.now(timezone.utc)
    ref_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
    output = os.path.join(parent, "{}_00_icon-ch2-eps_members.nc".format(ref_time.strftime("%Y_%m_%d")))
    grib_folder = output + ".tmp"

    try:
        if os.path.isfile(output):
            log.info("File {} already downloaded, skipping.".format(os.path.basename(output)))
        else:
            if os.path.exists(grib_folder):
                shutil.rmtree(grib_folder)
            session = create_stac_session(workers)

            log.info("Finding the run from {}".format(stac_time(ref_time)))
            items = list_stac_forecast(session, stac_url, collection, ref_time, variables, leads)

            log.info("Downloading the grid")
            download_stac_asset(session, stac_url, collection, constants, os.path.join(grib_folder, constants))
            grid, lat, lon = read_icon_grid(os.path.join(grib_folder, constants))

            log.info("Downloading {} files".format(len(items)))
            download_stac_forecast(session, stac_url, collection, ref_time, items, grib_folder, workers, log)

            log.info("Processing to {}".format(os.path.basename(output)))
            source = "model: icon-ch2-eps, generating_process: ensemble_forecast, product_category: eps_member"
            history = "Derived from MeteoSwiss open data collection {} on {}".format(collection, stac_time(now))
            process_icon_members(grib_folder, ref_time, output, grid, lat, lon, leads, members, statistics, outputs,
                                 source, history, log)
    finally:
        if os.path.exists(grib_folder):
            shutil.rmtree(grib_folder)
        delete_old_runs(parent, r"^(\d{4}_\d{2}_\d{2})_00_icon-ch2-eps_members\.nc(\.part|\.tmp)?$", now,
                        retention_days, log)


def meteodata(data_folder, ftp_password, folder="data", ftp_host="sftp.eawag.ch", ftp_port=22, ftp_user="simstrat"):
    """
    Download Meteodata from Eawag sftp server.
    A single file for the previous day is made available at around 10:15am and contains hourly data for a number of stations.
    This function looks for any non downloaded dates and process these files.
    """
    failed = []

    log = logger("meteodata", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download Meteodata from Eawag sftp server")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "meteoswiss/meteodata")
    if not os.path.exists(parent):
        os.makedirs(parent)

    log.info("Connecting to {}".format(ftp_host))
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    conn = pysftp.Connection(host=ftp_host, port=ftp_port, username=ftp_user, password=ftp_password, cnopts=cnopts)
    log.info("Successfully connected to {}".format(ftp_host))

    server_files = conn.listdir(folder)
    server_files.sort()
    last_update_file = os.path.join(parent, "last_update.txt")

    if os.path.exists(last_update_file):
        try:
            with open(last_update_file, "r") as f:
                last_update = int(f.readline())
            server_files = [f for f in server_files if int(f.split(".")[1][:8]) > last_update]
        except Exception as e:
            log.error("Failed to read last_update.txt, processing all files.", e)

    if len(server_files) > 0:
        log.info("Processing {} files.".format(len(server_files)))
        for server_file in server_files:
            log.info("Downloading file {}.".format(server_file), indent=1)
            temp_file = os.path.join(parent, server_file + ".temp")
            try:
                conn.get(os.path.join(folder, server_file), temp_file)
                df = pd.read_csv(temp_file, sep=";")
                df["time"] = pd.to_datetime(df['Date'], format='%Y%m%d%H', utc=True)
                for station in df["Station/Location"].unique():
                    log.info("Processing station {}.".format(station), indent=2)
                    if not os.path.exists(os.path.join(parent, station)):
                        os.makedirs(os.path.join(parent, station))
                    station_data = df.loc[df['Station/Location'] == station]
                    for year in range(df['time'].min().year, df['time'].max().year + 1):
                        station_year_file = os.path.join(parent, station, "VQCA44.{}.csv".format(year))
                        station_year_data = station_data[station_data['time'].dt.year == year].drop('time', axis=1)
                        if not os.path.exists(station_year_file):
                            log.info("Saving file new file {}.".format(station_year_file), indent=3)
                            station_year_data.to_csv(station_year_file, index=False)
                        else:
                            df_existing = pd.read_csv(station_year_file)
                            combined = pd.concat([df_existing, station_year_data])
                            combined = combined.drop_duplicates(subset=['Date'])
                            combined = combined.sort_values(by='Date')
                            combined = combined.fillna('-')
                            combined.to_csv(station_year_file, index=False)
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
            except Exception as e:
                log.error("Failed to download {}.".format(server_file), e)
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
                failed.append(server_file)

        with open(last_update_file, "w") as f:
            f.write(server_files[-1].split(".")[1][:8])

    else:
        raise ValueError("No new files available to process.")

    log.info("Closing connection to {}".format(ftp_host))
    conn.close()

    if len(failed) > 0:
        raise ValueError("Failed to download and process: {}".format(", ".join(failed)))

