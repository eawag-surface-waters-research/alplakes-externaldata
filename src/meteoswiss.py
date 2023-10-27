import os
import pysftp
import fnmatch
import pandas as pd
from functions import logger, unzip_combine, progressbar


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
                            combined.fillna('-', inplace=True)
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

