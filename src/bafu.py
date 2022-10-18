import os
import pysftp
from functions import logger


def hydrodata(data_folder, ssh_key, ftp_host="ftp.hydrodata.ch", ftp_user="eawag"):
    """
    Download Bafu data from Bafu sftp server.
    """
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    conn = pysftp.Connection(host=ftp_host, username=ftp_user, private_key=ssh_key, cnopts=cnopts)
    print(conn.listdir())
    conn.close()
