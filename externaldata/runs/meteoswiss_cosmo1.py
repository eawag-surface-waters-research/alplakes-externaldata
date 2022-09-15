import os
import sys
from externaldata.meteoswiss import cosmo_1

if len(sys.argv) == 3 and os.path.exists(str(sys.argv[1])):
    cosmo_1(str(sys.argv[1]), str(sys.argv[2]))
else:
    raise ValueError("Download directory and/or ftp password not provided.")
