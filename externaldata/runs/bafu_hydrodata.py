import os
import sys
from externaldata.bafu import hydrodata

if len(sys.argv) == 3 and os.path.exists(str(sys.argv[1])):
    hydrodata(str(sys.argv[1]), str(sys.argv[2]))
else:
    raise ValueError("Download directory and/or link to ssh-key not provided.")
