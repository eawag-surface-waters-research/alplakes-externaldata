import os
import logging
import traceback
from datetime import datetime, timedelta


class logger(object):
    def __init__(self, name, path="logs"):
        self.name = name + datetime.now().strftime("_%Y%m%d_%H%M%S") + ".txt"
        self.path = os.path.join(path, self.name)
        self.stage = 1

    def info(self, string, indent=0):
        logging.info(string)
        out = datetime.now().strftime("%H:%M:%S.%f") + (" " * 3 * (indent + 1)) + string
        print(out)
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def initialise(self, string):
        out = "****** " + string + " ******"
        print('\033[1m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def begin_stage(self, string):
        logging.info(string)
        self.newline()
        out = datetime.now().strftime("%H:%M:%S.%f") + "   Stage {}: ".format(self.stage) + string
        self.stage = self.stage + 1
        print('\033[95m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")
        return self.stage - 1

    def end_stage(self, stage):
        logging.info(string)
        out = datetime.now().strftime("%H:%M:%S.%f") + "   Stage {}: Completed.".format(stage)
        print('\033[92m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def warning(self, string, indent=0):
        logging.warning(string)
        out = datetime.now().strftime("%H:%M:%S.%f") + (" " * 3 * (indent + 1)) + "WARNING: " + string
        print('\033[93m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def error(self, stage):
        logging.error("ERROR: Script failed on stage {}".format(stage))
        out = datetime.now().strftime("%H:%M:%S.%f") + "   ERROR: Script failed on stage {}".format(stage)
        print('\033[91m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")
            file.write("\n")
            traceback.print_exc(file=file)

    def end(self, string):
        out = "****** " + string + " ******"
        print('\033[92m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def subprocess(self, process, error=""):
        failed = False
        while True:
            output = process.stdout.readline()
            out = output.strip()
            print(out)
            if error != "" and error in out:
                failed = True
            with open(self.path, "a") as file:
                file.write(out + "\n")
            return_code = process.poll()
            if return_code is not None:
                for output in process.stdout.readlines():
                    out = output.strip()
                    print(out)
                    with open(self.path, "a") as file:
                        file.write(out + "\n")
                break
        return failed

    def newline(self):
        print("")
        with open(self.path, "a") as file:
            file.write("\n")