import logging
import logging.handlers
import sys
import socket

logfile = "/var/log/argo-connectors/connectors.log"


class Logger:
    def __init__(self, connector):
        lfs = '%(name)s[%(process)s]: %(levelname)s %(message)s'
        lf = logging.Formatter(lfs)
        lv = logging.INFO

        logging.basicConfig(format=lfs, level=logging.INFO, stream=sys.stdout)
        self.logger = logging.getLogger(connector)

        try:
            sh = logging.handlers.SysLogHandler('/dev/log', logging.handlers.SysLogHandler.LOG_USER)
        except socket.error as e:
            sh = logging.StreamHandler()
        sh.setFormatter(lf)
        sh.setLevel(lv)
        self.logger.addHandler(sh)

        try:
            lffs = '%(asctime)s %(name)s[%(process)s]: %(levelname)s %(message)s'
            lff = logging.Formatter(lffs)
            fh = logging.handlers.RotatingFileHandler(logfile, maxBytes=512*1024, backupCount=5)
            fh.setFormatter(lff)
            fh.setLevel(lv)
            self.logger.addHandler(fh)
        except Exception:
            pass

    for func in ['warn', 'error', 'critical', 'info']:
        code = """def %s(self, msg):
                    self.logger.%s(msg)""" % (func, func)
        exec(code)
