import logging
import logging.handlers
import sys
import socket

LOGFILE = "/var/log/argo-connectors/connectors.log"


class Logger:
    def __init__(self, connector):
        lfs = '%(name)s[%(process)s]: %(levelname)s %(message)s'
        logformat = logging.Formatter(lfs)
        logverbose = logging.INFO

        logging.basicConfig(format=lfs, level=logging.INFO, stream=sys.stdout)
        self.logger = logging.getLogger(connector)

        try:
            sysloghandle = logging.handlers.SysLogHandler('/dev/log', logging.handlers.SysLogHandler.LOG_USER)
        except socket.error:
            sysloghandle = logging.StreamHandler()
        sysloghandle.setFormatter(logformat)
        sysloghandle.setLevel(logverbose)
        self.logger.addHandler(sysloghandle)

        try:
            lffs = '%(asctime)s %(name)s[%(process)s]: %(levelname)s %(message)s'
            lff = logging.Formatter(lffs)
            filehandle = logging.handlers.RotatingFileHandler(LOGFILE, maxBytes=512*1024, backupCount=5)
            filehandle.setFormatter(lff)
            filehandle.setLevel(logverbose)
            self.logger.addHandler(filehandle)
        except Exception:
            pass

    def warn(self, msg):
        self.logger.warn(msg)

    def critical(self, msg):
        self.logger.critical(msg)

    def error(self, msg):
        self.logger.error(msg)

    def info(self, msg):
        self.logger.info(msg)
