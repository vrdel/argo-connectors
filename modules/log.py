import logging, logging.handlers
import sys
import socket

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

    for func in ['warn', 'error', 'critical', 'info']:
        code = """def %s(self, msg):
                    self.logger.%s(msg)""" % (func, func)
        exec code
