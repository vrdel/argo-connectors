import datetime
import re
import time

from argo_egi_connectors.config import Global

strerr = ''
num_excp_expand = 0
daysback = 1

class retry:
    def __init__(self, func):
        self.func = func
        self.numr = self._parameters()[0]
        self.timeout = self._parameters()[1]
        self.sleepretry = self._parameters()[2]

    def _parameters(self):
        cglob = Global(None)
        globo = cglob.parse()

        numr = int(globo['ConnectionRetry'.lower()])
        timeout = int(globo['ConnectionTimeout'.lower()])
        sleepretry = int(globo['ConnectionSleepRetry'.lower()])

        return numr, timeout, sleepretry

    def __call__(self, *args, **kwargs):
        """Decorator that will repeat function calls in case of errors"""
        result = None
        # extract logger, object that called the func and number of tries
        logger = args[0]
        objname = args[1]
        loops = self.numr + 1
        try:
            i = 1
            while i <= range(loops):
                try:
                    result = self.func(*args, **kwargs)
                except Exception as e:
                    if i == loops:
                        raise e
                    else:
                        logger.warn('%s %s() Retry:%d Sleeping:%d - %s' %
                                    (objname, self.func.__name__, i,
                                     self.sleepretry, error_message(e)))
                        time.sleep(self.sleepretry)
                        pass
                else:
                    break
                i += 1
        except Exception as e:
            logger.error('%s %s() Giving up - %s' % (objname, self.func.__name__, error_message(e)))
            return False
        return result

def error_message(exception):
    global strerr, num_excp_expand
    if isinstance(exception, Exception) and getattr(exception, 'args', False):
        num_excp_expand += 1
        if not error_message(exception.args):
            return strerr
    elif isinstance(exception, dict):
        for s in exception.iteritems():
            error_message(s)
    elif isinstance(exception, list):
        for s in exception:
            error_message(s)
    elif isinstance(exception, tuple):
        for s in exception:
            error_message(s)
    elif isinstance(exception, str):
        if num_excp_expand <= 1:
            strerr += exception + ' '

def date_check(arg):
    if re.search("[0-9]{4}-[0-9]{2}-[0-9]{2}", arg):
        return True
    else:
        return False

def datestamp(daysback=None):
    if daysback:
        dateback = datetime.datetime.now() - datetime.timedelta(days=daysback)
    else:
        dateback = datetime.datetime.now()

    return str(dateback.strftime('%Y_%m_%d'))

def filename_date(logger, option, path, stamp=None):
    stamp = stamp if stamp else datestamp(daysback)
    filename = path + re.sub(r'DATE(.\w+)$', r'%s\1' % stamp, option)

    return filename

def module_class_name(obj):
    name = repr(obj.__class__.__name__)

    return name.replace("'",'')
