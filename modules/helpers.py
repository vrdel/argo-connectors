import datetime
import re

strerr = ''
num_excp_expand = 0
daysback = 1

class retry:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        """Decorator that will repeat function calls in case of errors"""
        result = None
        # extract logger, object that called the func and number of tries
        logger = args[0]
        objname = args[1]
        loops = int(args[2]['ConnectionRetry'.lower()]) + 1
        try:
            i = 1
            while i <= range(loops):
                try:
                    result = self.func(*args, **kwargs)
                except Exception as e:
                    if i == loops:
                        raise e
                    else:
                        logger.warn('%s %s() Retry:%d ' % (objname, self.func.__name__, i))
                        pass
                else:
                    break
                i += 1
        except Exception as e:
            logger.error('%s %s() Giving up' % (objname, self.func.__name__))
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
