import datetime
import re

strerr = ''
num_excp_expand = 0
daysback = 1

def retry(number=1):
    def deco(func):
        def wrap(*args, **kwargs):
            result = None
            logger = args[0]
            try:
                i = 1
                loops = number + 1
                while i <= range(loops):
                    try:
                        result = func(*args, **kwargs)
                    except Exception as e:
                        if i == loops:
                            raise e
                        else:
                            logger.warn('%s() Retry:%d ' % (func.__name__, i))
                            pass
                    i += 1
            except Exception as e:
                raise SystemExit(1)
            return result
        return wrap
    return deco

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
