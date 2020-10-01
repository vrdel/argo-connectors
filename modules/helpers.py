import datetime
import re
import time

strerr = ''
num_excp_expand = 0
daysback = 1


class retry:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        """
        Decorator that will repeat function calls in case of errors.

        First three arguments of decorated function should be:
            - logger object
            - prefix of each log msg that is usually name of object
              constructing msg
            - dictionary holding num of retries, timeout and sleepretry
              parameters
        """
        result = None
        logger = args[0]
        objname = args[1]
        self.numr = int(args[2]['ConnectionRetry'.lower()])
        self.sleepretry = int(args[2]['ConnectionSleepRetry'.lower()])
        loops = self.numr + 1
        try:
            i = 1
            while i <= loops:
                try:
                    result = self.func(*args, **kwargs)
                except Exception as e:
                    if i == loops:
                        raise e
                    else:
                        if getattr(logger, 'job', False):
                            msg = '{} {}() Customer:{} Job:{} Retry:{} Sleeping:{} - {}'.format(objname,
                                                                                                self.func.__name__,
                                                                                                logger.customer, logger.job, i,
                                                                                                self.sleepretry, repr(e))

                        else:
                            msg = '{} {}() Customer:{} Retry:{} Sleeping:{} - {}'.format(objname,
                                                                                         self.func.__name__,
                                                                                         logger.customer, i,
                                                                                         self.sleepretry, repr(e))
                        logger.warn(msg)
                        time.sleep(self.sleepretry)
                        pass
                else:
                    break
                i += 1
        except Exception as e:
            if getattr(logger, 'job', False):
                msg = '{} {}() Customer:{} Job:{} Giving up - {}'.format(objname, self.func.__name__, logger.customer, logger.job, repr(e))
            else:
                msg = '{} {}() Customer:{} Giving up - {}'.format(objname, self.func.__name__, logger.customer, repr(e))

            logger.error(msg)
            return False

        return result


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

    return name.replace("'", '')
