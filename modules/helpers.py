import datetime
import os
import re

strerr = ''
num_excp_expand = 0
daysback = 1

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

def gen_fname_timestamp(daysback):
    dateback = datetime.datetime.now() - datetime.timedelta(days=daysback)
    return str(dateback.strftime('%Y_%m_%d'))

def gen_fname_repdate(logger, option, path, datestamp=None):
    datestamp = datestamp if datestamp else gen_fname_timestamp(daysback)
    if re.search(r'DATE(.\w+)$', option):
        filename = path + re.sub(r'DATE(.\w+)$', r'%s\1' % datestamp, option)
    else:
        logger.error('No DATE placeholder in %s' % option)
        raise SystemExit(1)

    return filename

def module_class_name(obj):
    name = repr(obj.__class__.__name__)
    return name.replace("'",'')

def write_state(caller, statedir, state, savedays, datestamp=None):
    filenamenew = ''
    if 'topology' in caller:
        filenamebase = 'topology-ok'
    elif 'poem' in caller:
        filenamebase = 'poem-ok'
    elif 'weights' in caller:
        filenamebase = 'weights-ok'
    elif 'downtimes' in caller:
        filenamebase = 'downtimes-ok'

    if datestamp:
        datebackstamp = datestamp
    else:
        datebackstamp = gen_fname_timestamp(daysback)

    filenamenew = filenamebase + '_' + datebackstamp
    db = datetime.datetime.strptime(datebackstamp, '%Y_%m_%d')

    datestart = db - datetime.timedelta(days=int(savedays))
    i = 0
    while i < int(savedays)*2:
        d = datestart - datetime.timedelta(days=i)
        filenameold = filenamebase + '_' + d.strftime('%Y_%m_%d')
        if os.path.exists(statedir + '/' + filenameold):
            os.remove(statedir + '/' + filenameold)
        i += 1

    with open(statedir + '/' + filenamenew, 'w') as fp:
        fp.write(str(state))
