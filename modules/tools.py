import logging, logging.handlers
import sys
import re
import httplib
import socket
import signal

from OpenSSL.SSL import TLSv1_METHOD, Context, Connection
from OpenSSL.SSL import VERIFY_PEER, VERIFY_FAIL_IF_NO_PEER_CERT
from OpenSSL.SSL import Error as SSLError
from OpenSSL.SSL import OP_NO_SSLv3


def errmsg_from_excp(e):
    if getattr(e, 'message', False):
        retstr = ''
        if isinstance(e.message, list) or isinstance(e.message, tuple) \
                or isinstance(e.message, dict):
            for s in e.message:
                if isinstance(s, str):
                    retstr += s + ' '
                if isinstance(s, tuple) or isinstance(s, tuple):
                    retstr += ' '.join(s)
            return retstr
        elif isinstance(e.message, str):
            return e.message
        else:
            for s in e.message:
                retstr += str(s) + ' '
            return retstr
    else:
        return str(e)

def gen_fname_repdate(logger, timestamp, option, path):
    if re.search(r'DATE(.\w+)$', option):
        filename = path + re.sub(r'DATE(.\w+)$', r'%s\1' % timestamp, option)
    else:
        logger.error('No DATE placeholder in %s' % option)
        raise SystemExit(1)

    return filename

def make_connection(logger, globopts, scheme, host, url, msgprefix):
    i = 1
    try:
        while i <= int(globopts['ConnectionRetry'.lower()]):
            try:
                if scheme.startswith('https'):
                    if eval(globopts['AuthenticationVerifyServerCert'.lower()]):
                        verify_cert(host, globopts['AuthenticationCAPath'.lower()],
                                    timeout=int(globopts['ConnectionTimeout'.lower()]))
                    conn = httplib.HTTPSConnection(host, 443,
                                                   globopts['AuthenticationHostKey'.lower()],
                                                   globopts['AuthenticationHostCert'.lower()],
                                                   timeout=int(globopts['ConnectionTimeout'.lower()]))
                else:
                    conn = httplib.HTTPConnection(host, 80, timeout=int(globopts['ConnectionTimeout'.lower()]))

                conn.request('GET', url)
                return conn.getresponse()

            except(SSLError, socket.error, socket.timeout) as e:
                logger.warn('%sTry:%d Connection error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                                   i, scheme + '://' + host,
                                                                   errmsg_from_excp(e)))
                if i == int(globopts['ConnectionRetry'.lower()]):
                    raise e
                else:
                    pass
            i += 1

    except(SSLError, socket.error, socket.timeout) as e:
        logger.error('%sConnection error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                     scheme + '://' + host,
                                                     errmsg_from_excp(e)))
        raise SystemExit(1)


def verify_cert(host, capath, timeout):
    server_ctx = Context(TLSv1_METHOD)
    server_ctx.load_verify_locations(None, capath)

    def verify_cb(conn, cert, errnum, depth, ok):
        return ok
    server_ctx.set_verify(VERIFY_PEER|VERIFY_FAIL_IF_NO_PEER_CERT, verify_cb)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, 443))

    server_conn = Connection(server_ctx, sock)
    server_conn.set_connect_state()

    def handler(signum, frame):
        raise socket.error([('Timeout', 'after', str(timeout) + 's')])
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout)
    signal.alarm(0)
    try:
        server_conn.do_handshake()
    except SSLError as e:
        if 'sslv3 alert handshake failure' in errmsg_from_excp(e):
            pass
        else:
            raise SSLError(e.message)

    server_conn.shutdown()
    server_conn.close()

    return True
