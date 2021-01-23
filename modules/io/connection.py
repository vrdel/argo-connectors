import base64
import json
import requests
import socket
import xml.dom.minidom

from xml.parsers.expat import ExpatError
from urllib.parse import urlparse


class Retry:
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


class ConnectorError(Exception):
    pass


@Retry
def ConnectionWithRetry(logger, msgprefix, globopts, scheme, host, url, custauth=None):
    try:
        buf = None

        headers = {}
        if custauth and msgprefix == 'metricprofile-webapi-connector.py':
            headers = {'x-api-key': custauth['WebApiToken'.lower()],
                       'Accept': 'application/json'}
        elif msgprefix != 'PoemReader' and custauth and eval(custauth['AuthenticationUsePlainHttpAuth'.lower()]):
            userpass = '{}:{}'.format(custauth['AuthenticationHttpUser'.lower()], custauth['AuthenticationHttpPass'.lower()])
            b64userpass = base64.b64encode(userpass.encode())
            headers = {'Authorization': 'Basic ' + b64userpass.decode()}

        if scheme.startswith('https'):
            response = requests.get('https://' + host + url, headers=headers,
                                    cert=(globopts['AuthenticationHostCert'.lower()],
                                          globopts['AuthenticationHostKey'.lower()]),
                                    verify=eval(globopts['AuthenticationVerifyServerCert'.lower()]),
                                    timeout=int(globopts['ConnectionTimeout'.lower()]))
            response.raise_for_status()
        else:
            response = requests.get('http://' + host + url, headers=headers,
                                    timeout=int(globopts['ConnectionTimeout'.lower()]))

        if response.status_code >= 300 and response.status_code < 400:
            headers = response.headers
            location = filter(lambda h: 'location' in h[0], headers)
            if location:
                redir = urlparse(location[0][1])
            else:
                raise requests.exceptions.RequestException('No Location header set for redirect')

            return connection(logger, msgprefix, globopts, scheme, redir.netloc, redir.path + '?' + redir.query, custauth=custauth)

        elif response.status_code == 200:
            buf = response.content
            if not buf:
                raise requests.exceptions.RequestException('Empty response')

        else:
            raise requests.exceptions.RequestException('response: %s %s' % (response.status_code, response.reason))

        return buf

    except requests.exceptions.SSLError as e:
        if (getattr(e, 'args', False) and type(e.args) == tuple and
            type(e.args[0]) == str and 'timed out' in e.args[0]):
            raise e
        else:
            if getattr(logger, 'job', False):
                msg = '{}Customer:{} Job:{} SSL Error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                                      logger.customer, logger.job,
                                                                      scheme + '://' + host + url,
                                                                      repr(e))
            else:
                msg = '{}Customer:{} SSL Error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                               logger.customer,
                                                               scheme + '://' + host + url,
                                                               repr(e))

            logger.critical(msg)
        return False

    except(socket.error, socket.timeout) as e:
        if getattr(logger, 'job', False):
            msg = '{}Customer:{} Job:{} Connection error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                                         logger.customer, logger.job,
                                                                         scheme + '://' + host + url,
                                                                         repr(e))
        else:
            msg = '{}Customer:{} Connection error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                                  logger.customer,
                                                                  scheme + '://' + host + url,
                                                                  repr(e))
        logger.warn(msg)
        raise e

    except requests.exceptions.RequestException as e:
        if getattr(logger, 'job', False):
            msg = '{}Customer:{} Job:{} HTTP error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                                   logger.customer, logger.job,
                                                                   scheme + '://' + host + url,
                                                                   repr(e))
        else:
            msg = '{}Customer:{} HTTP error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                            logger.customer,
                                                            scheme + '://' + host + url,
                                                            repr(e))
        logger.warn(msg)
        raise e

    except Exception as e:
        if getattr(logger, 'job', False):
            msg = '{}Customer:{} Job:{} Error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                              logger.customer, logger.job,
                                                              scheme + '://' + host + url,
                                                              repr(e))
        else:
            msg = '{}Customer:{} Error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                       logger.customer,
                                                       scheme + '://' + host + url,
                                                       repr(e))
        logger.warn(msg)
        return False
