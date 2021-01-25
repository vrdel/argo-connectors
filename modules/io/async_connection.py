#!/usr/bin/python3

import aiohttp
import aiohttp.client_exceptions
import asyncio
import time
import ssl
import xml.dom.minidom
from aiohttp_retry import RetryClient, ExponentialRetry, ListRetry


def build_ssl_settings(globopts):
    sslcontext = ssl.create_default_context(capath=globopts['AuthenticationCAPath'.lower()])
    sslcontext.load_cert_chain(globopts['AuthenticationHostCert'.lower()],
                               globopts['AuthenticationHostKey'.lower()])

    return sslcontext


def find_paging_cursor_count(res):
    cursor, count = 1, 0

    doc = xml.dom.minidom.parseString(res)
    count = int(doc.getElementsByTagName('count')[0].childNodes[0].data)
    links = doc.getElementsByTagName('link')
    for le in links:
        if le.getAttribute('rel') == 'next':
            href = le.getAttribute('href')
            for e in href.split('&'):
                if 'next_cursor' in e:
                    cursor = e.split('=')[1]

    return count, cursor


def build_connection_retry_settings(globopts):
    retry = int(globopts['ConnectionRetry'.lower()])
    sleep_retry = int(globopts['ConnectionSleepRetry'.lower()])
    timeout = int(globopts['ConnectionTimeout'.lower()])
    list_retry = [(i + 1) * sleep_retry for i in range(retry)]
    return (retry, list_retry, timeout)


async def http_get(logger, session, url, sslcontext=None):
    try:
        async with session.get(url, ssl=sslcontext) as response:
            content = await response.text()
            return content
    except Exception as exc:
        logger.error(exc)


class ConnectorError(Exception):
    pass


async def ConnectionWithRetry(logger, msgprefix, globopts, scheme, host, url,
                              custauth=None, paginated=False):

    ssl_context = build_ssl_settings(globopts)
    n_try, list_retry, client_timeout = build_connection_retry_settings(globopts)

    http_retry_options = ListRetry(timeouts=list_retry)
    client_timeout = aiohttp.ClientTimeout(total=client_timeout, connect=None,
                                           sock_connect=None, sock_read=None)

    try:
        async with RetryClient(retry_options=http_retry_options, timeout=client_timeout) as session:
            n = 0
            while n < n_try:
                if paginated:
                    count, cursor = 1, 0
                    while count != 0:
                        try:
                            content = await http_get(logger, session,
                                                     '{}://{}{}&next_cursor={}'.format(scheme, host, url, cursor),
                                                     ssl_context)
                            count, cursor = find_paging_cursor_count(content)
                            return content
                        except asyncio.TimeoutError as e:
                            print('connection try - ', n_try)
                else:
                    try:
                        content = await http_get(logger, session,
                                                 '{}://{}{}'.format(scheme, host, url),
                                                 ssl_context)
                        return content
                    except asyncio.TimeoutError as exc:
                        print('connection try - ', n)
                n += 1
            else:
                print('connection exhausted')

    except Exception as e:
        print(type(e))
        print(e)
