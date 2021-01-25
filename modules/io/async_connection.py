import aiohttp
import aiohttp.client_exceptions
import asyncio
import time
import ssl
import xml.dom.minidom
from aiohttp_retry import RetryClient, ExponentialRetry, ListRetry


async def ConnectionWithRetry(logger, msgprefix, globopts, scheme, host, url, custauth=None, paginated=False):
    sslcontext = ssl.create_default_context(capath='/etc/grid-security/certificates/')
    sslcontext.load_cert_chain('hostcert.pem', 'hostkey.pem')

    suffix = url.split('method=')[1]

    # http_retry_options = ListRetry(timeouts=[10.0, 20.0, 30.0, 40.0])
    # http_retry_options = ExponentialRetry(attempts=2, statuses=[400])
    http_retry_options = ExponentialRetry(attempts=2)
    client_timeout = aiohttp.ClientTimeout(total=1*160, connect=None,
                                           sock_connect=None, sock_read=None)
    conn_try, connection_attempts = 1, 3

    print('aiohttp_get - started', f' - {time.ctime()}')
    try:
        async with RetryClient(retry_options=http_retry_options, timeout=client_timeout) as session:
            if paginated:
                count, cursor = 1, 0
                while count != 0:
                    while conn_try <= connection_attempts:
                        try:
                            async with session.get(url + f'&scope=&next_cursor={cursor}',
                                                   ssl=sslcontext) as resp:
                                print(resp.status)
                                content = await resp.text()
                                parsed = xml.dom.minidom.parseString(content)
                                write_file(suffix, content)
                                count = int(parsed.getElementsByTagName('count')[0].childNodes[0].data)
                                links = parsed.getElementsByTagName('link')
                                for le in links:
                                    if le.getAttribute('rel') == 'next':
                                        href = le.getAttribute('href')
                                        for e in href.split('&'):
                                            if 'next_cursor' in e:
                                                cursor = e.split('=')[1]
                                break
                        except asyncio.TimeoutError as e:
                            print('connection try - ', conn_try)
                        conn_try += 1
                    else:
                        print('connection exhausted')
                        break
                print(f'file-{suffix} written', f' - {time.ctime()}')
            else:
                async with session.get(url, ssl=sslcontext) as resp:
                    print(f'HTTP {suffix} ', resp.status, f' - {time.ctime()}')
                    content = await resp.text()
                    write_file(suffix, content)
                    print(f'file-{suffix} written', f' - {time.ctime()}')

    except Exception as e:
        print(type(e))
        print(e)
