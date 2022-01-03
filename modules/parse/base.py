import xml.dom.minidom

from xml.parsers.expat import ExpatError
from argo_egi_connectors.utils import module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError
import json


class ParseHelpers(object):
    def __init__(self, logger, *args, **kwargs):
        self.logger = logger

    def parse_xmltext(self, nodelist):
        rc = []
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

    def parse_extensions(self, extensions_node):
        extensions_dict = dict()

        for extension in extensions_node:
            if extension.nodeName == 'EXTENSION':
                key, value = None, None
                for ext_node in extension.childNodes:
                    if ext_node.nodeName == 'KEY':
                        key = ext_node.childNodes[0].nodeValue
                    if ext_node.nodeName == 'VALUE':
                        value = ext_node.childNodes[0].nodeValue
                    if key and value:
                        extensions_dict.update({key: value})

        return extensions_dict

    def parse_url_endpoints(self, endpoints_node):
        endpoints_urls = list()

        for endpoint in endpoints_node:
            if endpoint.nodeName == 'ENDPOINT':
                url = None
                for endpoint_node in endpoint.childNodes:
                    if endpoint_node.nodeName == 'ENDPOINT_MONITORED':
                        value = endpoint_node.childNodes[0].nodeValue
                        if value.lower() == 'y':
                            for url_node in endpoint.childNodes:
                                if url_node.nodeName == 'URL' and url_node.childNodes:
                                    url = url_node.childNodes[0].nodeValue
                                    endpoints_urls.append(url)

        if endpoints_urls:
            return ', '.join(endpoints_urls)
        else:
            return None

    def parse_scopes(self, xml_node):
        scopes = list()

        for elem in xml_node.childNodes:
            if elem.nodeName == 'SCOPES':
                for subelem in elem.childNodes:
                    if subelem.nodeName == 'SCOPE':
                        scopes.append(subelem.childNodes[0].nodeValue)

        return scopes

    def parse_json(self, data):
        try:
            if data is None:
                raise ConnectorParseError("{} Customer:{} : No JSON data fetched".format(module_class_name(self), self.logger.customer))

            return json.loads(data)

        except ValueError as exc:
            msg = '{} Customer:{} : Error parsing JSON feed - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except Exception as exc:
            msg = '{} Customer:{} : Error - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

    def parse_xml(self, data):
        try:
            if data is None:
                raise ConnectorParseError("{} Customer:{} : No XML data fetched".format(module_class_name(self), self.logger.customer))

            return xml.dom.minidom.parseString(data)

        except ExpatError as exc:
            msg = '{} Customer:{} : Error parsing XML feed - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except Exception as exc:
            msg = '{} Customer:{} : Error - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)
