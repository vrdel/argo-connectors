import xml.dom.minidom

from xml.parsers.expat import ExpatError
from argo_egi_connectors.io.http import ConnectorError
from argo_egi_connectors.utils import module_class_name


class ParseHelpers(object):
    def __init__(self, logger, *args, **kwargs):
        self.logger = logger

    def _parse_xmltext(self, nodelist):
        rc = []
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

    def _parse_contact(self, contacts_node, *attrs):
        values = list()
        for xml_attr in attrs:
            value = contacts_node.getElementsByTagName(xml_attr)
            if value and value[0].childNodes:
                values.append(value[0].childNodes[0].nodeValue)
            else:
                values.append('')
        return values

    def _parse_contacts(self, data, root_node, child_node, topo_node):
        interested = ('EMAIL', 'FORENAME', 'SURNAME', 'CERTDN', 'ROLE_NAME')

        try:
            data = list()
            xml_data = self._parse_xml(self.data)
            entities = xml_data.getElementsByTagName(root_node)
            for entity in entities:
                if entity.nodeName == root_node:
                    emails = list()
                    for entity_node in entity.childNodes:
                        if entity_node.nodeName == child_node:
                            contact = entity_node
                            email, name, surname, certdn, role = self._parse_contact(contact, *interested)
                            emails.append({
                                'email': email,
                                'forename': name,
                                'surname': surname,
                                'certdn': certdn,
                                'role': role
                            })
                        if entity_node.nodeName == topo_node:
                            entity_name = entity_node.childNodes[0].nodeValue
                data.append({
                    'name': entity_name,
                    'contacts': emails
                })

            return data

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def _parse_servicegroup_contacts(self, data):
        try:
            endpoints_contacts = list()
            xml_data = self._parse_xml(data)
            elements = xml_data.getElementsByTagName('SERVICE_GROUP')
            for element in elements:
                name, contact = None, None
                for child in element.childNodes:
                    if child.nodeName == 'NAME':
                        name = child.childNodes[0].nodeValue
                    if child.nodeName == 'CONTACT_EMAIL':
                        contact = child.childNodes[0].nodeValue
                if contact and name:
                    endpoints_contacts.append({
                        'name': name,
                        'contact': contact
                    })
            return endpoints_contacts

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def _parse_serviceendpoint_contacts(self, data):
        try:
            endpoints_contacts = list()
            xml_data = self._parse_xml(data)
            elements = xml_data.getElementsByTagName('SERVICE_ENDPOINT')
            for element in elements:
                fqdn, contact, servtype = None, None, None
                for child in element.childNodes:
                    if child.nodeName == 'HOSTNAME':
                        fqdn = child.childNodes[0].nodeValue
                    if child.nodeName == 'CONTACT_EMAIL':
                        contact = child.childNodes[0].nodeValue
                    if child.nodeName == 'SERVICE_TYPE':
                        servtype = child.childNodes[0].nodeValue
                if contact:
                    if ';' in contact:
                        lcontacts = list()
                        for single_contact in contact.split(';'):
                            lcontacts.append(single_contact)
                        endpoints_contacts.append({
                            'name': '{}+{}'.format(fqdn, servtype),
                            'contacts': lcontacts
                        })
                    else:
                        endpoints_contacts.append({
                            'name': '{}+{}'.format(fqdn, servtype),
                            'contacts': [contact]
                        })
            return endpoints_contacts

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def _parse_extensions(self, extensions_node):
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

    def _parse_url_endpoints(self, endpoints_node):
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

    def _parse_scopes(self, xml_node):
        scopes = list()

        for elem in xml_node.childNodes:
            if elem.nodeName == 'SCOPES':
                for subelem in elem.childNodes:
                    if subelem.nodeName == 'SCOPE':
                        scopes.append(subelem.childNodes[0].nodeValue)

        return scopes

    def _parse_xml(self, data):
        try:
            return xml.dom.minidom.parseString(data)

        except ExpatError as exc:
            msg = '{} Customer:{} : Error parsing XML feed - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            self.logger.error(msg)
            raise ConnectorError()

        except Exception as exc:
            msg = '{} Customer:{} : Error - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            self.logger.error(msg)
            raise exc
