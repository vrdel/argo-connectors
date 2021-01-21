import xml.dom.minidom
from xml.parsers.expat import ExpatError
from argo_egi_connectors.helpers import filename_date, module_class_name
from argo_egi_connectors.input import ConnectorError


class GOCDBParseSites(object):
    def __init__(self, logger, data):
        self.logger = logger
        self.data = data


class GOCDBParseServiceEndpoints(object):
    def __init__(self, logger, data):
        self.logger = logger
        self.data = data


class GOCDBParseServiceGroups(object):
    def __init__(self, logger, data, custname, uid=False):
        self.data = data
        self.uidservtype = uid
        self.logger = logger
        self.custname = custname
        # group_groups and group_endpoints components for ServiceGroup topology
        self._service_groups = dict()

    def _parse_xmltext(self, nodelist):
        rc = []
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

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

    def _service_groups_build(self):
        try:
            xml_data = self._parse_xml(self.data)
            groups = xml_data.getElementsByTagName('SERVICE_GROUP')
            for group in groups:
                groupId = group.getAttribute('PRIMARY_KEY')
                if groupId not in self._service_groups:
                    self._service_groups[groupId] = {}
                self._service_groups[groupId]['name'] = self._parse_xmltext(group.getElementsByTagName('NAME')[0].childNodes)
                self._service_groups[groupId]['monitored'] = self._parse_xmltext(group.getElementsByTagName('MONITORED')[0].childNodes)

                self._service_groups[groupId]['services'] = []
                services = group.getElementsByTagName('SERVICE_ENDPOINT')
                self._service_groups[groupId]['scope'] = ', '.join(self._parse_scopes(group))

                for service in services:
                    serviceDict = dict()

                    serviceDict['hostname'] = self._parse_xmltext(service.getElementsByTagName('HOSTNAME')[0].childNodes)
                    try:
                        serviceDict['service_id'] = self._parse_xmltext(service.getElementsByTagName('PRIMARY_KEY')[0].childNodes)
                    except IndexError:
                        serviceDict['service_id'] = service.getAttribute('PRIMARY_KEY')
                    serviceDict['type'] = self._parse_xmltext(service.getElementsByTagName('SERVICE_TYPE')[0].childNodes)
                    serviceDict['monitored'] = self._parse_xmltext(service.getElementsByTagName('NODE_MONITORED')[0].childNodes)
                    serviceDict['production'] = self._parse_xmltext(service.getElementsByTagName('IN_PRODUCTION')[0].childNodes)
                    serviceDict['scope'] = ', '.join(self._parse_scopes(service))
                    self._service_groups[groupId]['services'].append(serviceDict)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s Job:%s : Error parsing feed - %s' % (self.logger.customer, self.logger.job, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def _get_group_endpoints_servicegroups(self, groupofendpoints):
        group_list = list()

        group_list = group_list + [value for key, value in self._service_groups.items()]

        for group in group_list:
            for service in group['services']:
                tmpg = dict()
                tmpg['type'] = 'SERVICEGROUPS'
                tmpg['group'] = group['name']
                tmpg['service'] = service['type']
                if self.uidservtype:
                    tmpg['hostname'] = '{1}_{0}'.format(service['service_id'], service['hostname'])
                else:
                    tmpg['hostname'] = service['hostname']
                tmpg['tags'] = {'scope': service.get('scope', ''),
                                'monitored': '1' if service['monitored'].lower() == 'Y'.lower() or
                                service['monitored'].lower() == 'True'.lower() else '0',
                                'production': '1' if service['production'].lower() == 'Y'.lower() or
                                service['production'].lower() == 'True'.lower() else '0'}
                groupofendpoints.append(tmpg)

    def _get_group_groups_servicegroups(self, groupofgroups):
        group_list = list()
        group_list = group_list + [value for key, value in self._service_groups.items()]

        for group in group_list:
            tmpg = dict()
            tmpg['type'] = 'PROJECT'
            tmpg['group'] = self.custname
            tmpg['subgroup'] = group['name']
            tmpg['tags'] = {'monitored': '1' if group['monitored'].lower() == 'Y'.lower() or
                         group['monitored'].lower() == 'True'.lower() else '0', 'scope': group.get('scope', '')}
            groupofgroups.append(tmpg)

    def _get_group_groups(self):
        groupofgroups = list()
        self._get_group_groups_servicegroups(groupofgroups)

        return groupofgroups

    def _get_group_endpoints(self):
        groupofendpoints = list()
        self._get_group_endpoints_servicegroups(groupofendpoints)

        return groupofendpoints

    def get_data(self):
        self._service_groups_build()
        # TODO: split in two
        return self._get_group_groups() + self._get_group_endpoints()
