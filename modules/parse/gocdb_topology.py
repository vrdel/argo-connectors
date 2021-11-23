from argo_egi_connectors.parse.base import ParseHelpers
from argo_egi_connectors.utils import module_class_name
from argo_egi_connectors.parse.base import ConnectorParseError


class ParseSites(ParseHelpers):
    def __init__(self, logger, data, custname, uid=False,
                 pass_extensions=False):
        super().__init__(logger)
        self.data = data
        self.uidservtype = uid
        self.custname = custname
        self.pass_extensions = pass_extensions
        self._sites = dict()
        self._parse_data()

    def _parse_data(self):
        try:
            xml_data = self._parse_xml(self.data)
            sites = xml_data.getElementsByTagName('SITE')
            for site in sites:
                site_name = site.getAttribute('NAME')
                if site_name not in self._sites:
                    self._sites[site_name] = {'site': site_name}
                self._sites[site_name]['infrastructure'] = self._parse_xmltext(site.getElementsByTagName('PRODUCTION_INFRASTRUCTURE')[0].childNodes)
                self._sites[site_name]['certification'] = self._parse_xmltext(site.getElementsByTagName('CERTIFICATION_STATUS')[0].childNodes)
                self._sites[site_name]['ngi'] = self._parse_xmltext(site.getElementsByTagName('ROC')[0].childNodes)
                self._sites[site_name]['scope'] = ', '.join(self._parse_scopes(site))

                if self.pass_extensions:
                    extensions = self._parse_extensions(site.getElementsByTagName('EXTENSIONS')[0].childNodes)
                    self._sites[site_name]['extensions'] = extensions

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError

    def get_group_groups(self):
        group_list, groupofgroups = list(), list()
        group_list = group_list + sorted([value for _, value in self._sites.items()], key=lambda s: s['ngi'])

        for group in group_list:
            tmpg = dict()
            tmpg['type'] = 'NGI'
            tmpg['group'] = group['ngi']
            tmpg['subgroup'] = group['site']
            tmpg['tags'] = {'certification': group['certification'],
                            'scope': group.get('scope', ''),
                            'infrastructure': group['infrastructure']}

            if self.pass_extensions:
                for key, value in group['extensions'].items():
                    tmpg['tags'].update({
                        'info_ext_' + key: value
                    })

            groupofgroups.append(tmpg)

        return groupofgroups


class ParseServiceEndpoints(ParseHelpers):
    def __init__(self, logger, data=None, custname=None, uid=False,
                 pass_extensions=False):
        super().__init__(logger)
        self.data = data
        self.__class__.fetched_data = data
        self.uidservtype = uid
        self.custname = custname
        self.pass_extensions = pass_extensions
        self._service_endpoints = dict()

    def _parse_data(self):
        try:
            xml_data = self._parse_xml(self.data)
            services = xml_data.getElementsByTagName('SERVICE_ENDPOINT')
            for service in services:
                service_id = ''
                if service.getAttributeNode('PRIMARY_KEY'):
                    service_id = str(service.attributes['PRIMARY_KEY'].value)
                if service_id not in self._service_endpoints:
                    self._service_endpoints[service_id] = {}
                self._service_endpoints[service_id]['hostname'] = self._parse_xmltext(service.getElementsByTagName('HOSTNAME')[0].childNodes)
                self._service_endpoints[service_id]['type'] = self._parse_xmltext(service.getElementsByTagName('SERVICE_TYPE')[0].childNodes)
                self._service_endpoints[service_id]['monitored'] = self._parse_xmltext(service.getElementsByTagName('NODE_MONITORED')[0].childNodes)
                self._service_endpoints[service_id]['production'] = self._parse_xmltext(service.getElementsByTagName('IN_PRODUCTION')[0].childNodes)
                self._service_endpoints[service_id]['site'] = self._parse_xmltext(service.getElementsByTagName('SITENAME')[0].childNodes)
                self._service_endpoints[service_id]['roc'] = self._parse_xmltext(service.getElementsByTagName('ROC_NAME')[0].childNodes)
                self._service_endpoints[service_id]['service_id'] = service_id
                self._service_endpoints[service_id]['scope'] = ', '.join(self._parse_scopes(service))
                self._service_endpoints[service_id]['sortId'] = self._service_endpoints[service_id]['hostname'] + '-' + self._service_endpoints[service_id]['type'] + '-' + self._service_endpoints[service_id]['site']
                self._service_endpoints[service_id]['url'] = self._parse_xmltext(service.getElementsByTagName('URL')[0].childNodes)
                if self.pass_extensions:
                    extensions = self._parse_extensions(service.getElementsByTagName('EXTENSIONS')[0].childNodes)
                    self._service_endpoints[service_id]['extensions'] = extensions
                self._service_endpoints[service_id]['endpoint_urls'] = self._parse_url_endpoints(service.getElementsByTagName('ENDPOINTS')[0].childNodes)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError

    def get_group_endpoints(self):
        group_list, groupofendpoints = list(), list()
        group_list = group_list + sorted([value for _, value in self._service_endpoints.items()], key=lambda s: s['site'])

        for group in group_list:
            tmpg = dict()
            tmpg['type'] = 'SITES'
            tmpg['group'] = group['site']
            tmpg['service'] = group['type']
            if self.uidservtype:
                tmpg['hostname'] = '{1}_{0}'.format(group['service_id'], group['hostname'])
            else:
                tmpg['hostname'] = group['hostname']
            tmpg['tags'] = {'scope': group.get('scope', ''),
                            'monitored': '1' if group['monitored'] == 'Y' or
                            group['monitored'] == 'True' else '0',
                            'production': '1' if group['production'] == 'Y' or
                            group['production'] == 'True' else '0'}
            if group['url']:
                tmpg['tags'].update({
                    'info_URL': group['url']
                })
            if group['endpoint_urls']:
                tmpg['tags'].update({
                    'info_service_endpoint_URL': group['endpoint_urls']
                })

            if self.pass_extensions:
                for key, value in group['extensions'].items():
                    tmpg['tags'].update({
                        'info_ext_' + key: value
                    })

            groupofendpoints.append(tmpg)

        return groupofendpoints


class ParseServiceGroups(ParseHelpers):
    def __init__(self, logger, data, custname, uid=False,
                 pass_extensions=False):
        super().__init__(logger)
        self.data = data
        self.uidservtype = uid
        self.custname = custname
        self.pass_extensions = pass_extensions
        # group_groups and group_endpoints components for ServiceGroup topology
        self._service_groups = dict()
        self._parse_data()


    def _parse_data(self):
        try:
            xml_data = self._parse_xml(self.data)
            groups = xml_data.getElementsByTagName('SERVICE_GROUP')
            for group in groups:
                group_id = group.getAttribute('PRIMARY_KEY')
                if group_id not in self._service_groups:
                    self._service_groups[group_id] = {}
                self._service_groups[group_id]['name'] = self._parse_xmltext(group.getElementsByTagName('NAME')[0].childNodes)
                self._service_groups[group_id]['monitored'] = self._parse_xmltext(group.getElementsByTagName('MONITORED')[0].childNodes)

                self._service_groups[group_id]['services'] = []
                services = group.getElementsByTagName('SERVICE_ENDPOINT')
                self._service_groups[group_id]['scope'] = ', '.join(self._parse_scopes(group))

                for service in services:
                    tmps = dict()

                    tmps['hostname'] = self._parse_xmltext(service.getElementsByTagName('HOSTNAME')[0].childNodes)
                    try:
                        tmps['service_id'] = self._parse_xmltext(service.getElementsByTagName('PRIMARY_KEY')[0].childNodes)
                    except IndexError:
                        tmps['service_id'] = service.getAttribute('PRIMARY_KEY')
                    tmps['type'] = self._parse_xmltext(service.getElementsByTagName('SERVICE_TYPE')[0].childNodes)
                    tmps['monitored'] = self._parse_xmltext(service.getElementsByTagName('NODE_MONITORED')[0].childNodes)
                    tmps['production'] = self._parse_xmltext(service.getElementsByTagName('IN_PRODUCTION')[0].childNodes)
                    tmps['scope'] = ', '.join(self._parse_scopes(service))
                    tmps['endpoint_urls'] = self._parse_url_endpoints(service.getElementsByTagName('ENDPOINTS')[0].childNodes)
                    if self.pass_extensions:
                        extensions = self._parse_extensions(service.getElementsByTagName('EXTENSIONS')[0].childNodes)
                        tmps['extensions'] = extensions
                    self._service_groups[group_id]['services'].append(tmps)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError

    def get_group_endpoints(self):
        group_list, groupofendpoints = list(), list()
        group_list = group_list + [value for _, value in self._service_groups.items()]

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

                if self.pass_extensions:
                    for key, value in service['extensions'].items():
                        tmpg['tags'].update({
                            'info_ext_' + key: value
                        })
                if service['endpoint_urls']:
                    tmpg['tags'].update({
                        'info_service_endpoint_URL': service['endpoint_urls']
                    })

                groupofendpoints.append(tmpg)

        return groupofendpoints

    def get_group_groups(self):
        group_list, groupofgroups = list(), list()
        group_list = group_list + [value for _, value in self._service_groups.items()]

        for group in group_list:
            tmpg = dict()
            tmpg['type'] = 'PROJECT'
            tmpg['group'] = self.custname
            tmpg['subgroup'] = group['name']
            tmpg['tags'] = {'monitored': '1' if group['monitored'].lower() == 'Y'.lower() or
                            group['monitored'].lower() == 'True'.lower() else '0', 'scope': group.get('scope', '')}
            groupofgroups.append(tmpg)

        return groupofgroups
