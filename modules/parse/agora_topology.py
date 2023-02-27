from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.utils import module_class_name

import json
from json.decoder import JSONDecodeError


class ParseAgoraTopo(object):
    def __init__(self, logger, providers, resources, uidservendp):
        self.logger = logger
        self.providers = providers
        self.resources = resources
        self.uidservendp = uidservendp

    def get_group_groups(self):
        try:
            providers = list()
            providers_data = json.loads(self.providers)

            for data in providers_data:
                providers.append({
                    'group': 'NI4OS Providers',
                    'type': 'PROVIDERS',
                    'subgroup': data['epp_bai_id'],
                    'tags': {
                        'info_ext_catalog_id': data['id'],
                        'info_ext_catalog_type': 'provider',
                        'info_ext_catalog_url': f'https://catalogue.ni4os.eu/?_=/providers/{data["id"]}',
                        'info_ext_name': data['epp_bai_name'],
                    }
                })

            return providers

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, JSONDecodeError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing Agora Providers feed - %s' % (
                self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc

    def get_group_endpoints(self):
        try:
            resources = list()
            providers_data = json.loads(self.providers)
            resources_data = json.loads(self.resources)

            for data in resources_data:
                if data['erp_bai_providers_public'] != [] and len(data['erp_bai_providers_public']) > 1:
                    group_name = [d['epp_bai_id']
                                  for d in data['erp_bai_providers_public']]
                elif data['erp_bai_providers_public'] != []:
                    group_name = data['erp_bai_providers_public'][0]['epp_bai_id']

                no_https = f'{data["erp_bai_webpage"].split("//")[1] if data["erp_bai_webpage"] != None else ""}'

                if type(group_name) == list and group_name != []:
                    for i in range(len(group_name)):
                        resources.append({
                            'group': group_name[i],
                            'type': 'SERVICEGROUPS',
                            'service': 'catalog.service.entry',
                            'hostname': f'{no_https}__{data["erp_bai_id"].lower().replace(" ", "_")}',
                            'tags': {
                                'hostname': no_https,
                                'info_ID': f'{data["erp_bai_id"].lower().replace(" ", "_") if data["erp_bai_id"] is not None else ""}',
                                'info_ext_catalog_id': f'{data["id"]}',
                                'info_ext_catalog_type': 'resource',
                                'info_ext_catalog_url': f'https://catalogue.ni4os.eu/?_=/resources/{data["id"]}',
                                'info_ext_name': f'{data["erp_bai_name"]}'
                            }
                        })

                else:
                    resources.append({
                        'group': group_name,
                        'type': 'SERVICEGROUPS',
                        'service': 'catalog.service.entry',
                        'hostname': f'{no_https}__{data["erp_bai_id"].lower().replace(" ", "_")}',
                        'tags': {
                            'hostname': no_https,
                            'info_ID': f'{data["erp_bai_id"].lower().replace(" ", "_") if data["erp_bai_id"] is not None else ""}',
                            'info_ext_catalog_id': f'{data["id"]}',
                            'info_ext_catalog_type': 'resource',
                            'info_ext_catalog_url': f'https://catalogue.ni4os.eu/?_=/resources/{data["id"]}',
                            'info_ext_name': f'{data["erp_bai_name"]}'
                        }
                    })

            # constructed from provider entry and planted as artificial resource
            for pr_data in providers_data:
                resources.append({
                    'group': pr_data['epp_bai_id'],
                    'type': 'SERVICEGROUPS',
                    'service': 'catalog.provider.entry',
                    'hostname': 'agora.ni40s.eu__grnet',
                    'tags': {
                        'hostname': 'agora.ni4os.eu',
                        'info_ID': pr_data['epp_bai_id'],
                        'info_ext_catalog_id': pr_data['id'],
                        'info_ext_catalog_type': 'provider',
                        'info_ext_catalog_url': f'https://catalogue.ni4os.eu/?_=/providers/{pr_data["id"]}',
                        'info_ext_name': pr_data['epp_bai_name']
                    }
                })

            return resources

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, JSONDecodeError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing Agora Resources feed - %s' % (
                self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc
