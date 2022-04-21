from urllib.parse import urlparse
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.base import ParseHelpers


class ParseGocdbServiceTypes(ParseHelpers):
    def __init__(self, logger, data):
        self.data = data
        self.logger = logger

    def get_data(self):
        all_service_type = list()

        try:
            service_types = self.parse_xml(self.data).getElementsByTagName('SERVICE_TYPE')
            for st in service_types:
                if st.nodeName == 'SERVICE_TYPE':
                    name, desc = None, None
                    for child in st.childNodes:
                        if child.nodeName == 'SERVICE_TYPE_NAME':
                            name = child.childNodes[0].nodeValue
                        if child.nodeName == 'SERVICE_TYPE_DESC':
                            desc = child.childNodes[0].nodeValue
                    all_service_type.append({
                        "name": name,
                        "description": desc
                    })

            return all_service_type

        except (KeyError, IndexError, AttributeError, TypeError, AssertionError) as exc:
            msg = '{} Customer:{} : Error parsing service types feed - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc
