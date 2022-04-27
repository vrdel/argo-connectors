from urllib.parse import urlparse
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.base import ParseHelpers


class ParseFlatServiceTypes(ParseHelpers):
    def __init__(self, logger, data):
        self.data = data
        self.logger = logger
        pass

    def get_data(self):
        pass
