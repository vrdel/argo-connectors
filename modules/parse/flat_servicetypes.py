from urllib.parse import urlparse
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.base import ParseHelpers


class ParseFlatServiceTypes(ParseHelpers):
    def __init__(self, logger, data, is_csv=False):
        self.data = data
        self.logger = logger
        self.is_csv = is_csv
        try:
            if is_csv:
                self.data = self.csv_to_json(data)
            else:
                self.data = self.parse_json(data)

        except ConnectorParseError as exc:
            raise exc

    def get_data(self):
        import ipdb; ipdb.set_trace()
