import datetime
import xml.dom.minidom

from xml.parsers.expat import ExpatError
from argo_connectors.utils import module_class_name
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers


class ParseDowntimes(ParseHelpers):
    def __init__(self, logger, data, start, end, uid=False):
        pass
