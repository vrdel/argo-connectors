import unittest

import datetime

from argo_connectors.log import Logger
from argo_connectors.parse.flat_downtimes import ParseDowntimes
from argo_connectors.exceptions import ConnectorParseError

logger = Logger('test_downfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class ParseCsvDowntimes(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-downtimes.csv', encoding='utf-8') as feed_file:
            downtimes = feed_file.read()
        self.maxDiff = None
        date_2_21_2022 = datetime.datetime(2022, 2, 21)
        start = date_2_21_2022
        end = date_2_21_2022.replace(hour=23, minute=59, second=59)
        self.flat_downtimes = ParseDowntimes(logger, downtimes, start, end, False)

    def test_parseDowntimes(self):
        downtimes = self.flat_downtimes.get_data()
        self.assertEqual(downtimes, [])

if __name__ == '__main__':
    unittest.main()
