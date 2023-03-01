import unittest
from unittest.mock import patch, mock_open, Mock
import json
import os
import gzip

from argo_connectors.io.jsonwrite import JsonWriter

mock_json = [
    {
        "type": "NGI",
        "group": "iris.ac.uk",
        "subgroup": "dirac-durham",
        "tags": {
            "certification": "Certified",
            "scope": "iris.ac.uk",
            "infrastructure": "Production"
        },
        "notifications": {
            "contacts": [
                "a.g.basden@durham.ac.uk"
            ],
            "enabled": "true"
        }
    }
]

mock_filename = "mock_file.json"


class JsonWriteTest(unittest.TestCase):

    def tearDown(self):
        if os.path.exists('mock_file.json.gz'):
            os.remove('mock_file.json.gz')

    def test_save_data_to_file(self):
        with patch('builtins.open', mock_open()) as m:
            data_writer = JsonWriter(mock_json, 'mock_file.json')
            data_writer.write_json()

        success, error = data_writer.write_json()
        self.assertTrue(success)
        self.assertIsNone(error)

        self.assertTrue(os.path.exists('mock_file.json.gz'))

        with gzip.open('mock_file.json.gz', 'rb') as f:
            compressed_data = f.read()

        decompressed_data = gzip.decompress(compressed_data).decode('utf-8')

        expected_data = json.dumps(mock_json, indent=4)

        self.assertEqual(decompressed_data, expected_data)

    def test_fail_jsonwrite(self):
        mock_jsondumps = Mock(name='json.dumps')
        mock_jsondumps.side_effect = json.JSONDecodeError("Mocked error", '', 0)

        with patch('json.dumps', mock_jsondumps):
            writer = JsonWriter('mock_key: mock_value', mock_filename)
            result, error = writer.write_json()

            self.assertEqual(result, False)
            self.assertEqual(str(error), "Mocked error: line 1 column 1 (char 0)")


if __name__ == '__main__':
    unittest.main()
