import unittest
import json
import tempfile


class JsonWrite(unittest.TestCase):
    def test_read_file_data(self):
        sample_json = {
            "type": "foo_computationpower",
            "site": "mock_site_1",
            "weight": "0"
            }

        with tempfile.TemporaryFile('w+') as f:

            json_str = json.dumps(sample_json)

            f.write(json_str)
            f.seek(0)

            file_contents = f.read()

            self.assertEqual(
                file_contents, '{"type": "foo_computationpower", "site": "mock_site_1", "weight": "0"}')


if __name__ == '__main__':
    unittest.main()
