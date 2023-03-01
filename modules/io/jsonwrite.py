import json
import gzip

class JsonWriter(object):
    def __init__(self, data, filename):
        self.data = data
        self.filename = filename

    def write_json(self):
        try:
            json_data = json.dumps(self.data, indent=4)

            compressed_file = gzip.compress(json_data.encode())

            with gzip.open(self.filename + '.gz', 'wb') as f:
                f.write(compressed_file)

            return True, None

        except Exception as e:
            return False, e
