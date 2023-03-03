import json
import gzip

class JsonWriter(object):
    def __init__(self, data, filename):
        self.data = data
        self.filename = filename

    def write_json(self):
        try:
            json_data = json.dumps(self.data, indent=4)

            with gzip.open(self.filename + '.gz', 'wb') as f:
                f.write(json_data.encode())

            return True, None

        except Exception as e:
            return False, e
