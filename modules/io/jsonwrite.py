import json

class JsonWriter(object):
    def __init__(self, data, filename):
        self.data = data
        self.filename = filename

    def write_json(self):
        try:
            json_data = json.dumps(self.data, indent=4)

            with open(self.filename, 'w') as f:
                f.write(json_data)

            return True, None

        except Exception as e:
            return False, e
