import json
import gzip
import sys

from argo_connectors.config import Global

cglob = Global(sys.argv[0], None)
globopts = cglob.parse()
compress_json = globopts['generalcompressjson']

class JsonWriter(object):
    def __init__(self, data, filename):
        self.data = data
        self.filename = filename

    def write_json(self):
        try:
            if compress_json == 'True':
                json_data = json.dumps(self.data, indent=4)

                with gzip.open(self.filename + '.gz', 'wb') as f:
                    f.write(json_data.encode())

                return True, None
            
            else:
                json_data = json.dumps(self.data, indent=4)

                with open(self.filename, 'w') as f:
                    f.write(json_data)

                return True, None

        except Exception as e:
            return False, e
