#!/usr/bin/env python3

import json


class JsonWriter:
    def write_json(self, weights, filename):
        try:
            json_data = json.dumps(weights, indent=4)

            with open(filename, 'w') as f:
                f.write(json_data)
                f.close()

        except Exception as e:
            return False, e

        return True, None
