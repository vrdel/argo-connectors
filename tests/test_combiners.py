import unittest

from unittest import mock
from unittest.mock import patch

from argo_connectors.exe.combiner import ExecCombiner
from argo_connectors.tasks.combine_topology import TaskCombineTopology


class CombinerTopology(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.patcher1 = patch('argo_connectors.exe.combiner.argparse.ArgumentParser.parse_args')
        argmock = mock.Mock()
        argmock.yamlconf = 'tests/sample-combine.yml'
        mock_parseargs = self.patcher1.start()
        mock_parseargs.return_value = argmock
        exec_combiner = ExecCombiner(
            description="""Combiner tests""",
            exe_script="topology-combiner.py",
            combiner="topology",
        )
        self.topo_combine = TaskCombineTopology(exec_combiner)

    def test_ParseTasks(self):
        pass

    def tearDown(self):
        self.patcher1.stop()


if __name__ == '__main__':
    unittest.main()
