import json
import tempfile
import unittest

from raftstore.storage import Storage


class StorageTest(unittest.TestCase):
    def test_round_trip_and_valid_json(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Storage(directory)
            state = {"term": 3, "voted_for": "n2", "log": [{"term": 3}], "commit_index": 0}
            store.save(state)
            self.assertEqual(state, store.load())
            with store.path.open() as f:
                self.assertEqual(state, json.load(f))

