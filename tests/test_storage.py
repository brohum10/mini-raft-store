import json
import tempfile
import unittest

from raftstore.storage import Storage, StorageCorruptionError


class StorageTest(unittest.TestCase):
    def test_round_trip_and_valid_json(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Storage(directory)
            state = {"term": 3, "voted_for": "n2", "log": [{"term": 3}], "commit_index": 0}
            store.save(state)
            self.assertEqual(state, store.load())
            with store.path.open() as f:
                self.assertEqual(state, json.load(f))

    def test_reports_corrupt_state_instead_of_starting_from_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Storage(directory)
            store.path.write_text("not-json", encoding="utf-8")
            with self.assertRaisesRegex(StorageCorruptionError, "cannot read Raft state"):
                store.load()

    def test_rejects_wrong_state_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Storage(directory)
            store.path.write_text('{"term":"three","log":[],"commit_index":-1}', encoding="utf-8")
            with self.assertRaisesRegex(StorageCorruptionError, "invalid Raft state schema"):
                store.load()

    def test_rejects_commit_index_past_log(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Storage(directory)
            store.path.write_text('{"term":3,"voted_for":null,"log":[],"commit_index":0}', encoding="utf-8")
            with self.assertRaisesRegex(StorageCorruptionError, "invalid Raft state values"):
                store.load()
