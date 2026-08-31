import tempfile
import unittest

from raftstore.node import RaftNode


class NodeTest(unittest.TestCase):
    def test_single_node_elects_and_commits(self):
        with tempfile.TemporaryDirectory() as directory:
            node = RaftNode("n1", [], directory)
            node.start_election()
            self.assertEqual("leader", node.role)
            self.assertEqual((True, "n1"), node.put("language", "python"))
            self.assertEqual("python", node.kv["language"])

    def test_vote_requires_up_to_date_log(self):
        with tempfile.TemporaryDirectory() as directory:
            node = RaftNode("n1", [], directory)
            node.log = [{"term": 2, "op": "set", "key": "a", "value": 1}]
            result = node.request_vote({"term": 3, "candidate_id": "n2", "last_log_index": -1, "last_log_term": 0})
            self.assertFalse(result["vote_granted"])

    def test_append_conflict_repair_and_apply(self):
        with tempfile.TemporaryDirectory() as directory:
            node = RaftNode("n2", [], directory)
            node.term = 2
            node.log = [{"term": 1, "op": "set", "key": "old", "value": 1},
                        {"term": 2, "op": "set", "key": "bad", "value": 1}]
            result = node.append_entries({"term": 3, "leader_id": "n1", "prev_log_index": 0,
                "prev_log_term": 1, "entries": [{"term": 3, "op": "set", "key": "good", "value": 2}],
                "leader_commit": 1})
            self.assertTrue(result["success"])
            self.assertEqual("good", node.log[1]["key"])
            self.assertEqual(2, node.kv["good"])
            self.assertNotIn("bad", node.kv)

