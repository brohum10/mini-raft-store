import tempfile
import unittest

from raftstore.node import Peer, RaftNode


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

    def test_delete_is_replicated_and_replayed_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            node = RaftNode("n1", [], directory)
            node.start_election()
            self.assertEqual((True, "n1"), node.put("temporary", {"nested": True}))
            self.assertEqual((True, "n1"), node.delete("temporary"))
            self.assertNotIn("temporary", node.kv)

            restarted = RaftNode("n1", [], directory)
            self.assertNotIn("temporary", restarted.kv)
            self.assertEqual(node.commit_index, restarted.commit_index)

    def test_linearizable_read_crosses_current_term_barrier(self):
        with tempfile.TemporaryDirectory() as directory:
            node = RaftNode("n1", [], directory)
            node.start_election()
            node.put("answer", 42)
            ok, leader, found, value = node.linearizable_get("answer")
            self.assertEqual((True, "n1", True, 42), (ok, leader, found, value))

    def test_linearizable_read_distinguishes_missing_key(self):
        with tempfile.TemporaryDirectory() as directory:
            node = RaftNode("n1", [], directory)
            node.start_election()
            self.assertEqual((True, "n1", False, None), node.linearizable_get("missing"))

    def test_isolated_leader_cannot_serve_linearizable_read(self):
        with tempfile.TemporaryDirectory() as directory:
            node = RaftNode("n1", [Peer("n2", "http://unreachable")], directory)
            node.term = 1
            node.role, node.leader_id = "leader", "n1"
            node.log = [{"term": 1, "op": "noop"}]
            node.leader_barrier_index = 0
            node.next_index, node.match_index = {"n2": 1}, {"n2": -1}
            node._rpc = lambda *_args, **_kwargs: None

            self.assertEqual((False, "n1", False, None), node.linearizable_get("key", timeout=.05))
