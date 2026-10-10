import unittest
import tempfile
from pathlib import Path
from edge_agent import EdgeBuffer


class StoreAndForwardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "edge.sqlite")
        self.buffer = EdgeBuffer(self.path)

    def tearDown(self):
        self.buffer.close()
        self.tmp.cleanup()

    def test_unconfirmed_batch_survives_restart_and_preserves_key(self):
        self.buffer.append("device-1", 4)
        key, body = self.buffer.next_batch("gateway-1")
        self.buffer.close()
        self.buffer = EdgeBuffer(self.path)
        self.assertEqual((key, body), self.buffer.next_batch("gateway-1"))
        self.assertEqual(1, self.buffer.pending())

    def test_partial_confirmation_never_deletes_readings(self):
        self.buffer.append("device-1", 4)
        key, _ = self.buffer.next_batch("gateway-1")
        with self.assertRaises(ValueError):
            self.buffer.confirm(key, {"accepted": 0, "duplicated": 0})
        self.assertEqual(1, self.buffer.pending())

    def test_duplicate_confirmation_clears_only_confirmed_batch(self):
        self.buffer.append("device-1", 4)
        key, _ = self.buffer.next_batch("gateway-1")
        self.buffer.append("device-1", 5)
        self.buffer.confirm(key, {"accepted": 0, "duplicated": 1})
        self.assertEqual(1, self.buffer.pending())

    def test_sequence_survives_confirmations_and_restart(self):
        self.assertEqual(1, self.buffer.append("device-1", 4)["sequenceNumber"])
        key, _ = self.buffer.next_batch("gateway-1")
        self.buffer.confirm(key, {"accepted": 1, "duplicated": 0})
        self.buffer.close()
        self.buffer = EdgeBuffer(self.path)
        self.assertEqual(2, self.buffer.append("device-1", 4)["sequenceNumber"])

    def test_batch_size_is_bounded_at_500(self):
        for _ in range(501):
            self.buffer.append("device-1", 4)
        self.assertEqual(500, len(self.buffer.next_batch("gateway-1")[1]["readings"]))

    def test_five_minute_outage_preserves_all_fifty_sensor_readings(self):
        for tick in range(30):
            for device in range(50):
                self.buffer.append(
                    str(device),
                    4,
                    recorded_at=f"2026-10-09T20:{tick//6:02d}:{(tick%6)*10:02d}+00:00",
                )
        self.buffer.close()
        self.buffer = EdgeBuffer(self.path)
        self.assertEqual(1500, self.buffer.pending())
        while batch := self.buffer.next_batch("gateway-1"):
            self.buffer.confirm(
                batch[0], {"accepted": len(batch[1]["readings"]), "duplicated": 0}
            )
        self.assertEqual(0, self.buffer.pending())


if __name__ == "__main__":
    unittest.main()
