import unittest

from easy_events import Consumer, EventBus, Producer


class EventBusTests(unittest.TestCase):
    def setUp(self):
        self.bus = EventBus()
        self.producer = Producer(self.bus)

    def test_delivers_event_to_matching_consumer(self):
        received = []
        self.bus.subscribe(Consumer("user.created", received.append))

        count = self.producer.publish("user.created", {"id": 1})

        self.assertEqual(count, 1)
        self.assertEqual(received[0].payload, {"id": 1})
        self.assertEqual(received[0].topic, "user.created")

    def test_wildcard_consumer_receives_prefix_topics(self):
        received = []
        self.bus.subscribe(Consumer("orders.*", received.append))

        self.producer.publish("orders.created", {"id": 12})
        self.producer.publish("users.created", {"id": 23})

        self.assertEqual([event.topic for event in received], ["orders.created"])

    def test_unsubscribe_stops_delivery(self):
        received = []
        consumer = self.bus.subscribe(Consumer("ping", received.append))

        self.assertTrue(self.bus.unsubscribe(consumer))
        self.assertEqual(self.producer.publish("ping"), 0)
        self.assertEqual(received, [])


if __name__ == "__main__":
    unittest.main()
