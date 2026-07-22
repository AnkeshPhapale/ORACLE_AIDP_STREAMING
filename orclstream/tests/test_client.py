from base64 import b64decode
from types import SimpleNamespace

from orclstream import Consumer, Producer


class Models:
    class PutMessagesDetailsEntry:
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    class PutMessagesDetails:
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    class CreateGroupCursorDetails:
        def __init__(self, **kwargs): self.__dict__.update(kwargs)


class ProducerClient:
    def put_messages(self, stream_id, details):
        self.stream_id, self.details = stream_id, details
        entries = [SimpleNamespace(error=None, offset=index) for index, _ in enumerate(details.messages)]
        return SimpleNamespace(data=SimpleNamespace(entries=entries))


def test_producer_encodes_json_and_key():
    client = ProducerClient()
    producer = Producer(client, "ocid1.stream.oc1.test", models=Models)

    assert producer.produce({"order": 7}, key="order-7") == "0"
    entry = client.details.messages[0]
    assert b64decode(entry.key) == b"order-7"
    assert b64decode(entry.value) == b'{"order":7}'


class ConsumerClient:
    def create_group_cursor(self, stream_id, details):
        self.cursor_details = details
        return SimpleNamespace(data=SimpleNamespace(value="first-cursor"))
    def get_messages(self, stream_id, cursor, limit):
        item = SimpleNamespace(key=None, value="eyJvcmRlciI6N30=", partition="0", offset=12)
        return SimpleNamespace(data=[item], headers={"opc-next-cursor": "next-cursor"})
    def consumer_commit(self, stream_id, cursor):
        self.committed = (stream_id, cursor)


def test_consumer_handles_then_commits():
    client, received = ConsumerClient(), []
    consumer = Consumer(client, "ocid1.stream.oc1.test", "orders", models=Models)

    assert consumer.poll_once(received.append) == 1
    assert received[0].value == {"order": 7}
    assert client.committed == ("ocid1.stream.oc1.test", "next-cursor")
    assert client.cursor_details.commit_on_get is False

