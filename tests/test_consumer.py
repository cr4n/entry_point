import importlib
import json
import os
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


class _PikaAMQPConnectionError(Exception):
    pass


class _PikaAMQPError(Exception):
    pass


class _FakeBlockingConnection:
    def __init__(self, *args, **kwargs):
        self.is_closed = False

    def channel(self):
        return MagicMock()


def _load_consumer_module():
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("POSTGRES_PORT", "5432")
    os.environ.setdefault("POSTGRES_DB", "db")
    os.environ.setdefault("POSTGRES_USER", "user")
    os.environ.setdefault("POSTGRES_PASSWORD", "pass")

    fake_psycopg2 = types.SimpleNamespace(connect=MagicMock())
    fake_pika = types.SimpleNamespace(
        BlockingConnection=_FakeBlockingConnection,
        URLParameters=lambda url: url,
        exceptions=types.SimpleNamespace(
            AMQPConnectionError=_PikaAMQPConnectionError,
            AMQPError=_PikaAMQPError,
        ),
    )

    with patch.dict(sys.modules, {"psycopg2": fake_psycopg2, "pika": fake_pika}):
        if "consumer.consumer" in sys.modules:
            del sys.modules["consumer.consumer"]
        return importlib.import_module("consumer.consumer")


consumer = _load_consumer_module()


class TestConsumerHelpers(unittest.TestCase):
    def test_retry_sleep_seconds_uses_exponential_backoff(self):
        with patch.object(consumer.random, "uniform", return_value=0):
            self.assertEqual(consumer._retry_sleep_seconds(1), consumer.RETRY_BASE_DELAY_SECONDS)
            self.assertEqual(consumer._retry_sleep_seconds(2), consumer.RETRY_BASE_DELAY_SECONDS * 2)

    def test_retry_sleep_seconds_caps_at_max_delay(self):
        with patch.object(consumer.random, "uniform", return_value=0):
            self.assertEqual(consumer._retry_sleep_seconds(999), consumer.RETRY_MAX_DELAY_SECONDS)

    def test_require_env_returns_existing_value(self):
        with patch.dict(os.environ, {"MY_ENV": "value"}, clear=False):
            self.assertEqual(consumer._require_env("MY_ENV"), "value")

    def test_require_env_raises_when_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                consumer._require_env("MISSING_ENV")

    def test_env_int_returns_default_when_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(consumer._env_int("PORT", 1234), 1234)

    def test_env_int_parses_integer(self):
        with patch.dict(os.environ, {"PORT": "9876"}, clear=True):
            self.assertEqual(consumer._env_int("PORT", 1234), 9876)

    def test_env_int_raises_on_invalid_integer(self):
        with patch.dict(os.environ, {"PORT": "not-an-int"}, clear=True):
            with self.assertRaises(ValueError):
                consumer._env_int("PORT", 1234)

    def test_build_insert_query_contains_all_raw_user_operation_fields(self):
        query = consumer._build_raw_user_operations_insert()
        self.assertIn("INSERT INTO pipeline.raw_user_operations", query)
        for column, _ in consumer.RAW_USER_OPERATION_FIELDS:
            self.assertIn(column, query)
        self.assertEqual(query.count("%s"), len(consumer.RAW_USER_OPERATION_FIELDS))


class TestDatabaseFunctions(unittest.TestCase):
    def test_load_csv_to_postgres_copies_data_and_commits(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value

        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", newline="", delete=True) as csv_file:
            csv_file.write("col1,col2\n")
            csv_file.write("a,b\n")
            csv_file.flush()

            consumer.load_csv_to_postgres(csv_file.name, conn, "pipeline.table")

        cursor.copy_from.assert_called_once()
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()

    def test_setup_database_executes_schema_table_and_migrations(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value

        consumer.setup_database(conn)

        executed_statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(any("CREATE SCHEMA IF NOT EXISTS pipeline" in stmt for stmt in executed_statements))
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS pipeline.raw_user_operations" in stmt for stmt in executed_statements))
        for migration in consumer.RAW_USER_OPERATION_MIGRATIONS:
            self.assertIn(migration, executed_statements)
        self.assertEqual(conn.commit.call_count, 2)

    def test_setup_database_rolls_back_and_raises_on_failure(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.execute.side_effect = RuntimeError("db error")

        with self.assertRaises(RuntimeError):
            consumer.setup_database(conn)

        conn.rollback.assert_called_once()

    def test_insert_event_to_db_inserts_ordered_values(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value
        event = {event_key: f"value-{event_key}" for _, event_key in consumer.RAW_USER_OPERATION_FIELDS}

        consumer.insert_event_to_db(conn, event)

        cursor.execute.assert_called_once()
        args, _ = cursor.execute.call_args
        query, values = args
        self.assertIn("INSERT INTO pipeline.raw_user_operations", query)
        self.assertEqual(values, tuple(event[event_key] for _, event_key in consumer.RAW_USER_OPERATION_FIELDS))
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()

    def test_insert_event_to_db_rolls_back_and_raises_on_failure(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.execute.side_effect = RuntimeError("insert failed")
        event = {}

        with self.assertRaises(RuntimeError):
            consumer.insert_event_to_db(conn, event)

        conn.rollback.assert_called_once()


class TestConsumerClass(unittest.TestCase):
    def test_get_db_connection_creates_new_connection_when_missing(self):
        with patch.object(consumer, "connect_to_postgres", return_value=MagicMock(closed=False)) as connect_mock:
            worker = consumer.Consumer(postgres_params={"host": "localhost"})
            conn = worker.get_db_connection()

        self.assertIs(worker.db_conn, conn)
        connect_mock.assert_called_once_with({"host": "localhost"})

    def test_get_db_connection_reuses_existing_connection(self):
        worker = consumer.Consumer(postgres_params={"host": "localhost"})
        worker.db_conn = MagicMock(closed=False)

        with patch.object(consumer, "connect_to_postgres") as connect_mock:
            conn = worker.get_db_connection()

        self.assertIs(conn, worker.db_conn)
        connect_mock.assert_not_called()

    def test_get_db_connection_reconnects_when_closed(self):
        worker = consumer.Consumer(postgres_params={"host": "localhost"})
        worker.db_conn = MagicMock(closed=True)

        with patch.object(consumer, "connect_to_postgres", return_value=MagicMock(closed=False)) as connect_mock:
            conn = worker.get_db_connection()

        self.assertIs(worker.db_conn, conn)
        connect_mock.assert_called_once()

    def test_callback_acknowledges_message_on_success(self):
        worker = consumer.Consumer(postgres_params={"host": "localhost"})
        worker.db_conn = MagicMock(closed=False)

        channel = MagicMock()
        method = types.SimpleNamespace(delivery_tag="tag-1")
        body = json.dumps({"ok": True}).encode("utf-8")

        with patch.object(worker, "get_db_connection", return_value=worker.db_conn), patch.object(
            consumer, "insert_event_to_db"
        ) as insert_mock:
            worker.callback(channel, method, None, body)

        insert_mock.assert_called_once()
        channel.basic_ack.assert_called_once_with(delivery_tag="tag-1")
        channel.basic_nack.assert_not_called()

    def test_callback_acknowledges_invalid_json_without_requeue(self):
        worker = consumer.Consumer(postgres_params={"host": "localhost"})
        channel = MagicMock()
        method = types.SimpleNamespace(delivery_tag="tag-2")

        worker.callback(channel, method, None, b"{invalid json")

        channel.basic_ack.assert_called_once_with(delivery_tag="tag-2")
        channel.basic_nack.assert_not_called()

    def test_callback_nacks_and_resets_connection_on_processing_error(self):
        db_conn = MagicMock(closed=False)
        worker = consumer.Consumer(postgres_params={"host": "localhost"})
        worker.db_conn = db_conn

        channel = MagicMock()
        method = types.SimpleNamespace(delivery_tag="tag-3")
        body = json.dumps({"ok": True}).encode("utf-8")

        with patch.object(worker, "get_db_connection", return_value=db_conn), patch.object(
            consumer, "insert_event_to_db", side_effect=RuntimeError("db down")
        ):
            worker.callback(channel, method, None, body)

        db_conn.close.assert_called_once()
        self.assertIsNone(worker.db_conn)
        channel.basic_nack.assert_called_once_with(delivery_tag="tag-3", requeue=True)
        channel.basic_ack.assert_not_called()

    def test_close_shuts_down_open_db_connection(self):
        db_conn = MagicMock(closed=False)
        worker = consumer.Consumer(postgres_params={"host": "localhost"})
        worker.db_conn = db_conn

        worker.close()

        db_conn.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
