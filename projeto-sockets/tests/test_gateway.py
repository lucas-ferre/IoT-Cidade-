"""Regressões de persistência e TCP com SQLite temporário e sockets locais."""

from __future__ import annotations

import asyncio
from contextlib import closing
import importlib.util
from pathlib import Path
import sqlite3
import signal
import struct
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


GATEWAY_DIR = Path(__file__).resolve().parents[1] / "gateway"
sys.path.insert(0, str(GATEWAY_DIR))
sys.path.append(str(GATEWAY_DIR.parent / "client"))
spec = importlib.util.spec_from_file_location("gateway_main", GATEWAY_DIR / "main.py")
gateway = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = gateway
spec.loader.exec_module(gateway)
pb = gateway.messages_pb2
from gateway_transport import GatewayTcpClient  # noqa: E402


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.config = patch.multiple(
            gateway,
            DB_DIR=self.temp.name,
            DB_FILE=str(Path(self.temp.name) / "test.db"),
            TELEMETRY_QUEUE=asyncio.Queue(maxsize=100),
            TELEMETRY_BATCH_MAX_PAYLOADS=2,
            TELEMETRY_BATCH_FLUSH_INTERVAL_SECS=0.01,
        )
        self.config.start()
        gateway.init_db()
        gateway.DB_POOL = gateway.SQLiteConnectionPool(gateway.DB_FILE, 1)
        await gateway.DB_POOL.start()
        self.tasks = []
        self.servers = []
        self.writers = []

    async def asyncTearDown(self):
        for server in self.servers:
            server.close()
        for writer in self.writers:
            writer.close()
            await writer.wait_closed()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        clients = list(gateway._CLIENT_TASKS)
        for task in clients:
            task.cancel()
        await asyncio.gather(*clients, return_exceptions=True)
        for server in self.servers:
            await server.wait_closed()
        if gateway.DB_POOL is not None:
            await gateway.DB_POOL.close()
        gateway.DB_POOL = None
        self.config.stop()
        self.temp.cleanup()

    def payload(self, message_id="A", timestamp=None, value=10.0, device_id="camera_01"):
        payload = pb.DataPayload(
            message_id=message_id,
            timestamp=int(time.time()) if timestamp is None else timestamp,
            device_id=device_id,
            current_status=pb.STATUS_ON,
        )
        payload.metrics.add(name="temperature", value=value, unit="C")
        return payload

    def envelope(self, *args, **kwargs):
        return gateway.build_telemetry_envelope(self.payload(*args, **kwargs), "127.0.0.1")

    async def rows(self, query):
        async with gateway.DB_POOL.connection() as db:
            async with db.execute(query) as cursor:
                return await cursor.fetchall()

    async def test_interleaved_replay_updates_raw_and_rollups_once(self):
        now = int(time.time())
        await gateway.persist_telemetry_batch([
            self.envelope("A", now, 10.0),
            self.envelope("B", now, 20.0),
            self.envelope("A", now, 10.0),
        ])
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM metrics"), [(2,)])
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM telemetry_messages"), [(2,)])
        self.assertEqual(await self.rows("SELECT sample_count, value_sum FROM metrics_rollup_1m"), [(2, 30.0)])

    async def test_replay_and_old_payload_stay_rejected_after_restart(self):
        now = int(time.time())
        await gateway.persist_telemetry_batch([self.envelope("A", now)])
        await gateway.DB_POOL.close()
        gateway.init_db()
        gateway.DB_POOL = gateway.SQLiteConnectionPool(gateway.DB_FILE, 1)
        await gateway.DB_POOL.start()
        await gateway.persist_telemetry_batch([
            self.envelope("A", now), self.envelope("OLD", now - 1),
        ])
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM metrics"), [(1,)])
        self.assertEqual(await self.rows("SELECT sample_count FROM metrics_rollup_1m"), [(1,)])

    async def test_message_identity_is_scoped_to_device(self):
        await gateway.persist_telemetry_batch([
            self.envelope("A", device_id="dev-a"), self.envelope("A", device_id="dev-b"),
        ])
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM metrics"), [(2,)])

    async def test_failed_write_rolls_back_identity_and_can_be_retried(self):
        async with gateway.DB_POOL.connection() as db:
            await db.execute("CREATE TRIGGER fail_write BEFORE INSERT ON metrics BEGIN SELECT RAISE(ABORT, 'failed write'); END")
            await db.commit()
        batch = [self.envelope()]
        with self.assertRaises(sqlite3.IntegrityError):
            await gateway.persist_telemetry_batch(batch)
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM telemetry_messages"), [(0,)])
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM telemetry_state"), [(0,)])
        async with gateway.DB_POOL.connection() as db:
            await db.execute("DROP TRIGGER fail_write")
            await db.commit()
        await gateway.persist_telemetry_batch(batch)
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM metrics"), [(1,)])

    async def test_cancelled_transaction_returns_clean_connection(self):
        with self.assertRaises(asyncio.CancelledError):
            async with gateway.DB_POOL.connection() as db:
                await db.execute("INSERT INTO telemetry_state VALUES ('cancelled', 123)")
                raise asyncio.CancelledError
        async with gateway.DB_POOL.connection() as db:
            self.assertFalse(db.in_transaction)
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM telemetry_state"), [(0,)])

    async def test_worker_retries_without_acknowledging_failed_batch(self):
        persist = gateway.persist_telemetry_batch
        calls = 0
        failed = asyncio.Event()

        async def fail_once(batch):
            nonlocal calls
            calls += 1
            if calls == 1:
                failed.set()
                raise sqlite3.OperationalError("temporary failure")
            await persist(batch)

        gateway.enqueue_telemetry(self.payload(), "127.0.0.1")
        with patch.object(gateway, "persist_telemetry_batch", side_effect=fail_once):
            task = asyncio.create_task(gateway.telemetry_batch_worker_loop())
            self.tasks.append(task)
            await asyncio.wait_for(failed.wait(), 2)
            join = asyncio.create_task(gateway.TELEMETRY_QUEUE.join())
            await asyncio.sleep(0)
            self.assertFalse(join.done())
            await asyncio.wait_for(join, 3)
            await gateway.stop_telemetry_worker(task)
        self.assertEqual(calls, 2)
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM metrics"), [(1,)])

    async def test_shutdown_drains_more_than_one_batch(self):
        for index in range(7):
            gateway.enqueue_telemetry(self.payload(str(index)), "127.0.0.1")
        task = asyncio.create_task(gateway.telemetry_batch_worker_loop())
        self.tasks.append(task)
        await asyncio.wait_for(gateway.stop_telemetry_worker(task), 3)
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM metrics"), [(7,)])
        self.assertTrue(gateway.TELEMETRY_QUEUE.empty())

    async def test_shutdown_has_a_deadline_when_database_stays_unavailable(self):
        gateway.enqueue_telemetry(self.payload(), "127.0.0.1")
        with patch.object(gateway, "TELEMETRY_SHUTDOWN_TIMEOUT_SECS", 0.02), patch.object(
            gateway, "persist_telemetry_batch", side_effect=sqlite3.OperationalError("database unavailable")
        ), self.assertLogs(gateway.log, level="ERROR") as logs:
            task = asyncio.create_task(gateway.telemetry_batch_worker_loop())
            self.tasks.append(task)
            drained = await asyncio.wait_for(gateway.stop_telemetry_worker(task), 2)
        self.assertFalse(drained)
        self.assertTrue(task.done())
        self.assertTrue(any("sem confirmação" in line for line in logs.output))
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM metrics"), [(0,)])

    async def test_sigterm_closes_idle_clients_and_persists_pending_telemetry(self):
        await gateway.DB_POOL.close()
        gateway.DB_POOL = None
        loop = asyncio.get_running_loop()
        handlers = {}
        ready = loop.create_future()
        start_server = asyncio.start_server

        async def capture_server(*args, **kwargs):
            server = await start_server(*args, **kwargs)
            ready.set_result(server)
            return server

        with patch.multiple(gateway, UDP_TELEMETRY_PORT=0, UDP_DISCOVERY_PORT=0, TCP_PORT=0), patch.object(
            loop, "add_signal_handler", side_effect=lambda sig, callback: handlers.update({sig: callback})
        ), patch.object(asyncio, "start_server", side_effect=capture_server):
            task = asyncio.create_task(gateway.main())
            self.tasks.append(task)
            server = await asyncio.wait_for(ready, 2)
            reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
            self.writers.append(writer)
            for index in range(7):
                gateway.enqueue_telemetry(self.payload(str(index)), "127.0.0.1")
            handlers[signal.SIGTERM]()
            await asyncio.wait_for(task, 3)
            self.assertEqual(await asyncio.wait_for(reader.read(), 2), b"")
        self.assertFalse(gateway._CLIENT_TASKS)
        self.assertIsNone(gateway.DB_POOL)
        with closing(sqlite3.connect(gateway.DB_FILE)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM metrics").fetchone(), (7,))

    async def test_cancel_after_commit_cannot_duplicate_batch(self):
        persist = gateway.persist_telemetry_batch
        committed = asyncio.Event()

        async def commit_then_wait(batch):
            await persist(batch)
            committed.set()
            await asyncio.Future()

        gateway.enqueue_telemetry(self.payload(), "127.0.0.1")
        with patch.object(gateway, "persist_telemetry_batch", side_effect=commit_then_wait):
            task = asyncio.create_task(gateway.telemetry_batch_worker_loop())
            self.tasks.append(task)
            await asyncio.wait_for(committed.wait(), 2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.wait_for(gateway.TELEMETRY_QUEUE.join(), 2)
        self.assertEqual(await self.rows("SELECT COUNT(*) FROM metrics"), [(1,)])
        self.assertEqual(await self.rows("SELECT sample_count FROM metrics_rollup_1m"), [(1,)])

    async def start_server(self, handler):
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        self.servers.append(server)
        return server.sockets[0].getsockname()[1]

    async def test_persistent_tcp_responses_correlate_to_each_request(self):
        port = await self.start_server(gateway.handle_client_request)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        self.writers.append(writer)
        for message_id in ("first-request", "second-request"):
            request = pb.ClientRequest(message_id=message_id, timestamp=int(time.time()), type=pb.REQUEST_TYPE_LIST_DEVICES)
            data = request.SerializeToString()
            frame = struct.pack(">I", len(data)) + data
            # TCP fragmentado precisa manter a fronteira de mensagem.
            writer.write(frame[:2])
            await writer.drain()
            writer.write(frame[2:])
            await writer.drain()
            header = await asyncio.wait_for(reader.readexactly(4), 2)
            response = pb.ClientResponse.FromString(await reader.readexactly(struct.unpack(">I", header)[0]))
            self.assertTrue(response.success)
            self.assertEqual(response.message_id, message_id)

    async def test_dashboard_transport_queries_real_gateway_and_database(self):
        now = int(time.time())
        await gateway.persist_telemetry_batch([
            self.envelope("A", now - 1, 10.0), self.envelope("B", now, 20.0),
        ])
        port = await self.start_server(gateway.handle_client_request)
        client = GatewayTcpClient("127.0.0.1", port)
        try:
            inventory = await asyncio.to_thread(client.request, pb.ClientRequest(
                message_id="dashboard-inventory", timestamp=now, type=pb.REQUEST_TYPE_LIST_DEVICES,
            ))
            self.assertTrue(inventory.transport_ok, inventory.error_message)
            self.assertTrue(inventory.response.success)
            self.assertEqual(inventory.response.devices[0].device_id, "camera_01")
            analytics = await asyncio.to_thread(client.request, pb.ClientRequest(
                message_id="dashboard-analytics", timestamp=now,
                type=pb.REQUEST_TYPE_ANALYTICS_QUERY, query_op=pb.OP_AVERAGE,
                query_metric="temperature", start_timestamp=now - 2, end_timestamp=now,
            ))
            self.assertTrue(analytics.transport_ok, analytics.error_message)
            self.assertTrue(analytics.response.success)
            self.assertEqual(analytics.response.analytics_result, 15.0)
            self.assertEqual(len(analytics.response.graph_points), 2)
        finally:
            client.close()

    async def command_ack(self, response, *, update_status=True, update_frequency=False):
        async def actuator(reader, writer):
            try:
                header = await reader.readexactly(4)
                await reader.readexactly(struct.unpack(">I", header)[0])
                data = response.SerializeToString()
                writer.write(struct.pack(">I", len(data)) + data)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        port = await self.start_server(actuator)
        await gateway.process_discovery(pb.DiscoveryResponse(
            device_id="camera_01", type=pb.DEVICE_TYPE_CAMERA,
            control_port=port, is_controllable=True, initial_status=pb.STATUS_ON,
        ), "127.0.0.1")
        request = pb.ClientRequest(
            message_id="client-command", timestamp=int(time.time()),
            type=pb.REQUEST_TYPE_SEND_COMMAND, target_device_id="camera_01",
        )
        request.command_payload.CopyFrom(pb.ConfigCommand(
            command_id="expected-command", timestamp=int(time.time()),
            target_device_id="camera_01", update_status=update_status, target_status=pb.STATUS_ON,
            update_frequency=update_frequency, new_frequency_secs=5,
        ))
        return await gateway.build_client_response(request, "test")

    async def test_command_rejects_ack_for_different_command(self):
        response = await self.command_ack(pb.ConfigResponse(
            command_id="another-command", timestamp=int(time.time()),
            success=True, updated_status=pb.STATUS_ON, updated_frequency_secs=5,
        ))
        self.assertFalse(response.success)
        self.assertEqual(response.message_id, "client-command")
        self.assertIn("command_id", response.message)

    async def test_frequency_ack_can_preserve_valid_error_status(self):
        response = await self.command_ack(pb.ConfigResponse(
            command_id="expected-command", timestamp=int(time.time()),
            success=True, updated_status=pb.STATUS_ERROR, updated_frequency_secs=5,
        ), update_status=False, update_frequency=True)
        self.assertTrue(response.success)

    async def test_ack_must_confirm_requested_changes(self):
        response = await self.command_ack(pb.ConfigResponse(
            command_id="expected-command", timestamp=int(time.time()),
            success=True, updated_status=pb.STATUS_OFF, updated_frequency_secs=5,
        ))
        self.assertFalse(response.success)
        self.assertIn("alterações solicitadas", response.message)

    async def test_historical_query_uses_retained_source_and_explains_aggregation(self):
        now = int(time.time())
        timestamp = ((now - 35 * 86400) // 300) * 300 + 17
        await gateway.persist_telemetry_batch([self.envelope("historic", timestamp, 42.0)])
        async with gateway.DB_POOL.connection() as db:
            await db.execute("DELETE FROM metrics")
            await db.execute("DELETE FROM metrics_rollup_1m")
            await db.commit()
            request = pb.ClientRequest(
                query_metric="temperature", query_op=pb.OP_AVERAGE,
                start_timestamp=timestamp, end_timestamp=timestamp + 60,
            )
            result = await gateway.execute_adaptive_olap(db, request)
        self.assertTrue(result.success)
        self.assertEqual(result.analytics_result, 42.0)
        self.assertIn("rollup_5m", result.result_metadata)
        self.assertIn("Janela agregada", result.result_metadata)


if __name__ == "__main__":
    unittest.main()
