import json
import os
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import app


class MergerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name).resolve()
        self.source = root / "recordings"
        self.output = root / "merged"
        self.data = root / "data"
        self.source.mkdir()
        self.output.mkdir()
        self.patches = [
            patch.object(app, "DATA_DIR", self.data),
            patch.object(app, "DB_PATH", self.data / "test.sqlite3"),
            patch.object(app, "SERVICE_LOG_PATH", self.data / "service.log"),
            patch.object(app, "SOURCE_ROOT", self.source),
            patch.object(app, "OUTPUT_ROOT", self.output),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)
        app.init_db()
        config = {**app.DEFAULT_CONFIG, "source_dir": str(self.source),
                  "output_dir": str(self.output), "group_size": 60}
        with app.db() as connection:
            connection.execute("UPDATE config SET value=? WHERE id=1", (json.dumps(config),))

    def run_merger(self, fake_merge):
        merger = app.Merger()
        merger.lock.acquire()
        with patch.object(app, "merge_clips", side_effect=fake_merge):
            merger._run("manual")

    def test_prior_day_batches_deduplicate_and_late_arrival(self):
        day = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        prior = self.source / "P100000000001" / "01" / day
        today = self.source / "P100000000001" / "01" / datetime.now().strftime("%Y%m%d")
        prior.mkdir(parents=True)
        today.mkdir(parents=True)
        for index in range(125):
            (prior / f"clip-{index:03d}.mp4").write_bytes(b"clip")
        (today / "clip-000.mp4").write_bytes(b"today")
        sizes = []

        def fake_merge(clips, output):
            sizes.append(len(clips))
            output.write_bytes(b"merged")

        self.run_merger(fake_merge)
        self.assertEqual(sizes, [60, 60, 5])
        self.assertEqual(sorted(p.name for p in (self.output / day).iterdir()),
                         ["Video1.mp4", "Video2.mp4", "Video3.mp4"])
        app.init_db()  # Reopen the persistent database as on service restart.
        self.run_merger(fake_merge)
        self.assertEqual(sizes, [60, 60, 5])
        (prior / "clip-125.mp4").write_bytes(b"late")
        self.run_merger(fake_merge)
        self.assertEqual(sizes, [60, 60, 5, 1])
        self.assertTrue((self.output / day / "Video4.mp4").exists())
        with app.db() as connection:
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='success'").fetchone()[0], 4)
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM job_files").fetchone()[0], 126)

    def test_failure_records_error_and_retries_same_output(self):
        day = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        folder = self.source / "camera" / "01" / day
        folder.mkdir(parents=True)
        (folder / "a.mp4").write_bytes(b"clip")

        def fail(clips, output):
            raise RuntimeError("corrupt input")

        self.run_merger(fail)
        with app.db() as connection:
            job = connection.execute("SELECT * FROM jobs").fetchone()
            self.assertEqual(job["status"], "failed")
            self.assertIn("corrupt input", job["error"])
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM events WHERE level='error'").fetchone()[0], 2)

        app.init_db()
        self.run_merger(lambda clips, output: output.write_bytes(b"merged"))
        with app.db() as connection:
            job = connection.execute("SELECT * FROM jobs").fetchone()
            self.assertEqual(job["status"], "success")
            self.assertEqual(job["attempts"], 2)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1)

    def test_database_failure_does_not_leave_execution_locked(self):
        for fail_on_call in (1, 2):
            merger = app.Merger()
            merger.lock.acquire()
            original_db = app.db
            calls = 0

            def intermittent_db():
                nonlocal calls
                calls += 1
                if calls >= fail_on_call:
                    raise sqlite3.OperationalError("database unavailable")
                return original_db()

            with patch.object(app, "db", side_effect=intermittent_db):
                with self.assertRaises(sqlite3.OperationalError):
                    merger._run("manual")
            self.assertFalse(merger.lock.locked())
            app.init_db()

    def test_paths_are_limited_to_mounted_roots(self):
        config = {**app.DEFAULT_CONFIG, "source_dir": str(self.source),
                  "output_dir": str(self.output)}
        self.assertEqual(app.validate_config(config)["source_dir"], str(self.source))
        config["source_dir"] = str(self.source.parent)
        with self.assertRaises(ValueError):
            app.validate_config(config)

    def test_existing_interval_plan_migrates_to_daily_time(self):
        old = {**app.get_config(), "enabled": True, "interval_hours": 2,
               "start_hour": 3, "minute": 40}
        with app.db() as connection:
            connection.execute("UPDATE config SET value=? WHERE id=1", (json.dumps(old),))
        app.init_db()
        saved = app.get_config()
        self.assertNotIn("interval_hours", saved)
        self.assertEqual((saved["start_hour"], saved["minute"]), (3, 40))
        candidate = datetime.fromisoformat(app.next_run(saved))
        self.assertEqual((candidate.hour, candidate.minute), (3, 40))
        self.assertLessEqual(candidate - datetime.now().astimezone(), timedelta(days=1))

    def test_clearing_logs_reclaims_space_and_preserves_deduplication(self):
        day = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        folder = self.source / "camera" / "01" / day
        folder.mkdir(parents=True)
        (folder / "a.mp4").write_bytes(b"clip")
        merged = []

        def fake_merge(clips, output):
            merged.append(output)
            output.write_bytes(b"merged")

        self.run_merger(fake_merge)
        app.SERVICE_LOG_PATH.write_bytes(b"x" * 1024)
        result = app.clear_execution_logs()
        self.assertEqual(result["runs_deleted"], 1)
        self.assertGreater(result["events_deleted"], 0)
        self.assertTrue(result["database_compacted"])
        self.assertEqual(app.SERVICE_LOG_PATH.stat().st_size, 0)
        with app.db() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM job_files").fetchone()[0], 1)
        app.init_db()
        self.run_merger(fake_merge)
        self.assertEqual(len(merged), 1)

    def test_service_log_is_capped(self):
        app.SERVICE_LOG_PATH.write_bytes(b"x" * 33)
        with patch.object(app, "MAX_SERVICE_LOG_BYTES", 32):
            self.assertTrue(app.truncate_service_log())
        self.assertEqual(app.SERVICE_LOG_PATH.stat().st_size, 0)

    def test_native_gateway_routes_assets_and_limits_access(self):
        socket_path = Path(self.temp.name) / "app.sock"
        with patch.object(app, "BASE_PATH", "/app/easynvr-merger"), \
                patch.dict(os.environ, {"GATEWAY_MODE": "1"}):
            server = app.ThreadingUnixHTTPServer(str(socket_path), app.Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                def response(path, admin=True, method="GET", body=b"", extra_headers=""):
                    with socket.socket(socket.AF_UNIX) as client:
                        client.connect(str(socket_path))
                        headers = (f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n"
                                   f"X-Trim-Isadmin: {str(admin).lower()}\r\n"
                                   f"Content-Length: {len(body)}\r\n"
                                   f"{extra_headers}Connection: close\r\n\r\n")
                        client.sendall(headers.encode() + body)
                        data = bytearray()
                        while chunk := client.recv(65536):
                            data.extend(chunk)
                    return bytes(data)

                self.assertIn(b"308 Permanent Redirect", response("/app/easynvr-merger"))
                self.assertIn(b"200 OK", response("/app/easynvr-merger/"))
                self.assertIn(b"text/css", response("/app/easynvr-merger/app.css"))
                self.assertIn(b'"config"', response("/app/easynvr-merger/api/config"))
                self.assertIn(b"403", response("/app/easynvr-merger/api/config", False))
                self.assertIn(b"404", response("/api/config"))

                config_response = response("/app/easynvr-merger/api/config")
                csrf_token = json.loads(config_response.split(b"\r\n\r\n", 1)[1])["csrf_token"]
                payload = json.dumps({**app.get_config(), "group_size": 30,
                                      "enabled": True}).encode()
                gateway_headers = ("Origin: https://nas.example:5666\r\n"
                                   "Content-Type: application/json\r\n")
                self.assertIn(b"403 Forbidden", response(
                    "/app/easynvr-merger/api/config", method="POST", body=payload,
                    extra_headers=gateway_headers))
                self.assertIn(b"200 OK", response(
                    "/app/easynvr-merger/api/config", method="POST", body=payload,
                    extra_headers=gateway_headers + f"X-CSRF-Token: {csrf_token}\r\n"))
                self.assertEqual(app.get_config()["group_size"], 30)
                self.assertTrue(app.get_config()["enabled"])
                self.assertIsNotNone(app.next_run(app.get_config()))

                manual_headers = gateway_headers + f"X-CSRF-Token: {csrf_token}\r\n"
                with patch.object(app, "merger", app.Merger()):
                    self.assertIn(b"202 Accepted", response(
                        "/app/easynvr-merger/api/run", method="POST", body=b"{}",
                        extra_headers=manual_headers))
                    deadline = time.monotonic() + 3
                    while app.merger.lock.locked() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertFalse(app.merger.lock.locked())

                app.init_db()
                self.assertEqual(app.get_config()["group_size"], 30)
                self.assertTrue(app.get_config()["enabled"])
                self.assertIn(b'"manual"', response("/app/easynvr-merger/api/runs"))
                self.assertIn("执行计划配置已更新".encode(), response(
                    "/app/easynvr-merger/api/events"))
                with app.db() as connection:
                    run = connection.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 1").fetchone()
                    self.assertEqual(run["trigger"], "manual")
                    self.assertEqual(run["status"], "success")
                    self.assertGreaterEqual(connection.execute(
                        "SELECT COUNT(*) FROM events").fetchone()[0], 3)

                app.SERVICE_LOG_PATH.write_bytes(b"service output")
                self.assertIn(b"403 Forbidden", response(
                    "/app/easynvr-merger/api/logs/clear", method="POST", body=b"{}",
                    extra_headers=gateway_headers))
                app.merger.lock.acquire()
                try:
                    self.assertIn(b"409 Conflict", response(
                        "/app/easynvr-merger/api/logs/clear", method="POST", body=b"{}",
                        extra_headers=manual_headers))
                finally:
                    app.merger.lock.release()
                clear_response = response(
                    "/app/easynvr-merger/api/logs/clear", method="POST", body=b"{}",
                    extra_headers=manual_headers)
                self.assertIn(b"200 OK", clear_response)
                self.assertEqual(app.SERVICE_LOG_PATH.stat().st_size, 0)
                self.assertEqual(json.loads(response(
                    "/app/easynvr-merger/api/runs").split(b"\r\n\r\n", 1)[1])["items"], [])
                self.assertEqual(json.loads(response(
                    "/app/easynvr-merger/api/events").split(b"\r\n\r\n", 1)[1])["items"], [])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
