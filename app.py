#!/usr/bin/env python3
"""EasyNVR recording merger and its small local administration UI."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import secrets
import socketserver
import sqlite3
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
SOURCE_ROOT = Path(os.environ.get("SOURCE_ROOT", "/recordings")).resolve()
OUTPUT_ROOT = Path(os.environ.get("OUTPUT_ROOT", "/merged")).resolve()
BASE_PATH = os.environ.get("BASE_PATH", "").rstrip("/")
DB_PATH = DATA_DIR / "easynvr-merge.sqlite3"
SERVICE_LOG_PATH = DATA_DIR / "service.log"
MAX_SERVICE_LOG_BYTES = 10 * 1024 * 1024
STATIC_DIR = Path(__file__).parent / "static"
CSRF_TOKEN = secrets.token_urlsafe(32)
DEFAULT_CONFIG = {
    "source_dir": str(SOURCE_ROOT / "recordings" if SOURCE_ROOT == OUTPUT_ROOT else SOURCE_ROOT),
    "output_dir": str(OUTPUT_ROOT / "output" if SOURCE_ROOT == OUTPUT_ROOT else OUTPUT_ROOT),
    "extension": ".mp4",
    "group_size": 60,
    "sort_mode": "name",
    "enabled": False,
    "start_hour": 1,
    "minute": 15,
}


def now_text() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def db() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with db() as connection:
        connection.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS config (
                id INTEGER PRIMARY KEY CHECK (id = 1), value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY, trigger TEXT NOT NULL,
                started_at TEXT NOT NULL, finished_at TEXT,
                status TEXT NOT NULL, jobs_ok INTEGER NOT NULL DEFAULT 0,
                jobs_failed INTEGER NOT NULL DEFAULT 0, message TEXT
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY, group_key TEXT NOT NULL UNIQUE,
                day TEXT NOT NULL, source_dir TEXT NOT NULL,
                output_path TEXT NOT NULL UNIQUE, status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
                finished_at TEXT, error TEXT
            );
            CREATE TABLE IF NOT EXISTS job_files (
                job_id INTEGER NOT NULL REFERENCES jobs(id),
                path TEXT NOT NULL, size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL, PRIMARY KEY (job_id, path)
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_day ON jobs(day);
            CREATE INDEX IF NOT EXISTS idx_job_files_path ON job_files(path);
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY, created_at TEXT NOT NULL,
                run_id INTEGER REFERENCES runs(id),
                job_id INTEGER REFERENCES jobs(id),
                level TEXT NOT NULL, message TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, id);
        """)
        connection.execute(
            "INSERT OR IGNORE INTO config(id, value) VALUES(1, ?)",
            (json.dumps(DEFAULT_CONFIG, ensure_ascii=False),),
        )
        saved = json.loads(connection.execute(
            "SELECT value FROM config WHERE id=1").fetchone()["value"])
        if "interval_hours" in saved:
            saved.pop("interval_hours")
            connection.execute("UPDATE config SET value=? WHERE id=1",
                               (json.dumps(saved, ensure_ascii=False),))
            add_event(connection, None, None, "info", "执行计划已调整为每天固定时间执行")
        connection.execute(
            "UPDATE runs SET status='interrupted', finished_at=?, "
            "message='服务重启，执行中断' WHERE status='running'",
            (now_text(),),
        )
        connection.execute(
            "UPDATE jobs SET status='interrupted', error='服务重启，待下次重试' "
            "WHERE status='running'"
        )


def get_config() -> dict:
    with db() as connection:
        row = connection.execute("SELECT value FROM config WHERE id=1").fetchone()
    return json.loads(row["value"])


def confined_path(value: str, root: Path) -> Path:
    if not isinstance(value, str) or not value.startswith("/"):
        raise ValueError("目录必须是容器内的绝对路径")
    path = Path(value).resolve()
    if path != root and root not in path.parents:
        raise ValueError(f"目录必须位于 {root} 内")
    return path


def validate_config(value: dict) -> dict:
    if not isinstance(value, dict):
        raise ValueError("配置格式无效")
    source = confined_path(value.get("source_dir"), SOURCE_ROOT)
    output = confined_path(value.get("output_dir"), OUTPUT_ROOT)
    if not source.is_dir():
        raise ValueError(f"录像目录不存在：{source}")
    if source == output or source in output.parents or output in source.parents:
        raise ValueError("录像目录和输出目录不能互相包含")
    if not OUTPUT_ROOT.is_dir():
        raise ValueError(f"输出挂载目录不存在：{OUTPUT_ROOT}")
    extension = value.get("extension")
    if extension not in (".mp4", ".ts"):
        raise ValueError("录像格式仅支持 .mp4 或 .ts")
    group_size = value.get("group_size")
    if type(group_size) is not int or not 1 <= group_size <= 1000:
        raise ValueError("每组合并数量必须为 1–1000")
    sort_mode = value.get("sort_mode")
    if sort_mode not in ("name", "mtime"):
        raise ValueError("排序方式无效")
    enabled = value.get("enabled")
    if type(enabled) is not bool:
        raise ValueError("启用状态无效")
    hour = value.get("start_hour")
    minute = value.get("minute")
    if type(hour) is not int or not 0 <= hour <= 23:
        raise ValueError("起始小时必须为 0–23")
    if type(minute) is not int or not 0 <= minute <= 59:
        raise ValueError("分钟必须为 0–59")
    return {
        "source_dir": str(source), "output_dir": str(output),
        "extension": extension, "group_size": group_size,
        "sort_mode": sort_mode, "enabled": enabled,
        "start_hour": hour, "minute": minute,
    }


def add_event(connection: sqlite3.Connection, run_id: int | None,
              job_id: int | None, level: str, message: str) -> None:
    connection.execute(
        "INSERT INTO events(created_at,run_id,job_id,level,message) VALUES(?,?,?,?,?)",
        (now_text(), run_id, job_id, level, message[:4000]),
    )


def truncate_service_log(force: bool = False) -> bool:
    try:
        with SERVICE_LOG_PATH.open("r+b") as log_file:
            if force or log_file.seek(0, os.SEEK_END) > MAX_SERVICE_LOG_BYTES:
                log_file.truncate(0)
                return True
    except FileNotFoundError:
        pass
    return False


def clear_execution_logs() -> dict:
    connection = db()
    database_compacted = False
    warnings = []
    try:
        with connection:
            runs_deleted = connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
            events_deleted = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            connection.execute("DELETE FROM events")
            connection.execute("DELETE FROM runs")
        # Deleting rows alone does not reduce the SQLite file on disk.
        try:
            connection.execute("VACUUM")
            checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            database_compacted = checkpoint[0] == 0
            if not database_compacted:
                warnings.append("数据库日志文件正在使用，空间稍后回收")
        except sqlite3.Error as exc:
            warnings.append(f"数据库空间回收失败：{exc}")
    finally:
        connection.close()
    try:
        service_log_cleared = truncate_service_log(force=True)
    except OSError as exc:
        service_log_cleared = False
        warnings.append(f"服务日志清空失败：{exc}")
    return {"runs_deleted": runs_deleted, "events_deleted": events_deleted,
            "service_log_cleared": service_log_cleared,
            "database_compacted": database_compacted, "warning": "；".join(warnings)}


def eligible_day_dirs(source: Path, today: str):
    def raise_walk_error(error):
        raise error

    for root, dirs, _ in os.walk(source, followlinks=False, onerror=raise_walk_error):
        dirs[:] = sorted(d for d in dirs if not (Path(root) / d).is_symlink())
        folder = Path(root)
        if re.fullmatch(r"\d{8}", folder.name):
            dirs.clear()
            try:
                datetime.strptime(folder.name, "%Y%m%d")
            except ValueError:
                continue
            if folder.name < today:
                yield folder


def candidate_clips(folder: Path, extension: str, sort_mode: str):
    clips = [p for p in folder.iterdir()
             if p.is_file() and not p.is_symlink()
             and p.suffix.lower() == extension]
    if sort_mode == "mtime":
        return sorted(clips, key=lambda p: (p.stat().st_mtime_ns, p.name))
    return sorted(clips, key=lambda p: p.name)


def group_key(folder: Path, clips: list[Path]) -> str:
    details = [(str(p), p.stat().st_size, p.stat().st_mtime_ns) for p in clips]
    raw = json.dumps([str(folder), details], ensure_ascii=False).encode()
    return hashlib.sha256(raw).hexdigest()


def reserve_job(connection: sqlite3.Connection, day: str, folder: Path,
                clips: list[Path], output_root: Path) -> sqlite3.Row:
    key = group_key(folder, clips)
    existing = connection.execute("SELECT * FROM jobs WHERE group_key=?", (key,)).fetchone()
    if existing:
        return existing
    dest = output_root / day
    dest.mkdir(parents=True, exist_ok=True)
    used = {row[0] for row in connection.execute(
        "SELECT output_path FROM jobs WHERE day=?", (day,))}
    number = 1
    while str(dest / f"Video{number}.mp4") in used or (dest / f"Video{number}.mp4").exists():
        number += 1
    output = dest / f"Video{number}.mp4"
    cursor = connection.execute(
        "INSERT INTO jobs(group_key,day,source_dir,output_path,status,created_at) "
        "VALUES(?,?,?,?,?,?)",
        (key, day, str(folder), str(output), "pending", now_text()),
    )
    for clip in clips:
        stat = clip.stat()
        connection.execute(
            "INSERT INTO job_files(job_id,path,size,mtime_ns) VALUES(?,?,?,?)",
            (cursor.lastrowid, str(clip), stat.st_size, stat.st_mtime_ns),
        )
    return connection.execute("SELECT * FROM jobs WHERE id=?", (cursor.lastrowid,)).fetchone()


def merge_clips(clips: list[Path], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".merge-", dir=output.parent) as temp_name:
        temp = Path(temp_name)
        lines = ["ffconcat version 1.0"]
        for index, clip in enumerate(clips):
            alias = f"{index:05d}{clip.suffix.lower()}"
            (temp / alias).symlink_to(clip)
            lines.append(f"file '{alias}'")
        playlist = temp / "list.ffconcat"
        playlist.write_text("\n".join(lines) + "\n", encoding="utf-8")
        partial = temp / "merged.mp4"
        command = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin",
                   "-f", "concat", "-safe", "1", "-i", str(playlist),
                   "-c", "copy", "-movflags", "+faststart", str(partial)]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError(f"FFmpeg 退出码 {result.returncode}: {result.stderr[-3000:].strip()}")
        validate_video(partial)
        try:
            # The temporary file is on the same mount. A hard link publishes it
            # atomically and fails if another file claimed the name meanwhile.
            os.link(partial, output)
        except FileExistsError as exc:
            raise RuntimeError(f"输出文件已存在，未覆盖：{output}") from exc


def validate_video(path: Path) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise RuntimeError(f"输出文件为空或不存在：{path}")
    result = subprocess.run(
        ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin",
         "-i", str(path), "-map", "0:v:0", "-frames:v", "1", "-f", "null", "-"],
        capture_output=True, text=True,
    )
    if result.returncode:
        raise RuntimeError(f"输出文件校验失败：{result.stderr[-2000:].strip()}")


class Merger:
    def __init__(self):
        self.lock = threading.Lock()
        self.current_run_id: int | None = None

    def start(self, trigger: str) -> bool:
        if not self.lock.acquire(blocking=False):
            return False
        thread = threading.Thread(target=self._run, args=(trigger,), daemon=True)
        thread.start()
        return True

    def _run(self, trigger: str):
        run_id = None
        ok = failed = 0
        try:
            with db() as connection:
                run_id = connection.execute(
                    "INSERT INTO runs(trigger,started_at,status) VALUES(?,?,?)",
                    (trigger, now_text(), "running"),
                ).lastrowid
                add_event(connection, run_id, None, "info", "开始扫描历史日期目录")
        except Exception:
            self.lock.release()
            raise
        self.current_run_id = run_id
        try:
            config = get_config()
            source = confined_path(config["source_dir"], SOURCE_ROOT)
            output_root = confined_path(config["output_dir"], OUTPUT_ROOT)
            if not source.is_dir():
                raise RuntimeError(f"录像目录不存在：{source}")
            today = datetime.now().strftime("%Y%m%d")
            for folder in eligible_day_dirs(source, today):
                try:
                    clips = candidate_clips(folder, config["extension"], config["sort_mode"])
                    with db() as connection:
                        completed = {row[0] for row in connection.execute(
                            "SELECT jf.path FROM job_files jf JOIN jobs j ON j.id=jf.job_id "
                            "WHERE j.status='success' AND j.source_dir=?", (str(folder),))}
                    pending = [p for p in clips if str(p) not in completed]
                    for offset in range(0, len(pending), config["group_size"]):
                        group = pending[offset:offset + config["group_size"]]
                        if self._process_group(run_id, folder, group, output_root):
                            ok += 1
                        else:
                            failed += 1
                except Exception as exc:
                    failed += 1
                    with db() as connection:
                        add_event(connection, run_id, None, "error", f"扫描 {folder} 失败：{exc}")
            status = "failed" if failed else "success"
            message = f"完成：成功 {ok} 组，失败 {failed} 组"
        except Exception as exc:
            status = "failed"
            message = f"执行失败：{exc}"
            failed += 1
            with db() as connection:
                add_event(connection, run_id, None, "error", message)
        finally:
            try:
                with db() as connection:
                    connection.execute(
                        "UPDATE runs SET finished_at=?,status=?,jobs_ok=?,jobs_failed=?,message=? WHERE id=?",
                        (now_text(), status, ok, failed, message, run_id),
                    )
                    add_event(connection, run_id, None,
                              "error" if failed else "info", message)
            finally:
                self.current_run_id = None
                self.lock.release()

    def _process_group(self, run_id: int, folder: Path,
                       clips: list[Path], output_root: Path) -> bool:
        day = folder.name
        with db() as connection:
            job = reserve_job(connection, day, folder, clips, output_root)
            job_id = job["id"]
            output = Path(job["output_path"])
            connection.execute(
                "UPDATE jobs SET status='running', attempts=attempts+1, error=NULL "
                "WHERE id=?", (job_id,))
            add_event(connection, run_id, job_id, "info",
                      f"开始合并 {len(clips)} 段 → {output}")
        try:
            if output.exists():
                # A prior process may have finished the atomic rename before its DB commit.
                if job["status"] not in ("interrupted", "running", "failed"):
                    raise RuntimeError(f"输出文件已存在，未覆盖：{output}")
                validate_video(output)
            else:
                merge_clips(clips, output)
            with db() as connection:
                connection.execute(
                    "UPDATE jobs SET status='success',finished_at=?,error=NULL WHERE id=?",
                    (now_text(), job_id))
                add_event(connection, run_id, job_id, "success",
                          f"合并成功：{output}（{len(clips)} 段）")
            return True
        except Exception as exc:
            error = str(exc)[:4000]
            with db() as connection:
                connection.execute(
                    "UPDATE jobs SET status='failed',finished_at=?,error=? WHERE id=?",
                    (now_text(), error, job_id))
                add_event(connection, run_id, job_id, "error", f"合并失败：{error}")
            return False


def next_run(config: dict) -> str | None:
    if not config["enabled"]:
        return None
    current = datetime.now().astimezone()
    candidate = current.replace(hour=config["start_hour"], minute=config["minute"],
                                second=0, microsecond=0)
    if candidate <= current:
        candidate += timedelta(days=1)
    return candidate.isoformat(timespec="minutes")


def scheduler(merger: Merger):
    while True:
        try:
            config = get_config()
            current = datetime.now().astimezone()
            if (config["enabled"] and current.hour == config["start_hour"] and
                    current.minute == config["minute"]):
                slot = current.strftime("%Y%m%d%H%M%z")
                with db() as connection:
                    old = connection.execute(
                        "SELECT value FROM meta WHERE key='last_slot'").fetchone()
                    if not old or old["value"] != slot:
                        if merger.start("schedule"):
                            connection.execute(
                                "INSERT INTO meta(key,value) VALUES('last_slot',?) "
                                "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (slot,))
        except Exception as exc:
            try:
                with db() as connection:
                    add_event(connection, None, None, "error", f"调度器错误：{exc}")
            except Exception as log_error:
                print(f"调度器错误：{exc}；记录日志失败：{log_error}", flush=True)
        try:
            if truncate_service_log():
                print("服务日志超过 10 MiB，已自动清空。", flush=True)
        except OSError as exc:
            print(f"检查服务日志大小失败：{exc}", flush=True)
        time.sleep(15)


class Handler(BaseHTTPRequestHandler):
    server_version = "EasyNVR-Merger/1.0"

    def log_message(self, format, *args):
        peer = "gateway" if os.environ.get("GATEWAY_MODE") == "1" else self.address_string()
        print("%s - %s" % (peer, format % args), flush=True)

    def authorized(self) -> bool:
        if os.environ.get("GATEWAY_MODE") == "1":
            if self.headers.get("X-Trim-Isadmin", "").lower() == "true":
                return True
            self.json({"error": "仅管理员可访问"}, HTTPStatus.FORBIDDEN)
            return False
        username = os.environ.get("APP_USERNAME", "")
        password = os.environ.get("APP_PASSWORD", "")
        if not username or not password:
            return True
        header = self.headers.get("Authorization", "")
        try:
            scheme, token = header.split(" ", 1)
            pair = base64.b64decode(token, validate=True).decode("utf-8")
            supplied_user, supplied_password = pair.split(":", 1)
            if scheme.lower() == "basic" and hmac.compare_digest(supplied_user, username) and hmac.compare_digest(supplied_password, password):
                return True
        except (ValueError, UnicodeDecodeError, binascii.Error):
            pass
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", 'Basic realm="EasyNVR Merge"')
        self.send_header("Content-Length", "0")
        self.end_headers()
        return False

    def json(self, data, status=HTTPStatus.OK):
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        path = self.route_path()
        if path is None:
            return
        if path == "/health":
            self.json({"ok": True})
            return
        if not self.authorized():
            return
        if path == "/" or path == "/index.html":
            self.serve_file("index.html", "text/html; charset=utf-8")
        elif path == "/app.css":
            self.serve_file("app.css", "text/css; charset=utf-8")
        elif path == "/app.js":
            self.serve_file("app.js", "application/javascript; charset=utf-8")
        elif path == "/icon.png":
            self.serve_file("icon.png", "image/png")
        elif path == "/api/config":
            config = get_config()
            self.json({"config": config, "next_run": next_run(config),
                       "source_root": str(SOURCE_ROOT), "output_root": str(OUTPUT_ROOT),
                       "deployment": "fpk" if os.environ.get("GATEWAY_MODE") == "1" else "docker",
                       "authorization_required": os.environ.get("AUTHORIZATION_REQUIRED") == "1",
                       "csrf_token": CSRF_TOKEN})
        elif path == "/api/status":
            with db() as connection:
                totals = dict(connection.execute(
                    "SELECT COUNT(*) AS total, COALESCE(SUM(status='success'),0) AS success, "
                    "COALESCE(SUM(status='failed'),0) AS failed FROM jobs").fetchone())
                latest = connection.execute(
                    "SELECT * FROM runs ORDER BY id DESC LIMIT 1").fetchone()
            self.json({"running": merger.lock.locked(), "current_run_id": merger.current_run_id,
                       "totals": totals, "latest_run": dict(latest) if latest else None})
        elif path in ("/api/runs", "/api/jobs", "/api/events"):
            query = parse_qs(urlparse(self.path).query)
            try:
                limit = min(max(int(query.get("limit", [50])[0]), 1), 200)
            except ValueError:
                self.json({"error": "limit 参数无效"}, HTTPStatus.BAD_REQUEST)
                return
            with db() as connection:
                if path == "/api/runs":
                    rows = connection.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,))
                elif path == "/api/jobs":
                    rows = connection.execute(
                        "SELECT j.*, COUNT(jf.path) AS clip_count FROM jobs j "
                        "LEFT JOIN job_files jf ON jf.job_id=j.id "
                        "GROUP BY j.id ORDER BY j.id DESC LIMIT ?", (limit,))
                else:
                    rows = connection.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))
                self.json({"items": [dict(row) for row in rows]})
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def serve_file(self, name: str, content_type: str):
        payload = (STATIC_DIR / name).read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        path = self.route_path()
        if path is None:
            return
        if not self.authorized():
            return
        origin = self.headers.get("Origin")
        gateway_mode = os.environ.get("GATEWAY_MODE") == "1"
        if (not hmac.compare_digest(self.headers.get("X-CSRF-Token", ""), CSRF_TOKEN)
                or (not gateway_mode and origin and
                    urlparse(origin).netloc != self.headers.get("Host"))):
            self.json({"error": "跨站请求已拒绝"}, HTTPStatus.FORBIDDEN)
            return
        if self.headers.get("Content-Type", "").split(";", 1)[0] != "application/json":
            self.json({"error": "需要 JSON 请求"}, HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > 16_384 or length < 0:
                raise ValueError("请求内容过大")
            data = json.loads(self.rfile.read(length))
            if path == "/api/config":
                config = validate_config(data)
                with db() as connection:
                    connection.execute("UPDATE config SET value=? WHERE id=1",
                                       (json.dumps(config, ensure_ascii=False),))
                    add_event(connection, None, None, "info", "执行计划配置已更新")
                self.json({"config": config, "next_run": next_run(config)})
            elif path == "/api/run":
                if not merger.start("manual"):
                    self.json({"error": "已有任务正在执行"}, HTTPStatus.CONFLICT)
                else:
                    self.json({"started": True}, HTTPStatus.ACCEPTED)
            elif path == "/api/logs/clear":
                if not merger.lock.acquire(blocking=False):
                    self.json({"error": "任务执行中，请完成后再清空日志"}, HTTPStatus.CONFLICT)
                else:
                    try:
                        self.json(clear_execution_logs())
                    finally:
                        merger.lock.release()
            else:
                self.send_error(HTTPStatus.NOT_FOUND)
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            self.json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except (sqlite3.Error, OSError) as exc:
            self.json({"error": f"存储操作失败：{exc}"}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def route_path(self) -> str | None:
        path = urlparse(self.path).path
        if not BASE_PATH:
            return path
        if path == BASE_PATH:
            self.send_response(HTTPStatus.PERMANENT_REDIRECT)
            self.send_header("Location", BASE_PATH + "/")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return None
        if not path.startswith(BASE_PATH + "/"):
            self.send_error(HTTPStatus.NOT_FOUND)
            return None
        return path[len(BASE_PATH):]


merger = Merger()


class ThreadingUnixHTTPServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


def main():
    if bool(os.environ.get("APP_USERNAME")) != bool(os.environ.get("APP_PASSWORD")):
        raise SystemExit("APP_USERNAME 和 APP_PASSWORD 必须同时设置，或同时留空")
    init_db()
    threading.Thread(target=scheduler, args=(merger,), daemon=True).start()
    socket_path = os.environ.get("SOCKET_PATH")
    if socket_path:
        socket_file = Path(socket_path)
        socket_file.unlink(missing_ok=True)
        server = ThreadingUnixHTTPServer(str(socket_file), Handler)
        socket_file.chmod(0o660)
        print(f"EasyNVR merger listening on {socket_file}", flush=True)
    else:
        port = int(os.environ.get("PORT", "8080"))
        server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        print(f"EasyNVR merger listening on :{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
