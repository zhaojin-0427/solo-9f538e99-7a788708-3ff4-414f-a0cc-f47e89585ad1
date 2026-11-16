#!/usr/bin/env python3
"""归档迁移功能的端到端测试。

覆盖：
  * 原有上传/配对/下载流程不变（四个结果逐字节正确）；
  * 完成后归档：四份结果复制到独立卷，字节、统计、任务号不变，
    原件回收后原下载链接仍可读取归档副本；
  * 同任务重复 / 并发归档只产生一份有效归档；
  * 跨文件系统复制（归档目录位于 tmpfs /dev/shm，与处理卷不同文件系统）；
  * 复制失败、登记失败、删除失败、重启中断后原链接仍可读取完整结果；
  * 重启继续未完成的复制/回收，不误删有效副本；
  * 慢下载期间归档完成（原件被回收）下载仍完整，且不阻塞新任务发布；
  * 旧库就地升级、内存峰值低于 128 MiB。

运行：python3 tests/test_archive.py
"""
import hashlib
import json
import os
import resource
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from http.cookiejar import CookieJar as _CookieJar
import uuid
from werkzeug.serving import make_server

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_DIR = os.path.join(ROOT, "app")
sys.path.insert(0, APP_DIR)

TMP_ROOT = tempfile.mkdtemp(prefix="fastq-archive-test-")
DATA_DIR = os.path.join(TMP_ROOT, "data")
ARCHIVE_DIR = os.path.join("/dev/shm", f"fastq-arch-test-{os.getpid()}")
os.environ["DATA_DIR"] = DATA_DIR
os.environ["ARCHIVE_DIR"] = ARCHIVE_DIR
os.environ.setdefault("MAX_DECOMP_BYTES", str(256 * 1024 * 1024))

import app as A            # noqa: E402
import archive as archive_mod   # noqa: E402
import pipeline            # noqa: E402

SAMPLES = os.path.join(ROOT, "samples")
RESULTS = pipeline.OUTPUT_FILES
BASE = ""


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def db_row(jid):
    con = sqlite3.connect(os.path.join(DATA_DIR, "app.db"), timeout=30)
    con.row_factory = sqlite3.Row
    try:
        return con.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    finally:
        con.close()


def db_exec(sql, args=()):
    con = sqlite3.connect(os.path.join(DATA_DIR, "app.db"), timeout=30)
    try:
        con.execute(sql, args)
        con.commit()
    finally:
        con.close()


def wait_for(pred, timeout=60, interval=0.05, what="条件"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = pred()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError(f"等待超时: {what}")


def wait_archive_state(jid, state, timeout=60):
    return wait_for(
        lambda: (db_row(jid)["archive_status"] == state) and db_row(jid),
        timeout=timeout, what=f"{jid} 进入归档状态 {state}")


def http(method, path, data=None, headers=None, timeout=60):
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers=headers or {})
    # 共享 cookie jar，重定向后可以看到 flash 提示
    return _OPENER.open(req, timeout=timeout)


_OPENER = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(_CookieJar()))


def multipart(fields, files):
    boundary = "----test" + uuid.uuid4().hex
    body = bytearray()
    for name, value in fields.items():
        body += (f"--{boundary}\r\nContent-Disposition: form-data; "
                 f'name="{name}"\r\n\r\n{value}\r\n').encode()
    for name, (filename, content) in files.items():
        body += (f"--{boundary}\r\nContent-Disposition: form-data; "
                 f'name="{name}"; filename="{filename}"\r\n'
                 "Content-Type: application/gzip\r\n\r\n").encode()
        body += content
        body += b"\r\n"
    body += f"--{boundary}--\r\n".encode()
    return bytes(body), {"Content-Type":
                         f"multipart/form-data; boundary={boundary}"}


def upload(r1_path, r2_path, timeout=120):
    with open(r1_path, "rb") as fh:
        r1_data = fh.read()
    with open(r2_path, "rb") as fh:
        r2_data = fh.read()
    files = {"r1": (os.path.basename(r1_path), r1_data),
             "r2": (os.path.basename(r2_path), r2_data)}
    body, headers = multipart({}, files)
    resp = http("POST", "/jobs", data=body, headers=headers, timeout=timeout)
    page = resp.read().decode()
    jid = resp.geturl().rstrip("/").split("/")[-1]
    assert jid and jid != "jobs", page[:500]
    return jid


def upload_normal():
    jid = upload(os.path.join(SAMPLES, "normal_r1.fastq.gz"),
                 os.path.join(SAMPLES, "normal_r2.fastq.gz"))
    wait_for(lambda: db_row(jid)["status"] == "done", timeout=120,
             what=f"{jid} 完成")
    return jid


def download_bytes(jid, key, chunk=1 << 16, delay=0.0):
    """下载结果；delay 用于模拟慢客户端。"""
    h = hashlib.sha256()
    size = 0
    with http("GET", f"/jobs/{jid}/download/{key}", timeout=120) as resp:
        while True:
            chunk_data = resp.read(chunk)
            if not chunk_data:
                break
            h.update(chunk_data)
            size += len(chunk_data)
            if delay:
                time.sleep(delay)
    return size, h.hexdigest()


def job_state(jid):
    row = db_row(jid)
    return row["status"], row["archive_status"]


class ArchiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global BASE
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        A.init_and_recover()
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        cls.server = make_server("127.0.0.1", port, A.app, threaded=True)
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                      daemon=True)
        cls.thread.start()
        BASE = f"http://127.0.0.1:{port}"
        wait_for(lambda: _health(), timeout=10, what="服务启动")
        # 确认归档卷与处理卷确实位于不同文件系统（跨文件系统复制）
        assert os.stat(DATA_DIR).st_dev != os.stat(ARCHIVE_DIR).st_dev, (
            "测试需要归档目录位于另一个文件系统")

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
        shutil.rmtree(ARCHIVE_DIR, ignore_errors=True)

    def setUp(self):
        self._orig = {}
        self._patched = []

    def tearDown(self):
        for name, (obj, attr, value) in self._orig.items():
            setattr(obj, attr, value)
        self._patched.clear()

    def patch(self, obj, attr, value):
        self._orig[f"{id(obj)}.{attr}"] = (obj, attr, getattr(obj, attr))
        setattr(obj, attr, value)

    # ---------------- 基础流程 ----------------

    def test_01_pipeline_unchanged(self):
        jid = upload_normal()
        job = db_row(jid)
        self.assertEqual((job["paired"], job["r1_only"], job["r2_only"]),
                         (240, 16, 8))
        with tempfile.TemporaryDirectory() as td:
            for i, (key, name) in enumerate(RESULTS.items(), 1):
                size, digest = download_bytes(jid, key)
                path = os.path.join(td, name)
                with open(path, "wb") as f:
                    f.write(b"")
                # 重新下载到磁盘（小文件，直接取 urlopen 内容）
                with http("GET", f"/jobs/{jid}/download/{key}") as resp:
                    data = resp.read()
                self.assertEqual(hashlib.sha256(data).hexdigest(), digest)
                with open(path, "wb") as f:
                    f.write(data)
            sys.path.insert(0, os.path.join(ROOT, "tests"))
            import verify_pairing
            argv = sys.argv
            sys.argv = ["verify", os.path.join(SAMPLES, "normal_r1.fastq.gz"),
                        os.path.join(SAMPLES, "normal_r2.fastq.gz"),
                        os.path.join(td, RESULTS["paired_r1"]),
                        os.path.join(td, RESULTS["paired_r2"]),
                        os.path.join(td, RESULTS["unpaired_r1"]),
                        os.path.join(td, RESULTS["unpaired_r2"])]
            try:
                with self.assertRaises(SystemExit) as cm:
                    verify_pairing.main()
                self.assertEqual(cm.exception.code, 0)
            finally:
                sys.argv = argv

    def test_02_archive_flow_bytes_and_links(self):
        jid = upload_normal()
        before = dict(db_row(jid))
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {name: sha256(os.path.join(out_dir, name))
                   for name in RESULTS.values()}
        uploads = os.path.join(DATA_DIR, "jobs", jid, "uploads")
        uploads_sha = {n: sha256(os.path.join(uploads, n))
                       for n in os.listdir(uploads)}

        resp = http("POST", f"/jobs/{jid}/archive", data=b"")
        self.assertIn("已开始归档", resp.read().decode())
        row = wait_archive_state(jid, "archived")
        after = dict(row)

        # 统计与任务号不变
        for col in ("id", "created_at", "r1_name", "r2_name", "status",
                    "paired", "r1_only", "r2_only", "message"):
            self.assertEqual(before[col], after[col], col)
        self.assertTrue(after["archive_time"])
        # 归档目录恰含四份文件，逐字节一致
        arch_dir = os.path.join(ARCHIVE_DIR, jid)
        self.assertEqual(sorted(os.listdir(arch_dir)), sorted(RESULTS.values()))
        meta = json.loads(after["archive_meta"])
        for key, name in RESULTS.items():
            path = os.path.join(arch_dir, name)
            self.assertEqual(sha256(path), src_sha[name])
            self.assertEqual(meta[key]["sha256"], src_sha[name])
            self.assertEqual(meta[key]["size"], os.path.getsize(path))
        # 原件已回收，上传件保留
        self.assertFalse(os.path.exists(out_dir))
        self.assertEqual(sorted(os.listdir(uploads)),
                         sorted(["r1.fastq.gz", "r2.fastq.gz"]))
        self.assertEqual({n: sha256(os.path.join(uploads, n))
                          for n in os.listdir(uploads)}, uploads_sha)
        # 原链接仍可下载且内容完整
        for key, name in RESULTS.items():
            size, digest = download_bytes(jid, key)
            self.assertEqual(digest, src_sha[name])
        # 已归档后重复归档被拒绝且不改变状态
        page = http("POST", f"/jobs/{jid}/archive", data=b"").read().decode()
        self.assertIn("已经归档", page)
        self.assertEqual(db_row(jid)["archive_status"], "archived")
        self.assertFalse([n for n in os.listdir(ARCHIVE_DIR)
                          if n.startswith(archive_mod.TMP_PREFIX)])

    def test_03_duplicate_and_concurrent_archive(self):
        jid = upload_normal()
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {n: sha256(os.path.join(out_dir, n))
                   for n in RESULTS.values()}
        results = []

        def post():
            try:
                results.append(http("POST", f"/jobs/{jid}/archive",
                                    data=b"").status)
            except Exception as e:  # noqa: BLE001
                results.append(e)

        threads = [threading.Thread(target=post) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wait_archive_state(jid, "archived")
        self.assertEqual(db_row(jid)["archive_status"], "archived")
        arch_dir = os.path.join(ARCHIVE_DIR, jid)
        self.assertEqual(sorted(os.listdir(arch_dir)), sorted(RESULTS.values()))
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(arch_dir, name)), digest)
        leftovers = [n for n in os.listdir(ARCHIVE_DIR)
                     if n.startswith(archive_mod.TMP_PREFIX)]
        self.assertEqual(leftovers, [], "不应残留归档临时目录")
        self.assertFalse(os.path.exists(out_dir))
        for key, name in RESULTS.items():
            self.assertEqual(download_bytes(jid, key)[1], src_sha[name])

    def test_03b_concurrent_archive_two_processes(self):
        """两个独立进程同时归档同一任务：仍只产生一份有效归档。"""
        jid = upload_normal()
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {n: sha256(os.path.join(out_dir, n))
                   for n in RESULTS.values()}
        worker = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "_archive_worker.py")
        procs = [subprocess.Popen(
            [sys.executable, worker, APP_DIR, jid],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for _ in range(2)]
        outputs = [p.communicate(timeout=120)[0] for p in procs]
        self.assertEqual([p.returncode for p in procs], [0, 0], outputs)
        wait_archive_state(jid, "archived")
        arch_dir = os.path.join(ARCHIVE_DIR, jid)
        self.assertEqual(sorted(os.listdir(arch_dir)), sorted(RESULTS.values()))
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(arch_dir, name)), digest)
        leftovers = [n for n in os.listdir(ARCHIVE_DIR)
                     if n.startswith(archive_mod.TMP_PREFIX)]
        self.assertEqual(leftovers, [])
        self.assertFalse(os.path.exists(out_dir))
        for key, name in RESULTS.items():
            self.assertEqual(download_bytes(jid, key)[1], src_sha[name])

    def test_04_failed_job_not_archivable(self):
        jid = upload(os.path.join(SAMPLES, "dup_r1.fastq.gz"),
                     os.path.join(SAMPLES, "dup_r2.fastq.gz"))
        wait_for(lambda: db_row(jid)["status"] == "failed", timeout=60,
                 what="失败样例")
        page = http("POST", f"/jobs/{jid}/archive", data=b"").read().decode()
        self.assertIn("只有已完成的任务可以归档", page)
        row = db_row(jid)
        self.assertEqual(row["archive_status"], "")
        self.assertFalse(os.path.exists(os.path.join(ARCHIVE_DIR, jid)))
        self.assertFalse(os.path.exists(os.path.join(DATA_DIR, "jobs", jid, "out")))

    # ---------------- 故障注入 ----------------

    def _prepare_claimed(self, jid):
        db_exec("UPDATE jobs SET archive_status='copying' WHERE id=?", (jid,))

    def test_05_copy_failure_keeps_originals(self):
        jid = upload_normal()
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {n: sha256(os.path.join(out_dir, n))
                   for n in RESULTS.values()}
        calls = {"n": 0}
        real = archive_mod._copy_hashed

        def flaky(src, dst):
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError("模拟跨卷复制中断（磁盘错误）")
            return real(src, dst)

        self.patch(archive_mod, "_copy_hashed", flaky)
        self._prepare_claimed(jid)
        A.archiver.run(jid)

        row = db_row(jid)
        self.assertEqual(row["archive_status"], "")
        self.assertIn("模拟跨卷复制中断", row["archive_error"])
        self.assertEqual(sorted(os.listdir(out_dir)), sorted(RESULTS.values()))
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(out_dir, name)), digest)
            self.assertEqual(download_bytes(jid, RESULTS_INV[name])[1], digest)
        # 归档目录不应留下半成品
        self.assertFalse(os.path.exists(os.path.join(ARCHIVE_DIR, jid)))
        self.assertEqual([n for n in os.listdir(ARCHIVE_DIR)
                          if n.startswith(archive_mod.TMP_PREFIX)], [])

        # 故障消失后可以再次归档
        setattr(archive_mod, "_copy_hashed", real)
        self.assertTrue(A.archiver.start(jid))
        wait_archive_state(jid, "archived")
        self.assertFalse(os.path.exists(out_dir))

    def test_06_registration_failure_keeps_originals(self):
        jid = upload_normal()
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {n: sha256(os.path.join(out_dir, n))
                   for n in RESULTS.values()}

        def broken_register(self_, con, jid_, meta):
            raise sqlite3.OperationalError("模拟登记数据库写入失败")

        self.patch(archive_mod.Archiver, "_register", broken_register)
        self._prepare_claimed(jid)
        A.archiver.run(jid)

        row = db_row(jid)
        self.assertEqual(row["archive_status"], "")
        self.assertIn("模拟登记数据库写入失败", row["archive_error"])
        self.assertEqual(sorted(os.listdir(out_dir)), sorted(RESULTS.values()))
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(out_dir, name)), digest)
        # 归档副本完整存在，但尚未登记 -> 仍必须保留原件
        arch_dir = os.path.join(ARCHIVE_DIR, jid)
        self.assertEqual(sorted(os.listdir(arch_dir)), sorted(RESULTS.values()))
        # 原链接可读
        for key, name in RESULTS.items():
            self.assertEqual(download_bytes(jid, key)[1], src_sha[name])

        setattr(archive_mod.Archiver, "_register",
                self._orig[f"{id(archive_mod.Archiver)}._register"][2])
        self.assertTrue(A.archiver.start(jid))
        wait_archive_state(jid, "archived")
        self.assertFalse(os.path.exists(out_dir))

    def test_07_delete_failure_keeps_copy_and_retries(self):
        jid = upload_normal()
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {n: sha256(os.path.join(out_dir, n))
                   for n in RESULTS.values()}

        def broken_recycle(self_, jid_, meta):
            raise OSError("模拟回收原件失败（文件被占用）")

        self.patch(archive_mod.Archiver, "_recycle_originals", broken_recycle)
        self._prepare_claimed(jid)
        A.archiver.run(jid)

        row = db_row(jid)
        self.assertEqual(row["archive_status"], "copied")
        self.assertIn("模拟回收原件失败", row["archive_error"])
        self.assertTrue(os.path.exists(out_dir))
        arch_dir = os.path.join(ARCHIVE_DIR, jid)
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(arch_dir, name)), digest)
            self.assertEqual(download_bytes(jid, RESULTS_INV[name])[1], digest)

        setattr(archive_mod.Archiver, "_recycle_originals",
                self._orig[f"{id(archive_mod.Archiver)}._recycle_originals"][2])
        self.assertTrue(A.archiver.start(jid))
        row = wait_archive_state(jid, "archived")
        self.assertEqual(row["archive_error"], "")
        self.assertFalse(os.path.exists(out_dir))
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(arch_dir, name)), digest)

    # ---------------- 重启恢复 ----------------

    def test_08_recover_mid_copy(self):
        jid = upload_normal()
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {n: sha256(os.path.join(out_dir, n))
                   for n in RESULTS.values()}
        # 模拟“复制完成、登记前崩溃”：归档已完整，数据库仍是 copying
        self._prepare_claimed(jid)
        sources = [(k, n, os.path.join(out_dir, n))
                   for k, n in RESULTS.items()]
        A.archiver._make_archive(jid, sources)
        self.assertEqual(db_row(jid)["archive_status"], "copying")

        A.init_and_recover()  # 重启
        row = wait_archive_state(jid, "archived")
        self.assertEqual(row["status"], "done")
        self.assertFalse(os.path.exists(out_dir))
        arch_dir = os.path.join(ARCHIVE_DIR, jid)
        self.assertEqual(sorted(os.listdir(arch_dir)), sorted(RESULTS.values()))
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(arch_dir, name)), digest)
            self.assertEqual(download_bytes(jid, RESULTS_INV[name])[1], digest)
        self.assertEqual([n for n in os.listdir(ARCHIVE_DIR)
                          if n.startswith(archive_mod.TMP_PREFIX)], [])

    def test_09_recover_mid_recycle(self):
        jid = upload_normal()
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {n: sha256(os.path.join(out_dir, n))
                   for n in RESULTS.values()}
        sources = [(k, n, os.path.join(out_dir, n))
                   for k, n in RESULTS.items()]
        meta = A.archiver._make_archive(jid, sources)
        db_exec("UPDATE jobs SET archive_status='copied', archive_meta=? "
                "WHERE id=?", (json.dumps(meta), jid))

        A.init_and_recover()
        row = wait_archive_state(jid, "archived")
        self.assertEqual(row["archive_status"], "archived")
        self.assertFalse(os.path.exists(out_dir))
        arch_dir = os.path.join(ARCHIVE_DIR, jid)
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(arch_dir, name)), digest)

    def test_10_recover_does_not_trust_corrupt_archive(self):
        jid = upload_normal()
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {n: sha256(os.path.join(out_dir, n))
                   for n in RESULTS.values()}
        sources = [(k, n, os.path.join(out_dir, n))
                   for k, n in RESULTS.items()]
        meta = A.archiver._make_archive(jid, sources)
        db_exec("UPDATE jobs SET archive_status='copied', archive_meta=? "
                "WHERE id=?", (json.dumps(meta), jid))
        victim = os.path.join(ARCHIVE_DIR, jid, RESULTS["paired_r2"])
        with open(victim, "r+b") as f:
            f.seek(20)
            f.write(b"\x00\x00\x00\x00")

        A.init_and_recover()
        row = wait_for(lambda: (db_row(jid)["archive_status"] == "copied"
                                and db_row(jid)["archive_error"]) and db_row(jid),
                       what="损坏副本被识别")
        self.assertIn("保留处理卷原件", row["archive_error"])
        # 原件一个不少且完整；损坏的归档副本未被当作有效副本
        self.assertEqual(sorted(os.listdir(out_dir)), sorted(RESULTS.values()))
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(out_dir, name)), digest)
            self.assertEqual(download_bytes(jid, RESULTS_INV[name])[1], digest)

    def test_11_missing_meta_keeps_originals(self):
        jid = upload_normal()
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        sources = [(k, n, os.path.join(out_dir, n))
                   for k, n in RESULTS.items()]
        A.archiver._make_archive(jid, sources)
        db_exec("UPDATE jobs SET archive_status='copied', archive_meta='' "
                "WHERE id=?", (jid,))
        A.init_and_recover()
        row = wait_for(lambda: db_row(jid)["archive_error"] and db_row(jid),
                       what="登记缺失被识别")
        self.assertEqual(row["archive_status"], "copied")
        self.assertIn("登记信息不完整", row["archive_error"])
        self.assertEqual(sorted(os.listdir(out_dir)), sorted(RESULTS.values()))

    # ---------------- 慢下载 / 并发隔离 ----------------

    def test_12_slow_download_survives_recycle_and_new_job(self):
        big_r1 = os.path.join(TMP_ROOT, "big_r1.fastq.gz")
        big_r2 = os.path.join(TMP_ROOT, "big_r2.fastq.gz")
        subprocess.run([sys.executable, "-I",
                        os.path.join(ROOT, "tests", "make_big.py"),
                        big_r1, big_r2, "60000"], check=True,
                       stdout=subprocess.DEVNULL)
        jid = upload(big_r1, big_r2, timeout=300)
        wait_for(lambda: db_row(jid)["status"] == "done", timeout=300,
                 what="大任务完成")
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {n: sha256(os.path.join(out_dir, n)) for n in RESULTS.values()}

        # 让复制变慢，确保下载进行中归档才完成回收
        def slow_copy(src, dst):
            with open(src, "rb") as fi, open(dst, "wb") as fo:
                h = hashlib.sha256()
                size = 0
                while True:
                    chunk = fi.read(archive_mod.COPY_BUF)
                    if not chunk:
                        break
                    h.update(chunk)
                    fo.write(chunk)
                    size += len(chunk)
                    time.sleep(0.12)
                fo.flush()
                os.fsync(fo.fileno())
            return size, h.hexdigest()

        self.patch(archive_mod, "_copy_hashed", slow_copy)
        downloaded = {}
        dl_errors = []

        def slow_download():
            try:
                downloaded["paired_r1"] = download_bytes(
                    jid, "paired_r1", chunk=1 << 15, delay=0.02)
            except Exception as e:  # noqa: BLE001
                dl_errors.append(e)

        dl = threading.Thread(target=slow_download)
        dl.start()
        time.sleep(0.2)  # 确保下载已打开文件
        http("POST", f"/jobs/{jid}/archive", data=b"")

        # 归档进行期间上传并完成一个新任务（不被慢下载/归档阻塞）
        new_jid = upload_normal()
        self.assertEqual(db_row(new_jid)["status"], "done")

        wait_archive_state(jid, "archived", timeout=120)
        self.assertFalse(os.path.exists(out_dir))
        dl.join(timeout=180)
        self.assertFalse(dl.is_alive())
        self.assertEqual(dl_errors, [])
        self.assertEqual(downloaded["paired_r1"][1], src_sha[RESULTS["paired_r1"]])
        self.assertEqual(downloaded["paired_r1"][0],
                         os.path.getsize(os.path.join(ARCHIVE_DIR, jid,
                                                      RESULTS["paired_r1"])))
        for key, name in RESULTS.items():
            self.assertEqual(download_bytes(jid, key)[1], src_sha[name])

    # ---------------- 兼容 / 资源 ----------------

    def test_13_old_schema_database_upgrades(self):
        jid = upload_normal()
        # 模拟旧版本数据库：删除归档列
        con = sqlite3.connect(os.path.join(DATA_DIR, "app.db"), timeout=30)
        try:
            for col in ("archive_status", "archive_meta", "archive_time",
                        "archive_error"):
                con.execute(f"ALTER TABLE jobs DROP COLUMN {col}")
            con.commit()
        finally:
            con.close()
        A.init_and_recover()  # 启动时应自动补列
        cols = {r[1] for r in sqlite3.connect(
            os.path.join(DATA_DIR, "app.db")).execute("PRAGMA table_info(jobs)")}
        self.assertTrue({"archive_status", "archive_meta", "archive_time",
                         "archive_error"} <= cols)
        row = db_row(jid)
        self.assertEqual(row["status"], "done")
        self.assertEqual(row["archive_status"], "")
        self.assertEqual(row["paired"], 240)
        self.assertTrue(http("POST", f"/jobs/{jid}/archive", data=b"").read())
        wait_archive_state(jid, "archived")

    def test_15_kill9_mid_copy_then_restart(self):
        """真实 SIGKILL：复制中途被杀，重启后清理临时目录并继续归档。"""
        jid = upload_normal()
        out_dir = os.path.join(DATA_DIR, "jobs", jid, "out")
        src_sha = {n: sha256(os.path.join(out_dir, n))
                   for n in RESULTS.values()}
        worker = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "_archive_worker.py")
        proc = subprocess.Popen(
            [sys.executable, worker, APP_DIR, jid, "--slow"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            wait_for(lambda: [n for n in os.listdir(ARCHIVE_DIR)
                              if n.startswith(archive_mod.TMP_PREFIX)],
                     timeout=20, what="归档临时目录出现")
            time.sleep(0.3)  # 确保已进入复制
        finally:
            proc.kill()
            proc.wait(timeout=10)

        # 中断后：任务仍完成态，原件完整且原链接可读
        row = db_row(jid)
        self.assertEqual(row["status"], "done")
        self.assertIn(row["archive_status"], ("copying", ""))
        self.assertEqual(sorted(os.listdir(out_dir)), sorted(RESULTS.values()))
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(out_dir, name)), digest)
            self.assertEqual(download_bytes(jid, RESULTS_INV[name])[1], digest)

        A.init_and_recover()  # 模拟重启
        row = wait_archive_state(jid, "archived", timeout=120)
        self.assertEqual(row["archive_error"], "")
        self.assertFalse(os.path.exists(out_dir))
        arch_dir = os.path.join(ARCHIVE_DIR, jid)
        self.assertEqual(sorted(os.listdir(arch_dir)), sorted(RESULTS.values()))
        for name, digest in src_sha.items():
            self.assertEqual(sha256(os.path.join(arch_dir, name)), digest)
            self.assertEqual(download_bytes(jid, RESULTS_INV[name])[1], digest)
        self.assertEqual([n for n in os.listdir(ARCHIVE_DIR)
                          if n.startswith(archive_mod.TMP_PREFIX)], [])

    def test_14_memory_peak_under_128_mib(self):
        jid = upload_normal()
        before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        http("POST", f"/jobs/{jid}/archive", data=b"").read()
        wait_archive_state(jid, "archived")
        peak_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.assertLess(peak_kib, 128 * 1024,
                        f"内存峰值 {peak_kib} KiB 超过 128 MiB（归档前 {before} KiB）")
        print(f"\n    归档流程内存峰值: {peak_kib / 1024:.1f} MiB", end="")


RESULTS_INV = {name: key for key, name in RESULTS.items()}


def _health():
    try:
        with http("GET", "/healthz", timeout=2) as r:
            return r.status == 200
    except Exception:  # noqa: BLE001
        return False


if __name__ == "__main__":
    unittest.main(verbosity=2)
