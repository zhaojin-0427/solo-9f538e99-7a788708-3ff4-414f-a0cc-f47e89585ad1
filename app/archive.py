#!/usr/bin/env python3
"""已完成任务的 gzip 结果归档迁移。

可靠性约定：
  * 只处理 ``done`` 任务在 ``out/`` 下的四份结果文件，不碰上传件与统计；
  * 先复制到归档目录内的临时目录，逐文件比较源/副本的大小与 sha256；
  * 归档文件完整且数据库登记（含哈希、大小、时间）提交成功后，
    才删除处理卷上的原件；
  * 任一阶段失败都不删除原件；跨文件系统复制走普通流式读写；
  * 重启时按数据库状态继续未完成的复制/回收，绝不删除有效归档副本；
  * 同一任务用进程内锁串行化，重复/并发归档只产生一份有效归档。

数据库字段（由 app.py 负责建列）：
  archive_status: '' | 'copying' | 'copied' | 'archived'
  archive_meta:   JSON，key -> {name, size, sha256}
  archive_time:   归档登记时间
  archive_error:  最近一次失败信息（成功后清空）
"""
import hashlib
import json
import os
import shutil
import threading
import time
import uuid

import pipeline

COPY_BUF = 1024 * 1024       # 1 MiB 流式复制缓冲，内存占用可忽略
TMP_PREFIX = ".tmp-"          # 归档临时目录前缀（位于归档卷根目录）


class ArchiveError(Exception):
    """可预期的归档失败；消息可直接展示给用户。"""


def _fsync_dir(path):
    """尽力 fsync 目录；目录不存在或平台不支持时忽略。"""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _hash_file(path):
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(COPY_BUF)
            if not chunk:
                break
            size += len(chunk)
            h.update(chunk)
    return size, h.hexdigest()


def _copy_hashed(src, dst):
    """流式复制并计算源数据 sha256；关闭前 fsync 副本。"""
    h = hashlib.sha256()
    size = 0
    with open(src, "rb") as fi, open(dst, "wb") as fo:
        while True:
            chunk = fi.read(COPY_BUF)
            if not chunk:
                break
            h.update(chunk)
            fo.write(chunk)
            size += len(chunk)
        fo.flush()
        os.fsync(fo.fileno())
    return size, h.hexdigest()


class Archiver:
    def __init__(self, jobs_dir, archive_dir, connect_db, logger=None):
        self.jobs_dir = os.path.abspath(jobs_dir)
        self.archive_dir = os.path.abspath(archive_dir)
        self._connect_db = connect_db
        self._log = logger or (lambda *args, **kwargs: None)
        self._locks = {}
        self._locks_guard = threading.Lock()

    # ---------------- 路径 ----------------

    def job_out(self, jid):
        return os.path.join(self.jobs_dir, jid, "out")

    def job_archive(self, jid):
        return os.path.join(self.archive_dir, jid)

    def ensure_dirs(self):
        os.makedirs(self.archive_dir, exist_ok=True)

    def _lock_for(self, jid):
        with self._locks_guard:
            lock = self._locks.get(jid)
            if lock is None:
                lock = self._locks[jid] = threading.Lock()
            return lock

    # ---------------- 数据库 ----------------

    def _set(self, con, jid, expect=None, **fields):
        """更新归档字段；expect 给定时仅在当前状态匹配时更新（并发安全）。"""
        cols = ", ".join(f"{name}=?" for name in fields)
        sql = f"UPDATE jobs SET {cols} WHERE id=?"
        args = [*fields.values(), jid]
        if expect is not None:
            states = (expect,) if isinstance(expect, str) else tuple(expect)
            sql += f" AND archive_status IN ({','.join('?' * len(states))})"
            args.extend(states)
        cur = con.execute(sql, args)
        con.commit()
        return cur.rowcount

    # ---------------- 入口 ----------------

    def claim(self, jid):
        """原子认领归档任务（重复/并发提交只有一个成功）。"""
        con = self._connect_db()
        try:
            cur = con.execute(
                "UPDATE jobs SET archive_status='copying', archive_error='' "
                "WHERE id=? AND status='done' AND "
                "(archive_status IS NULL OR archive_status IN ('','copied'))",
                (jid,))
            con.commit()
            return cur.rowcount == 1
        finally:
            con.close()

    def start(self, jid):
        """认领并启动后台归档线程。

        返回 True 表示本次认领成功；False 表示已在复制中、已归档或状态不允许。
        ``copied``（归档已登记、原件待回收）允许再次触发以继续回收；
        认领用带条件的 UPDATE 完成，并发请求只有一个能成功。
        """
        if not self.claim(jid):
            return False
        threading.Thread(target=self.run, args=(jid,), daemon=True,
                         name=f"archive-{jid}").start()
        return True

    def archive(self, jid):
        """认领并同步执行归档（供测试/运维脚本使用）；未认领返回 False。"""
        if not self.claim(jid):
            return False
        self.run(jid)
        return True

    def recover(self):
        """启动恢复：清理临时目录，继续所有未完成的复制/回收。"""
        self.ensure_dirs()
        self._cleanup_orphan_tmp()
        con = self._connect_db()
        try:
            rows = con.execute(
                "SELECT id FROM jobs WHERE status='done' AND "
                "archive_status IN ('copying','copied')").fetchall()
        finally:
            con.close()
        for row in rows:
            threading.Thread(target=self.run, args=(row["id"],), daemon=True,
                             name=f"archive-recover-{row['id']}").start()
        return len(rows)

    def _cleanup_orphan_tmp(self):
        try:
            entries = os.listdir(self.archive_dir)
        except OSError:
            return
        for name in entries:
            if name.startswith(TMP_PREFIX):
                shutil.rmtree(os.path.join(self.archive_dir, name),
                              ignore_errors=True)

    # ---------------- 状态机 ----------------

    def run(self, jid):
        """执行/继续归档：复制校验 -> 登记 -> 回收原件。"""
        with self._lock_for(jid):
            con = self._connect_db()
            try:
                job = con.execute(
                    "SELECT status, archive_status, archive_meta FROM jobs "
                    "WHERE id=?", (jid,)).fetchone()
                if job is None or job["status"] != "done":
                    return
                state = job["archive_status"] or ""
                if state == "archived":
                    return

                if state == "copying":
                    # 若已有登记且归档完整（上次在回收前中断，用户点“继续”），
                    # 直接进入回收；否则复制校验并登记。
                    meta = self._registered_archive_if_valid(
                        jid, job["archive_meta"])
                    if meta is None:
                        try:
                            meta = self._copy_and_register(jid, con)
                        except Exception as e:
                            con.rollback()
                            # 复制/登记失败：回到“未归档”等待修复后重试。
                            # 保留已有 meta：若旧归档副本仍完好，重试时可复用
                            # 直接回收；若已损坏则重新复制（旧副本会被重建）。
                            try:
                                self._set(con, jid, expect="copying",
                                          archive_status="",
                                          archive_error=str(e))
                            except Exception:
                                self._log("无法重置任务 %s 的归档状态", jid,
                                          exc_info=True)
                            self._log("任务 %s 归档失败: %s", jid, e)
                            return
                elif state == "copied":
                    try:
                        meta = self._load_meta(job["archive_meta"])
                    except ArchiveError as e:
                        self._set(con, jid, expect="copied",
                                  archive_error=str(e))
                        self._log("任务 %s 归档登记不可用: %s", jid, e)
                        return
                else:
                    return

                try:
                    self._recycle_originals(jid, meta)
                except Exception as e:
                    # 归档已登记且完整；只保留“待回收”状态等重启/重试，
                    # 绝不因为回收失败删除归档副本。
                    try:
                        self._set(con, jid, expect=("copying", "copied"),
                                  archive_status="copied",
                                  archive_error=str(e))
                    except Exception:
                        self._log("任务 %s 回收状态写回失败", jid,
                                  exc_info=True)
                    self._log("任务 %s 原件回收失败: %s", jid, e)
                    return
                self._set(con, jid, expect=("copying", "copied"),
                          archive_status="archived", archive_error="")
            finally:
                con.close()

    # ---------------- 复制与登记 ----------------

    def _sources(self, jid):
        out = self.job_out(jid)
        sources = []
        for key, name in pipeline.OUTPUT_FILES.items():
            src = os.path.join(out, name)
            if not os.path.isfile(src):
                raise ArchiveError(f"结果文件缺失，无法归档: {name}")
            sources.append((key, name, src))
        return sources

    def _registered_archive_if_valid(self, jid, meta_text):
        """已登记且归档副本逐字节校验通过时返回 meta，否则返回 None。"""
        if not meta_text:
            return None
        try:
            meta = self._load_meta(meta_text)
            self._verify_registered_archive(self.job_archive(jid), meta)
        except (ArchiveError, OSError):
            return None
        return meta

    def _copy_and_register(self, jid, con):
        sources = self._sources(jid)
        self.ensure_dirs()
        meta = self._make_archive(jid, sources)
        self._register(jid, con, meta)
        return meta

    def _register(self, jid, con, meta):
        """登记归档元数据；只有登记提交成功，后续才可能回收原件。"""
        cur = con.execute(
            "UPDATE jobs SET archive_status='copied', archive_meta=?, "
            "archive_time=?, archive_error='' "
            "WHERE id=? AND archive_status='copying'",
            (json.dumps(meta, ensure_ascii=False, sort_keys=True),
             time.strftime("%Y-%m-%d %H:%M:%S"), jid))
        con.commit()
        if cur.rowcount != 1:
            raise ArchiveError("归档状态已被其他操作改变")

    def _make_archive(self, jid, sources):
        """把四份源文件复制到归档目录。

        若已有完整的同任务归档（例如上次已复制但登记前重启），
        校验与源文件一致后直接复用，避免产生第二份归档。
        """
        final = self.job_archive(jid)
        expected = {name for _, name, _ in sources}

        if os.path.exists(final):
            meta = self._validate_against_sources(final, sources, expected)
            if meta is not None:
                _fsync_dir(self.archive_dir)
                return meta
            self._remove_stale_archive(final, expected)

        tmp = os.path.join(
            self.archive_dir, f"{TMP_PREFIX}{jid}-{uuid.uuid4().hex[:12]}")
        os.makedirs(tmp)
        try:
            meta = {}
            for key, name, src in sources:
                dst = os.path.join(tmp, name)
                size, sha = _copy_hashed(src, dst)
                got_size, got_sha = _hash_file(dst)
                if got_size != size or got_sha != sha:
                    raise ArchiveError(f"归档副本校验失败: {name}")
                meta[key] = {"name": name, "size": size, "sha256": sha}
            _fsync_dir(tmp)
            try:
                os.replace(tmp, final)
            except OSError:
                # 发布时目标已被（另一个进程/上次中断）建立：若它是完整一致
                # 的归档则直接复用，否则保留临时副本待下次重试。
                if os.path.isdir(final):
                    reused = self._validate_against_sources(
                        final, sources, expected)
                    if reused is not None:
                        shutil.rmtree(tmp, ignore_errors=True)
                        _fsync_dir(self.archive_dir)
                        return reused
                raise
            _fsync_dir(self.archive_dir)
            return meta
        except BaseException:
            shutil.rmtree(tmp, ignore_errors=True)
            raise

    def _validate_against_sources(self, path, sources, expected):
        """确认已有归档目录恰含四份文件且与源文件逐字节一致。

        返回 meta（一致）、None（明确不一致，可安全重建）。
        读取出错时抛 ArchiveError（无法确认，必须保留，交由人工/重试）。
        """
        try:
            entries = set(os.listdir(path))
        except OSError as e:
            raise ArchiveError(f"无法读取旧归档目录，已保留原件: {e}") from e
        if entries != expected:
            return None
        meta = {}
        for key, name, src in sources:
            dst = os.path.join(path, name)
            if not os.path.isfile(dst):
                return None
            try:
                ssize, ssha = _hash_file(src)
                dsize, dsha = _hash_file(dst)
            except OSError as e:
                raise ArchiveError(
                    f"校验旧归档时读取出错，已保留原件: {e}") from e
            if ssize != dsize or ssha != dsha:
                return None
            meta[key] = {"name": name, "size": dsize, "sha256": dsha}
        return meta

    def _remove_stale_archive(self, path, expected):
        """删除不完整/不一致的旧归档，但绝不碰未知文件。"""
        try:
            entries = set(os.listdir(path))
        except OSError as e:
            raise ArchiveError(f"无法读取旧归档目录: {e}") from e
        extra = entries - expected
        if extra:
            raise ArchiveError(
                "归档目录存在未知文件，未自动处理: "
                + ", ".join(sorted(extra)))
        for name in expected:
            p = os.path.join(path, name)
            if os.path.isfile(p) or os.path.islink(p):
                os.unlink(p)
        try:
            os.rmdir(path)
        except OSError as e:
            raise ArchiveError(f"无法清理不完整的旧归档目录: {e}") from e
        _fsync_dir(self.archive_dir)

    # ---------------- 登记信息 ----------------

    def _load_meta(self, text):
        try:
            meta = json.loads(text or "{}")
        except (TypeError, ValueError) as e:
            raise ArchiveError("归档登记信息损坏，已保留处理卷原件") from e
        if set(meta) != set(pipeline.OUTPUT_FILES):
            raise ArchiveError("归档登记信息不完整，已保留处理卷原件")
        for key, name in pipeline.OUTPUT_FILES.items():
            rec = meta.get(key)
            if (not isinstance(rec, dict) or rec.get("name") != name
                    or not isinstance(rec.get("size"), int) or rec["size"] < 0
                    or not isinstance(rec.get("sha256"), str)
                    or len(rec["sha256"]) != 64):
                raise ArchiveError("归档登记信息不完整，已保留处理卷原件")
        return meta

    def _verify_registered_archive(self, final, meta):
        """删除原件前的最终校验：目录内容、大小、sha256 全部与登记一致。"""
        try:
            entries = set(os.listdir(final))
        except OSError as e:
            raise ArchiveError(
                f"归档目录不可读，已保留处理卷原件: {e}") from e
        expected = {rec["name"] for rec in meta.values()}
        if entries != expected:
            raise ArchiveError("归档目录内容与登记不一致，已保留处理卷原件")
        for rec in meta.values():
            path = os.path.join(final, rec["name"])
            size, sha = _hash_file(path)
            if size != rec["size"] or sha != rec["sha256"]:
                raise ArchiveError(
                    f"归档副本校验失败，已保留处理卷原件: {rec['name']}")

    # ---------------- 回收原件 ----------------

    def _recycle_originals(self, jid, meta):
        """归档已登记且再次校验通过后才删除 out/ 下的四份原件。"""
        self._verify_registered_archive(self.job_archive(jid), meta)
        out = self.job_out(jid)
        for name in pipeline.OUTPUT_FILES.values():
            try:
                os.unlink(os.path.join(out, name))
            except FileNotFoundError:
                pass
        _fsync_dir(out)
        try:
            os.rmdir(out)  # 四份原件已回收，空目录一并移除
        except OSError:
            pass
