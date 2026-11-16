#!/usr/bin/env python3
"""测试辅助：在独立进程中归档指定任务。

用法: _archive_worker.py APP_DIR JOB_ID [--slow]
  --slow  在复制每个数据块后休眠，便于测试在复制中途 SIGKILL。
"""
import hashlib
import os
import sys
import time

sys.path.insert(0, sys.argv[1])
import app as A  # noqa: E402
import archive  # noqa: E402

jid = sys.argv[2]
if "--slow" in sys.argv[3:]:
    def slow_copy(src, dst):
        h = hashlib.sha256()
        size = 0
        with open(src, "rb") as fi, open(dst, "wb") as fo:
            while True:
                chunk = fi.read(64 * 1024)
                if not chunk:
                    break
                h.update(chunk)
                fo.write(chunk)
                fo.flush()
                size += len(chunk)
                time.sleep(1.2)
            os.fsync(fo.fileno())
        return size, h.hexdigest()

    archive._copy_hashed = slow_copy

# 与 HTTP 入口一致：先原子认领，再执行归档状态机
A.archiver.archive(jid)
