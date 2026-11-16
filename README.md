# 双端 FASTQ 配对修复台

测序质控场景：收到**顺序错乱的双端读段**（R1/R2 两个 gzip FASTQ），本服务将其恢复为
可交给比对器的成对文件，并分离出缺端记录。Flask + SQLite，全流式处理，
容器内存限制 128 MiB 下可处理单端解压后 256 MiB 的输入。

## 功能

- 网页上传 R1、R2 两份 gzip FASTQ（每份解压后 ≤ 256 MiB，超限整批失败）；
- 读段 ID = 标题首个词，去掉末尾 `/1`、`/2`（兼容 `@ID/1` 与 `@ID 1:N:0:XX` 两种标题）；
- **整批校验**：同侧重复 ID、非法四行记录（标题非 `@`、第三行非 `+`、
  序列与质量长度不符、记录残缺、非 gzip 数据）一律整批失败——
  包括文件末尾才出现的错误（先全量校验建索引，后产出结果）；
- 按 ID 配对，两个成对输出均按 **R1 原顺序** 排列；
  缺端记录分别输出 `unpaired_R1` / `unpaired_R2`（各自保持原侧顺序）；
  原始标题、序列、质量值逐字节保留；
- 任务列表/详情页展示状态（排队中/处理中/完成/失败）、配对数与缺端数，
  完成后可下载四个 gzip 结果；
- **可靠性**：结果先写入临时区，全部产出并 fsync、原子 rename、
  SQLite 登记成功后才开放下载；失败不保留任何部分结果；
  服务重启时自动清理中断任务的未完成产物，已完成结果仍可下载。
- **结果归档（详情页“开始归档”）**：把已完成任务的四份 gzip 结果迁移到
  独立的归档卷（`ARCHIVE_DIR`），为处理卷腾空间：
  - 只迁移 `done` 任务 `out/` 下的四份结果；上传件、统计数字、任务号不变；
  - 流式复制到归档卷的临时目录（1 MiB 缓冲）→ 逐文件比对大小与 sha256 →
    写入归档副本的原子 rename → SQLite 登记（元数据含哈希/大小/时间）→
    再次校验归档副本后才删除处理卷原件；任一阶段失败都保留原件；
  - 复制期间、以及原件已回收后，原下载链接始终可用；已打开的下载
    持有文件句柄，回收不会截断它（POSIX 语义），慢下载也不会阻塞
    其他任务的处理或归档；
  - 同一任务重复/并发点击（含多进程）由带条件的 UPDATE 原子认领，
    只产生一份有效归档；已有完整归档会被复用而非重建；
  - 复制中、登记前、回收中任意时刻重启：启动时清理临时目录，按数据库
    状态自动继续；删除原件前会重新核对登记哈希，绝不误删有效副本，
    损坏/不完整的副本不会被当作有效归档；
  - 列表与详情页显示“未归档 / 归档中 / 归档完成·待回收原件 / 已归档”
    及失败原因；`copied`（待回收）可点“继续回收原件”重试。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

- 容器内存上限 128 MiB（`mem_limit`），两个命名卷：
  `fastq_data:/data`（处理卷：上传文件、结果原件、SQLite 登记库）、
  `fastq_archive:/archive`（归档卷：四个结果文件副本）；
- 用 `samples/` 下的 `normal_r1.fastq.gz` / `normal_r2.fastq.gz` 即可体验
  乱序 + 缺端配对；`dup_*`、`badqual_*`、`truncated_*`、`notgzip_*`
  为必失败样例，可验证整批失败行为；
- 上传配对完成后进入任务详情页点“开始归档”即可体验迁移，归档后原
  下载链接照常可用。

## 本地开发运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cd app && DATA_DIR=../data PORT=8000 ../.venv/bin/python app.py
```

流水线也可脱离 Web 独立自测：

```bash
python3 -I app/pipeline.py R1.fastq.gz R2.fastq.gz WORK_DIR OUT_DIR
```

归档相关自动化测试（会真实启动服务、跨文件系统归档、并发/故障注入/重启）：

```bash
python3 tests/test_archive.py      # 需 /dev/shm 与数据目录不同文件系统（默认满足）
```

## 归档目录布局与状态

```
$ARCHIVE_DIR/<任务号>/paired_R1.fastq.gz   # 与处理卷上的原名一致
                     /paired_R2.fastq.gz
                     /unpaired_R1.fastq.gz
                     /unpaired_R2.fastq.gz
$ARCHIVE_DIR/.tmp-<任务号>-<随机>/          # 复制中的临时目录，仅短暂存在
```

| `archive_status` | 页面显示 | 含义 | 原下载链接 |
|---|---|---|---|
| `''` | 未归档 | 尚未迁移 | 读处理卷原件 |
| `copying` | 归档中 | 复制/校验/登记中 | 读处理卷原件 |
| `copied` | 归档完成·待回收原件 | 已登记且副本已校验，原件待删（删除失败或刚重启） | 优先原件 |
| `archived` | 已归档 | 原件已回收，结果只在归档卷 | 读归档卷副本 |

归档登记元数据（`jobs.archive_meta`）保存每份文件的
`name/size/sha256` 与归档时间；删除原件前会重新核对哈希。

## 样例数据

`samples/make_samples.py` 可重新生成全部样例（确定性随机）：

| 样例 | 说明 | 预期 |
|---|---|---|
| `normal_r1/r2.fastq.gz` | 240 对乱序 + 16 条 R1 缺端 + 8 条 R2 缺端 | paired=240, r1_only=16, r2_only=8 |
| `dup_*` | R1 末尾出现重复 ID | 整批失败 |
| `badqual_*` | R1 末尾记录序列/质量长度不符 | 整批失败 |
| `truncated_*` | R1 末尾记录只有 3 行 | 整批失败 |
| `notgzip_*` | 内容不是 gzip | 整批失败 |

## 输出文件

| 文件 | 内容 |
|---|---|
| `paired_R1.fastq.gz` | 有 mate 的 R1 记录，按 R1 原顺序 |
| `paired_R2.fastq.gz` | 对应的 R2 记录，与 paired_R1 同序 |
| `unpaired_R1.fastq.gz` | 缺端 R1（R2 中无配对），R1 原顺序 |
| `unpaired_R2.fastq.gz` | 缺端 R2（R1 中无配对），R2 原顺序 |

## 设计要点（为何能在 128 MiB 内存下工作）

1. **流式校验建索引**：逐行解压（`readline` 带 16 MiB 单行保护上限），
   原始记录字节顺序写入磁盘 records 文件，仅把 `(id, offset, length)`
   登记进临时 SQLite（唯一索引查重），全程不在内存保存读段集合；
2. **游标式配对**：按 R1 的 rowid（即原始顺序）分批游标遍历，
   对 R2 索引做点查，用 `os.pread` 按偏移取回记录写出四个 gzip；
3. **原子发布**：结果先写 `work/`，全部完成并 fsync 后 `rename` 进 `out/`，
   再提交 SQLite 登记；任一步失败即清理，下载接口只对 `done` 任务开放；
4. **启动恢复**：`queued`/`processing` 状态的任务在重启时标记失败并
   删除其 `work/`、`out/`；目录中无登记的任务目录一并清除。
5. **归档迁移**（`app/archive.py`）：1 MiB 缓冲流式复制 → 副本 fsync →
   逐文件 sha256 比对 → 临时目录原子 rename 为 `<任务号>/` →
   SQLite 登记提交 → 再次核对登记哈希 → 才 unlink 原件。
   跨文件系统时 `os.replace` 仅用于同卷上的临时目录→正式目录；
   复制本身用普通读写而非 `copy_file_range`，任何本地/网络卷都可用。

实测：100 万对读段（每侧解压约 209 MiB）处理约 50 秒，
流水线峰值内存 17 MiB，含 Flask 的服务进程峰值约 39 MiB；
归档流程（含哈希核验）测试进程峰值约 51 MiB，仍低于 128 MiB。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `/data`（容器） | 处理卷目录（上传、结果原件、SQLite） |
| `ARCHIVE_DIR` | `/archive`（容器） | 归档目录（必须与 `DATA_DIR` 可独立挂载；本地默认 `../archive`） |
| `PORT` | `8000` | 监听端口 |
| `MAX_DECOMP_BYTES` | `268435456` | 每份文件解压后上限（256 MiB） |
| `MAX_UPLOAD_BYTES` | 上值 + 2 MiB | 上传压缩包大小上限 |
| `SECRET_KEY` | 开发默认值 | Flask 会话密钥，生产应覆盖 |

## 目录结构

```
app/
  app.py            Flask 应用：页面、任务登记、后台处理、下载、归档入口、启动恢复
  archive.py        归档迁移：流式复制、sha256 校验、登记、回收、重启继续
  pipeline.py       流式配对流水线（校验/索引/配对/原子发布）
  templates/        页面模板
samples/            演示样例 + 生成脚本
tests/
  verify_pairing.py 独立校验脚本（比对输入与四个输出）
  make_big.py       大样本生成（压测用）
  test_archive.py   归档迁移端到端测试（并发/跨卷/故障注入/重启/慢下载）
  _archive_worker.py 测试辅助：独立进程归档
Dockerfile / docker-compose.yml / requirements.txt
```

## 已知限制

- 内置 Flask 开发服务器（threaded），适合演示与内部工具；
  生产部署建议换 gunicorn/waitress 并设置 `SECRET_KEY`；
- 仅支持标准四行 FASTQ（不支持序列折行的多行 FASTQ）；
  单行超过 16 MiB 视为非法记录（资源保护）；
- 末尾记录缺少换行符时输出会补一个换行（保证输出为合法 FASTQ），
  其余内容逐字节保留；
- 无任务删除/清理界面，历史任务与文件会一直保留在两个数据卷中；
- 归档为同步哈希校验的整份复制（非硬链接/去重）：归档卷需预留与结果
  同等空间，复制耗时与结果总大小成正比；期间任务详情页自动刷新状态；
- 归档操作在单进程内并发安全，跨进程依靠数据库条件更新保证只认领一份；
  但不提供分布式锁之外的跨主机协调（本应用为单实例部署）。
