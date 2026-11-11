# 合作报表的批次化分隔文本接入

这是一个只依赖 Python 标准库（3.12+）的批次接收项目。它按显式契约读取不同合作社导出的分隔文本，保留原始来源和逐行证据，把无法解释的记录分区留给数据维护人员，并支持在更正契约后从已保存的原始文件重放。所有随附样例和行内容都是虚构产量数据，不含真实合作社名单。

## 设计要点

- **契约先行，不做格式猜测。** `configs/*.json` 显式声明分隔符、编码、引号规则、注释前缀、列契约、数字分隔符和行错误策略。读取时不使用 `csv.Sniffer`，也不根据第一行反推完整格式。
- **原始来源可重放。** 文件第一次提交时按 SHA-256 保存原始字节到 `runtime/sources/<sha256>.source`，源文件本身只读。重放使用这份已保存的原始字节。
- **批次路径由内容与契约共同决定。** `runtime/batches/<sha256>/<契约身份>/`。同一摘要重复提交（即使文件名改变）返回已有批次，不新增内容；修正契约会产生新的契约身份和新批次目录，旧批次不会被覆盖。
- **逐记录流式读取。** 字节用增量解码器按块解码，缓冲区边界落在多字节字符中间也不会损坏字符；物理行交给 `csv` 组装带换行的引号字段。读取始终按记录推进，不把整个文件载入内存。
- **逐行可追溯。** 接收和拒绝记录都保存 `source_digest`、`line_start`/`line_end`、`raw_fields` 和 `raw_text`。重复表头、注释行和空行进入 `skipped.jsonl` 与审计日志。
- **错误分区。** 拒收记录写入 `rejected.jsonl`，同时按原因码写入 `errors/<原因码>.jsonl`。
- **审计可见批次完整性。** `manifest.json` 的 `status`、`complete`、`published`、`counts`、`rejections` 和 `issues` 说明批次是否完整、哪些行被拒绝、拒绝原因及原文。`audit.jsonl` 记录表头、跳过行、格式变化、拒绝和最终批次决定。
- **格式指纹变化暂停文件。** 起始表头与契约不符、文件中段出现不同表头、出现另一个分隔符、或声明编码无法解码时，批次进入 `quarantined`，不发布任何合格行，而不是把字段硬塞进旧契约。
- **截断文件暂停发布。** 文件在引号字段内结束时，最后一条不完整记录以 `truncated_quoted_field` 拒绝；此前已解析的合格行保留供复核，但批次为 `incomplete` 且不发布。

## 批次状态

| status | complete | published | 含义 |
| --- | --- | --- | --- |
| `complete` | true | true | 所有数据行合格 |
| `partial` | false | 取决于 `row_error_policy` | 有拒绝行；`publish_valid` 发布合格行，`hold_all` 全部暂存 |
| `quarantined` | false | false | 表头/格式/编码与契约不符，暂停整个文件 |
| `incomplete` | false | false | 文件截断或记录解析中断，暂停发布 |

`publish_valid` 是默认策略：合格行可以发布，但 `complete=false`，审计人员能看到批次不完整以及每条拒绝的原文原因。需要整批人工复核时，把契约中的 `row_error_policy` 设为 `hold_all`。

## 数值规范化

数字列保存 `kind`、`raw`、`value`、`has_thousands_separator`、`is_zero`：

| 原文 | kind | value | 说明 |
| --- | --- | --- | --- |
| `""` 或空白 | `null` | null | 空值 |
| `0` / `0.00` / `-0` | `zero` | `0` | 显式零，与空值不同 |
| `1,205` | `thousands` | `1205` | 契约声明了千位分隔符，分组必须合法 |
| `1205` | `number` | `1205` | 普通数字 |
| `1,20` / `abc` | `invalid` | null | 非法数字，该行会被拒绝 |

千位分隔符和小数分隔符都由契约声明（默认 `,` 和 `.`），非法分组不会被静默改写。

## 目录结构

```
app/
  contract.py     # 契约模型、JSON 加载、契约身份
  numbering.py    # 数字规范化
  streaming.py    # 增量解码、物理行号、引号/注释处理
  ingest.py       # 内容寻址存储、批次写入、分区错误、重放
  cli.py          # ingest / reprocess / list / show
  demo.py         # 合成样例端到端演示
  csvsource.py    # 原有的基础读取入口
configs/          # 逗号、分号、单字节编码、供应方空值模板
fixtures/         # 合成输入
tests/            # 单元测试
runtime/          # 运行结果（已加入 .gitignore）
```

每个批次目录包含：

```
manifest.json    # 状态、完整性、计数、拒绝统计、格式问题
accepted.jsonl   # 合格记录（是否发布由 manifest 决定）
rejected.jsonl   # 拒绝记录及原文原因
skipped.jsonl    # 注释行、空行、重复表头
records.jsonl    # 按文件顺序的合格/拒绝记录
audit.jsonl      # 审计事件与最终批次决定
errors/          # 按原因码分区的拒绝记录
```

## 本地运行

```sh
# 全流程演示（重建 runtime/demo/store）
python3 -m app.demo

# 接收一个文件
python3 -m app.cli ingest fixtures/weekly.csv --contract configs/weekly_comma.json

# 查看批次
python3 -m app.cli list
python3 -m app.cli show --run runtime/batches/<digest>/<run>

# 更正契约后从已保存的原文件重放
python3 -m app.cli reprocess \
  --digest sha256:<digest> \
  --contract configs/weekly_semicolon.json

# 测试与编译检查
python3 -m unittest discover -s tests -v
python3 -m compileall -q app tests
```

## 演示覆盖的合成场景

| 文件 | 场景 |
| --- | --- |
| `fixtures/weekly.csv` | 逗号、显式零、千位分隔 |
| `fixtures/bom.csv` | UTF-8 BOM |
| `fixtures/semicolon.csv` | 分号、引号内换行（该行数字因此非法并被拒绝） |
| `fixtures/notes.csv` | 注释行和文件中段的重复表头 |
| `fixtures/truncated.csv` | 引号字段在 EOF 前未闭合 |
| `fixtures/format_change.csv` | 文件中段表头变化，触发暂停 |
| `fixtures/single_byte_cp1252.csv` | 契约声明的本地单字节编码 |
| `fixtures/supplier_empty.csv` | 供应方空值（允许） |
| `fixtures/semicolon_ok.csv` | 用于演示错误契约后更正重放 |

源文件应保持只读；处理结果写入 `runtime/`。若连接真实来源，先按来源方授权范围配置读取权限。
