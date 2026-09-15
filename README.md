# MySQL/MariaDB Instance Tester

面向自建 MySQL、MariaDB 和云数据库实例的数据面自动化测试工具。它通过真实 TCP 连接、
MySQL 协议和参数化 SQL，验证连接认证、读写闭环、数据类型、事务、受控并发、索引、可选
权限账号、复制、故障切换观察、性能基线和持续稳定性，并生成脱敏 JSON 报告。

本项目只访问用户提供的已有数据库实例和专用测试 database/schema。它不登录朱雀云控制台，
不调用朱雀云或其他云厂商控制面 API，也不创建、修改或删除云数据库实例。

## 目录概览

```text
.
|-- mysql_mariadb_instance_test.py    # CLI、配置校验、适配器和全部 suite
|-- mysql-mariadb-test.example.json   # 完整且可校验的示例配置
|-- requirements.txt                  # 运行依赖（纯 Python PyMySQL）
|-- requirements-dev.txt              # 开发和测试依赖
|-- pyproject.toml                     # 包元数据和可选命令行入口
|-- tests/
|   |-- test_runner.py                 # 离线 fake database 单元测试
|   `-- test_integration.py            # 显式开启的真实实例集成测试
|-- .github/workflows/tests.yml        # Python 3.8/3.12 普通与优化模式 CI
`-- reports/                           # 默认报告目录，不提交 Git
```

## 最简安装

要求 Python 3.8+。PyMySQL 是纯 Python 驱动，不要求本机编译器或 MySQL 客户端动态库。

### Ubuntu/Debian（推荐）

Ubuntu 默认可能没有 `python` 命令，且部分环境会把 pip 指向无法同步 PyMySQL 的内部镜像。下面的命令会创建隔离虚拟环境，并在安装依赖时明确使用 PyPI：

```bash
git clone https://github.com/Tianwen2000/mysql-mariadb-instance-tester.git
cd mysql-mariadb-instance-tester

sudo apt update
sudo apt install -y python3 python3-pip python3-venv ca-certificates

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip --index-url https://pypi.org/simple
python -m pip install --index-url https://pypi.org/simple -r requirements.txt
python mysql_mariadb_instance_test.py --list-suites
```

激活 `.venv` 后，本文后续示例中的 `python` 和 `python -m pip` 都指向该虚拟环境。新的终端
需要重新激活：

```bash
cd ~/mysql-mariadb-instance-tester
source .venv/bin/activate
```

如果公司网络不能访问官方 PyPI，可将上面两个 `--index-url` 替换为组织批准且已同步
`PyMySQL>=1.1.0,<2` 的镜像，例如：

```bash
python -m pip install --index-url https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt
```

也可以执行 `python -m pip install --index-url https://pypi.org/simple .` 安装命令
`mysql-mariadb-instance-test`。直接运行仓库内脚本则不需要安装项目本身。

## 验证依赖

```bash
python --version
python -c "import pymysql; print(pymysql.__version__)"
python mysql_mariadb_instance_test.py --help
python mysql_mariadb_instance_test.py --list-suites
```

如果没有使用虚拟环境，把上述命令中的 `python` 改成 `python3`，并使用
`python3 -m pip ...` 安装依赖。即使尚未安装 PyMySQL，帮助命令仍可用；真正连接数据库时才
要求驱动。

## 测试前准备

需要准备以下资源：

1. 一台运行测试工具且网络可达数据库接入地址的机器。
2. 一个已有的专用测试 database/schema，例如 `mysql_instance_test`。工具不会创建或删除
   database。
3. 一个只作用于该测试 database 的账号。`standard` 至少需要连接、查询、建表、写入、建索引
   和删表权限。
4. 如果测试 TLS，准备控制台或数据库管理员提供的 CA；双向 TLS 还需要客户端证书和私钥。
5. 如果测试复制，准备明确的主库和只读副本数据面地址；如果观察故障切换，必须使用专用高可用
   测试环境，并由操作者在工具外部触发切换。

先设置密码环境变量，不要把密码放进 JSON 或 Git。下面的 `replace-with-secret` 只是占位符，
必须替换成 MariaDB 数据库账号的实际密码（不是朱雀云控制台登录密码）；`export` 只对当前
终端会话有效：

```bash
export MARIADB_PASSWORD='replace-with-secret'
```

默认配置和示例配置都读取名为 `MARIADB_PASSWORD` 的变量，因此设置后还需要在测试命令中使用
`--password-env MARIADB_PASSWORD`（或沿用默认配置）：

```bash
python mysql_mariadb_instance_test.py \
  --host 10.0.1.15 --port 3306 \
  --database mysql_instance_test \
  --username test_user \
  --password-env MARIADB_PASSWORD \
  --profile connectivity
```

更安全的方式是不把密码直接写进 shell 历史，而是在当前终端隐藏输入：

```bash
read -r -s -p "Database password: " MARIADB_PASSWORD
printf '\n'
export MARIADB_PASSWORD
```

也可以不设置环境变量，让工具临时提示输入密码：

```bash
python mysql_mariadb_instance_test.py \
  --set authentication.mode=prompt \
  --host 10.0.1.15 --port 3306 \
  --database mysql_instance_test \
  --username test_user \
  --profile connectivity
```

PowerShell：

```powershell
$env:MARIADB_PASSWORD = 'replace-with-secret'
```

建议先复制示例配置并修改连接信息。`execution.cleanup_policy=always` 是默认值：无论 suite 成功
还是失败，都尝试删除本次生成的测试表。

## 参数说明

CLI 参数：

| 参数 | 含义 |
| --- | --- |
| `--config PATH` | JSON 配置文件；未声明项继承内置默认值 |
| `--host HOST` / `--port PORT` | 数据库连接地址和端口 |
| `--database NAME` | 已有的专用测试 database/schema |
| `--username USER` | 登录用户名 |
| `--password-env NAME` | 保存密码的环境变量名称，不是密码本身 |
| `--ssl-ca PATH` | CA 路径，并把 `ssl_mode` 设为 `verify_ca` |
| `--profile NAME` | `connectivity`、`smoke`、`standard`、`performance`、`soak` 或 `ha` |
| `--suites A,B` | 精确选择 suite，覆盖 profile |
| `--set SECTION.OPTION=VALUE` | 覆盖任意已声明配置，可重复；值接受 JSON 类型或普通字符串 |
| `--duration-seconds N` | 覆盖 `soak.duration_seconds`；`0` 表示直到 `Ctrl+C` |
| `--report PATH` | 指定 JSON 报告路径；默认写入 `reports/` |
| `--list-suites` | 列出 profile、suite 及写表提示 |

主要配置参数：

| 配置 | 默认值 | 含义和安全限制 |
| --- | ---: | --- |
| `connection.connect_timeout_seconds` | `5` | TCP/握手连接时限 |
| `connection.read_timeout_seconds` / `write_timeout_seconds` | `10` | 单次 socket 读写时限 |
| `connection.charset` | `utf8mb4` | 会话字符集 |
| `connection.ssl_mode` | `disabled` | `disabled`、`preferred`、`required`、`verify_ca` 或 `verify_identity` |
| `execution.namespace` | `zhuque_mysql_test` | 所有测试表名的命名空间，只允许字母、数字和下划线 |
| `execution.cleanup_policy` | `always` | `always`、`on_success` 或 `never` |
| `sql.rows` / `sql.value_size` | `50` / `256` | 功能测试行数和 payload 字节数，均有硬上限 |
| `sql.statement_timeout_seconds` | `30` | 并发 suite 的总体保护时限；单条 SQL 另受连接读写时限约束 |
| `concurrency.workers` / `connections` | `4` / `4` | 有界线程和独立连接上限，worker 不得超过 connection |
| `concurrency.operations` / `max_in_flight` | `100` / `8` | 并发操作总数和配置安全上限 |
| `performance.rows` / `workers` / `batch_size` | `1000` / `4` / `50` | 性能样本数据量、并发和批量大小 |
| `performance.duration_seconds` | `60` | 性能 suite 的安全截止时间，不是通用 SLA |
| `performance.min_tps` | `null` | 可选最小总操作 TPS；默认不设置门槛 |
| `performance.max_p95_ms` / `max_p99_ms` | `null` | 可选延迟门槛；默认不设置门槛 |
| `replication.primary_endpoint` / `replica_endpoint` | `null` | 显式的 `HOST:PORT` 主从数据面地址 |
| `soak.workers` / `operation_interval_seconds` | `3` / `1` | 持续模式并发和每个 worker 的操作间隔 |
| `soak.read_write_ratio` | `0.7` | 读操作概率，范围 `0..1` |
| `soak.max_operations` | `0` | `0` 表示不以操作数停止；可设置硬上限 |
| `soak.max_rows` | `10000` | 固定 upsert 槽位数，防止长期运行时测试表无限增长 |
| `soak.max_latency_samples` | `10000` | 固定长度延迟窗口，内存不随总操作数增长 |

全部默认项及类型可查看
[`mysql-mariadb-test.example.json`](mysql-mariadb-test.example.json)。未知 section、未知 option、
错误类型、越界数据量、不完整 TLS 证书对和危险并发组合都会在连接前拒绝，并给出具体配置路径。

## 在朱雀云控制台查找参数

从 <https://console.zhuque.jp/mariadb/ins/create> 进入 MariaDB 产品后，应在**已有实例**的详情页
查找数据面连接参数，不要在本工具中创建实例。控制台版本和实例类型不同，菜单名称可能略有差异：

| 本工具参数 | 通常查找位置 |
| --- | --- |
| `--host`、`--port` | 实例详情的“连接信息/内外网地址”，并确认“网络访问/白名单/安全组”允许测试机访问 |
| `--database` | 实例内由管理员准备的专用测试 database；它不是实例名称 |
| `--username` | “账号管理/数据库账号”中获得的专用测试账号 |
| `--password-env` | 本地环境变量名；密码值来自账号创建或重置流程，不应写入配置 |
| `--ssl-ca` | “SSL 设置/下载证书/连接指引”提供的 CA 文件 |
| 主库/从库地址 | “只读实例/复制实例/连接信息”中分别取得的数据面地址 |
| 高可用地址 | “高可用配置/连接信息”中的稳定接入地址；本工具不操作切换按钮 |

内网和公网地址同时存在时，使用测试机所在网络真正可达的地址。网络白名单、安全组、VPC 路由
和 DNS 均属于运行前提，本工具只报告数据面连接结果，不读取控制台规则。

## 快速连接测试

`connectivity` 不创建表，验证 TCP、握手、用户名密码、`SELECT 1`、版本、字符集和 TLS 会话状态：

```bash
python mysql_mariadb_instance_test.py \
  --host 10.0.1.15 --port 3306 \
  --database mysql_instance_test \
  --username test_user --password-env MARIADB_PASSWORD \
  --profile connectivity
```

可增加版本期望：

```bash
--set expectations.expected_server_family=mariadb \
--set expectations.expected_version_prefix=10.11.
```

## 标准数据面验收

```bash
python mysql_mariadb_instance_test.py \
  --config mysql-mariadb-test.example.json \
  --profile standard
```

`standard` 依次运行 `connectivity`、`authentication`、`sql_roundtrip`、`datatype`、
`transaction`、`concurrency` 和 `index`。测试表名包含 namespace、prefix、run_id、suite 和随机
后缀，最多 64 字符；框架不会枚举、更新或删除已有业务表。

权限 suite 不在 standard 中。只有准备专用角色账号后才显式执行：

```bash
export DB_READONLY_PASSWORD='replace-with-secret'
python mysql_mariadb_instance_test.py \
  --config mysql-mariadb-test.example.json \
  --suites permissions \
  --set permissions.enabled=true \
  --set permissions.read_only_username=acceptance_reader \
  --set permissions.read_only_password_env=DB_READONLY_PASSWORD
```

可以同样配置 `read_write_*` 和 `no_access_*`。没有专用账号时结果是 `SKIP`，不会拿业务账号
猜测权限。

## 性能测试

先不设阈值，多次运行建立同一测试机、网络、实例规格下的基线：

```bash
python mysql_mariadb_instance_test.py \
  --config mysql-mariadb-test.example.json \
  --profile performance \
  --set performance.rows=20000 \
  --set performance.workers=4 \
  --set performance.batch_size=200
```

有稳定基线后再设置项目自己的回归门槛：

```bash
--set performance.min_tps=1000 \
--set performance.max_p95_ms=20 \
--set performance.max_p99_ms=50
```

性能 suite 统计批量写、主键查询、总 TPS、平均延迟、p50/p95/p99 和错误率。延迟窗口有固定
上限；`duration_seconds` 是安全截止时间。结果只适合固定条件下回归比较，不等同于专业容量压测，
示例阈值也不是所有实例的通用标准。

## 持续 soak 测试

默认持续运行到 `Ctrl+C`：

```bash
python mysql_mariadb_instance_test.py \
  --config mysql-mariadb-test.example.json \
  --profile soak
```

固定运行一小时：

```bash
python mysql_mariadb_instance_test.py \
  --config mysql-mariadb-test.example.json \
  --profile soak --duration-seconds 3600 \
  --set soak.workers=4 \
  --set soak.operation_interval_seconds=0.25 \
  --set soak.max_operations=100000
```

运行期间按 `status_interval_seconds` 输出累计操作、成功、失败、TPS 和错误率。任一 worker
异常会停止新操作并把有限错误样本写入报告。`Ctrl+C` 会停止领取新操作，等待在途 worker，关闭
连接、按策略清理表并生成最终报告。内存只保留累计计数、最多 20 个错误和固定长度延迟窗口；
写操作循环使用 `max_rows` 个主键槽位，数据库侧测试数据也不会随运行时间无限增长。

## TLS/SSL 测试

校验 CA：

```bash
python mysql_mariadb_instance_test.py \
  --config mysql-mariadb-test.example.json \
  --ssl-ca /etc/ssl/certs/mariadb-ca.pem \
  --set expectations.require_tls=true \
  --profile connectivity
```

校验证书和主机名时使用：

```bash
--set connection.ssl_mode=verify_identity \
--set connection.ssl_ca_location=/etc/ssl/certs/mariadb-ca.pem \
--set connection.server_hostname=db.example.internal \
--host db.example.internal
```

双向 TLS 再配置 `connection.ssl_certificate_location` 和 `connection.ssl_key_location`，两者必须
同时存在。私钥内容从不读取到报告，私钥路径也会脱敏。当前 PyMySQL 连接要求
`server_hostname` 与实际连接 `host` 相同；需要用独立 IP 和不同 SNI 名称的环境不在当前覆盖内。

## 复制和高可用

只验证主库写入后副本在时限内读到完全一致的 payload：

```bash
python mysql_mariadb_instance_test.py \
  --config mysql-mariadb-test.example.json \
  --suites replication \
  --set replication.enabled=true \
  --set replication.primary_endpoint=db-primary.internal:3306 \
  --set replication.replica_endpoint=db-replica.internal:3306 \
  --set replication.max_replication_lag_seconds=30
```

复制未启用或未显式提供端点时是 `SKIP`，不是 `FAIL`。主从使用当前配置的相同 database、账号
和 TLS 设置；账号必须能在主库写测试表、从库读取它。

故障切换 suite **不会触发切换**。它只在专用环境中持续重连稳定接入地址，等待操作者或外部
测试系统执行切换，并验证已提交种子行在恢复后仍存在：

```bash
python mysql_mariadb_instance_test.py \
  --config mysql-mariadb-test.example.json \
  --suites failover \
  --set failover.enabled=true \
  --set failover.confirm_dedicated_environment=true \
  --set failover.endpoint=db-ha.internal:3306 \
  --set failover.duration_seconds=300 \
  --set failover.max_recovery_seconds=30 \
  --set failover.require_observed_interruption=true
```

若没有观察到中断，默认返回 `WARN`；要求本轮必须证明切换时可把
`require_observed_interruption` 设为 `true`，此时没有中断会 `FAIL`。

## JSON 报告和退出码

每个 suite 输出 `PASS`、`FAIL`、`WARN` 或 `SKIP`。报告原子写入，包含：

- `schema_version`、`run_id`、`target` 和 `selected_suites`；
- `summary`、逐项 `results`、`duration` 和 `exit_code`；
- `sanitized_config`、清理结果和是否中断；
- 明确的 `control_plane_api_used=false` 和 `business_tables_modified=false`。

密码值、认证失败中的密码、私钥路径和包含密码的连接 URI 会被脱敏。报告会保留 host、port、
database、username 和密码环境变量名称，以便定位测试目标；这些仍可能属于内部元数据，应按组织
规则保存。

| 退出码 | 含义 |
| ---: | --- |
| `0` | 没有 `FAIL`；允许有 `WARN` 或 `SKIP` |
| `1` | suite、清理或报告失败 |
| `2` | 配置、密码环境变量或依赖错误，尚未运行测试 |
| `130` | 非 soak 阶段被用户中断；仍尽力清理并写报告 |

## 安全边界和清理策略

- 只连接已有数据面，不使用控制台 API、云 SDK 或实例生命周期操作。
- 不创建或删除 database，不执行 `DROP DATABASE`、全库清理或业务表 DDL/DML。
- 所有 DML 值都通过 PyMySQL `%s` 参数绑定；SQL 标识符只来自受校验且唯一的框架生成表名。
- `always` 总是尝试 `DROP TABLE IF EXISTS` 本轮表；`on_success` 仅成功时清理；`never` 保留表
  供排查。MySQL DDL 会隐式提交，因此表清理采用精确表名删除，不承诺用事务回滚 DDL。
- `transaction` suite 单独证明 COMMIT、ROLLBACK、异常原子性和跨连接可见性。
- 并发、行数、payload、批量、运行时长、连接数和延迟样本均有校验上限。
- 清理失败会成为 `FAIL` 并保留精确生成表名，供管理员只处理该表。

## 当前覆盖与未覆盖范围

当前覆盖 MySQL/MariaDB 协议连接、用户名密码、常用 TLS、参数化 CRUD、常见跨版本类型、InnoDB
事务、受控并发、索引计划、显式角色账号、主从可见性、外部故障切换观察、轻量性能基线和 soak。

明确未覆盖：完整认证插件矩阵、IAM/LDAP 等外部身份链路、云控制面、自动建库/建账号、自动触发
主从切换、备份恢复、PITR、跨地域容灾、GTID/binlog 拓扑审计、专业容量/极限压测、业务 SQL
兼容性、所有 MySQL/MariaDB 版本差异，以及故障时服务端是否存在未暴露的数据丢失。`datatype`
会记录实际 family/version，但不能据此声称全类型兼容。

## 开发和测试

默认测试离线运行，不连接真实数据库：

```bash
python -m pip install -r requirements-dev.txt
python -m compileall -q .
python -m unittest discover -s tests -v
python -O -m unittest discover -s tests -v
```

只有显式设置以下环境变量时才运行真实集成测试：

```bash
export MARIADB_INTEGRATION=1
export MARIADB_HOST=10.0.1.15
export MARIADB_PORT=3306
export MARIADB_DATABASE=mysql_instance_test
export MARIADB_USER=test_user
export MARIADB_PASSWORD_ENV=MARIADB_PASSWORD
export MARIADB_PASSWORD='replace-with-secret'
# 可选：export MARIADB_SSL_CA=/path/to/ca.pem

python -m unittest tests.test_integration -v
```

集成测试只运行 connectivity、SQL roundtrip 和 transaction，并在 `finally` 中清理本轮测试表。
