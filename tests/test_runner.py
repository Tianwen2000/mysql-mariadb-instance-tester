from __future__ import annotations

import _thread
import copy
import io
import json
import re
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import mysql_mariadb_instance_test as target


class FakeDatabase:
    def __init__(self) -> None:
        self.tables: dict[str, list[dict[str, object]]] = {}
        self.lock = threading.RLock()


class FakeConnection:
    def __init__(self, database: FakeDatabase, *, reject: bool = False) -> None:
        if reject:
            raise RuntimeError("Access denied for user")
        self.database = database
        self.pending: list[tuple[str, str, object]] = []
        self.closed = False
        self.auto_commit = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def begin(self) -> None:
        return None

    def autocommit(self, enabled: bool) -> None:
        self.auto_commit = enabled

    def commit(self) -> None:
        with self.database.lock:
            for operation, table, value in self.pending:
                if operation == "insert":
                    self.database.tables[table].append(copy.deepcopy(value))
                elif operation == "upsert":
                    row = copy.deepcopy(value)
                    same_id = [
                        index
                        for index, existing in enumerate(self.database.tables[table])
                        if existing.get("id") == row.get("id")
                    ]
                    if same_id:
                        self.database.tables[table][same_id[0]] = row
                    else:
                        self.database.tables[table].append(row)
                elif operation == "update":
                    marker, counter = value
                    for row in self.database.tables[table]:
                        if row.get("marker") == marker:
                            row["counter_value"] = counter
                elif operation == "delete":
                    marker = value
                    self.database.tables[table] = [
                        row
                        for row in self.database.tables[table]
                        if row.get("marker") != marker
                    ]
            self.pending.clear()

    def rollback(self) -> None:
        self.pending.clear()

    def close(self) -> None:
        self.closed = True

    def rows(self, table: str) -> list[dict[str, object]]:
        with self.database.lock:
            rows = copy.deepcopy(self.database.tables.get(table, []))
            for operation, pending_table, value in self.pending:
                if pending_table != table:
                    continue
                if operation == "insert":
                    rows.append(copy.deepcopy(value))
                elif operation == "update":
                    marker, counter = value
                    for row in rows:
                        if row.get("marker") == marker:
                            row["counter_value"] = counter
                elif operation == "delete":
                    rows = [row for row in rows if row.get("marker") != value]
            return rows


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.results: list[tuple[object, ...]] = []
        self.description: list[tuple[str]] | None = None

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    @staticmethod
    def table(sql: str) -> str:
        match = re.search(r"`([A-Za-z0-9_]+)`", sql)
        if not match:
            raise RuntimeError(f"No generated table in SQL: {sql}")
        return match.group(1)

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> int:
        compact = " ".join(sql.split())
        upper = compact.upper()
        database = self.connection.database
        self.results = []
        self.description = None
        if upper.startswith("CREATE TABLE"):
            with database.lock:
                database.tables[self.table(compact)] = []
            return 0
        if upper.startswith("DROP TABLE"):
            with database.lock:
                database.tables.pop(self.table(compact), None)
            return 0
        if upper.startswith("SET SESSION"):
            return 0
        if upper == "SELECT %S":
            self.results = [(params[0],)]
            self.description = [("value",)]
            return 1
        if upper.startswith("SELECT @@VERSION"):
            self.results = [("10.11.8-MariaDB", "MariaDB Server", "utf8mb4")]
            self.description = [
                ("@@version",),
                ("@@version_comment",),
                ("@@character_set_connection",),
            ]
            return 1
        if upper.startswith("SHOW STATUS LIKE"):
            self.results = [("Ssl_cipher", "TLS_AES_256_GCM_SHA384")]
            self.description = [("Variable_name",), ("Value",)]
            return 1
        if upper.startswith("INSERT INTO"):
            table = self.table(compact)
            column_match = re.search(r"\(([^)]+)\)\s+VALUES", compact, re.IGNORECASE)
            if not column_match:
                raise RuntimeError(f"Cannot parse INSERT: {compact}")
            columns = [
                column.strip().strip("`") for column in column_match.group(1).split(",")
            ]
            row = dict(zip(columns, params))
            if "id" not in row:
                row["id"] = len(self.connection.rows(table)) + 1
            operation = "upsert" if "ON DUPLICATE KEY UPDATE" in upper else "insert"
            self.connection.pending.append((operation, table, row))
            if self.connection.auto_commit:
                self.connection.commit()
            return 1
        if upper.startswith("UPDATE"):
            table = self.table(compact)
            self.connection.pending.append(("update", table, (params[1], params[0])))
            return 1
        if upper.startswith("DELETE FROM"):
            table = self.table(compact)
            self.connection.pending.append(("delete", table, params[0]))
            return 1
        if upper.startswith("SELECT"):
            table = self.table(compact)
            rows = self.connection.rows(table)
            if "WHERE MARKER=%S" in upper:
                rows = [row for row in rows if row.get("marker") == params[0]]
            elif "WHERE MARKER IN (%S, %S)" in upper:
                rows = [row for row in rows if row.get("marker") in set(params)]
            elif "WHERE MARKER LIKE %S" in upper:
                prefix = str(params[0]).rstrip("%")
                rows = [
                    row for row in rows if str(row.get("marker", "")).startswith(prefix)
                ]
            elif "WHERE OPERATION_ID=%S" in upper:
                rows = [row for row in rows if row.get("operation_id") == params[0]]
            if "SELECT COUNT(*) FROM (SELECT OPERATION_ID" in upper:
                seen: dict[object, int] = {}
                for row in rows:
                    seen[row.get("operation_id")] = (
                        seen.get(row.get("operation_id"), 0) + 1
                    )
                self.results = [(sum(count > 1 for count in seen.values()),)]
                self.description = [("COUNT(*)",)]
                return 1
            if "COUNT(*)" in upper:
                self.results = [(len(rows),)]
                self.description = [("COUNT(*)",)]
                return 1
            select_match = re.search(r"SELECT (.+?) FROM", compact, re.IGNORECASE)
            if not select_match:
                raise RuntimeError(f"Cannot parse SELECT: {compact}")
            columns = [
                column.strip().strip("`") for column in select_match.group(1).split(",")
            ]
            if "ORDER BY counter_value" in compact:
                rows.sort(key=lambda row: int(row["counter_value"]))
            self.results = [
                tuple(row.get(column) for column in columns) for row in rows
            ]
            self.description = [(column,) for column in columns]
            return len(self.results)
        raise RuntimeError(f"Unsupported fake SQL: {compact}")

    def executemany(self, sql: str, rows: list[tuple[object, ...]]) -> int:
        for row in rows:
            self.execute(sql, row)
        return len(rows)

    def fetchone(self) -> tuple[object, ...] | None:
        return self.results[0] if self.results else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self.results)


class FakeAdapter:
    def __init__(
        self, database: FakeDatabase, expected_password: str = "secret"
    ) -> None:
        self.database = database
        self.expected_password = expected_password
        self.connections: list[FakeConnection] = []

    def connect(
        self,
        _endpoint: tuple[str, int] | None = None,
        *,
        username: str | None = None,
        password: str | None = None,
    ) -> FakeConnection:
        del username
        connection = FakeConnection(
            self.database,
            reject=password is not None and password != self.expected_password,
        )
        self.connections.append(connection)
        return connection


def valid_config() -> dict[str, object]:
    config = copy.deepcopy(target.DEFAULT_CONFIG)
    target.validate_config(config)
    return config


class ConfigTests(unittest.TestCase):
    def test_default_config_is_valid_and_independent(self) -> None:
        first = target.load_config(None)
        second = target.load_config(None)
        first["sql"]["rows"] = 1
        self.assertEqual(second["sql"]["rows"], 50)
        target.validate_config(second)

    def test_config_deep_merge_and_cli_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(
                json.dumps({"connection": {"host": "db.internal"}, "sql": {"rows": 9}}),
                encoding="utf-8",
            )
            config = target.load_config(str(path))
        args = target.build_parser().parse_args(
            [
                "--port",
                "3307",
                "--instance-name",
                "instance-override",
                "--database",
                "acceptance",
                "--set",
                "concurrency.workers=2",
            ]
        )
        target.apply_cli_overrides(config, args)
        self.assertEqual(config["connection"]["host"], "db.internal")
        self.assertEqual(config["connection"]["port"], 3307)
        self.assertEqual(config["connection"]["database"], "acceptance")
        self.assertEqual(config["target"]["instance_name"], "instance-override")
        self.assertEqual(config["concurrency"]["workers"], 2)

    def test_unknown_config_and_bad_ranges_are_rejected(self) -> None:
        config = valid_config()
        config["connection"]["unknown"] = True
        with self.assertRaisesRegex(ValueError, "Unknown connection option"):
            target.validate_config(config)
        config = valid_config()
        config["concurrency"]["workers"] = 10
        with self.assertRaisesRegex(ValueError, "must not exceed"):
            target.validate_config(config)

    def test_endpoint_parsing(self) -> None:
        self.assertEqual(target.parse_endpoint("db.example:3306"), ("db.example", 3306))
        self.assertEqual(
            target.parse_endpoint("[2001:db8::1]:3307"), ("2001:db8::1", 3307)
        )
        with self.assertRaisesRegex(ValueError, "HOST:PORT"):
            target.parse_endpoint("db.example")

    def test_ssl_validation_requires_ca_and_cert_key_pair(self) -> None:
        config = valid_config()
        config["connection"]["ssl_mode"] = "verify_ca"
        with self.assertRaisesRegex(ValueError, "ssl_ca_location"):
            target.validate_config(config)
        config = valid_config()
        config["connection"].update(
            {"ssl_mode": "required", "ssl_certificate_location": "x"}
        )
        with self.assertRaisesRegex(ValueError, "configured together"):
            target.validate_config(config)

    def test_adapter_maps_ssl_modes_without_implicit_tls(self) -> None:
        config = valid_config()
        with mock.patch.object(
            target.pymysql, "connect", return_value=object()
        ) as connect:
            target.PyMySQLAdapter(config, "secret").connect()
        disabled = connect.call_args.kwargs
        self.assertTrue(disabled["ssl_disabled"])
        self.assertIsNone(disabled["ssl"])

        config["connection"]["ssl_mode"] = "required"
        with mock.patch.object(
            target.pymysql, "connect", return_value=object()
        ) as connect:
            target.PyMySQLAdapter(config, "secret").connect()
        required = connect.call_args.kwargs
        self.assertFalse(required["ssl_disabled"])
        self.assertIsNotNone(required["ssl"])

        with tempfile.TemporaryDirectory() as directory:
            ca = Path(directory) / "ca.pem"
            ca.write_text("test ca", encoding="ascii")
            config["connection"].update(
                {"ssl_mode": "verify_identity", "ssl_ca_location": str(ca)}
            )
            target.validate_config(config)
            with mock.patch.object(
                target.pymysql, "connect", return_value=object()
            ) as connect:
                target.PyMySQLAdapter(config, "secret").connect()
        verified = connect.call_args.kwargs
        self.assertEqual(verified["ssl_ca"], str(ca))
        self.assertTrue(verified["ssl_verify_cert"])
        self.assertTrue(verified["ssl_verify_identity"])

    def test_example_config_validates(self) -> None:
        path = Path(__file__).resolve().parents[1] / "mysql-mariadb-test.example.json"
        config = target.load_config(str(path))
        target.validate_config(config)
        self.assertEqual(config["execution"]["profile"], "standard")

    def test_unique_table_name_and_identifier_safety(self) -> None:
        first = target.make_table_name("namespace", "prefix", "run", "case")
        second = target.make_table_name("namespace", "prefix", "run", "case")
        self.assertNotEqual(first, second)
        self.assertLessEqual(len(first), 64)
        self.assertIn("namespace", first)
        self.assertIn("prefix", first)
        self.assertIn("run", first)
        self.assertIn("case", first)
        self.assertEqual(target.quote_identifier(first), f"`{first}`")
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            target.quote_identifier("business`; DROP TABLE users")

    def test_sql_values_are_passed_as_parameters(self) -> None:
        cursor = mock.MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.execute.return_value = 1
        connection = mock.MagicMock()
        connection.cursor.return_value = cursor
        hostile = "x'; DROP TABLE business; --"
        target.MySQLTestRunner.execute(
            connection, "SELECT id FROM `safe_table` WHERE marker=%s", (hostile,)
        )
        cursor.execute.assert_called_once_with(
            "SELECT id FROM `safe_table` WHERE marker=%s", (hostile,)
        )

    def test_suite_selection_and_percentiles(self) -> None:
        config = valid_config()
        self.assertEqual(target.choose_suites(config), target.PROFILES["standard"])
        config["execution"]["suites"] = ["soak", "soak"]
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            target.choose_suites(config)
        self.assertEqual(target.percentile([], 0.95), 0.0)
        self.assertEqual(target.percentile([4, 1, 2, 3], 0.50), 2.0)
        self.assertEqual(target.percentile([4, 1, 2, 3], 0.99), 4.0)


class RunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = valid_config()
        self.config["sql"].update({"rows": 8, "value_size": 64})
        self.config["transaction"].update(
            {"transaction_count": 8, "rollback_ratio": 0.25}
        )
        self.config["concurrency"].update(
            {"workers": 3, "connections": 4, "operations": 18, "max_in_flight": 4}
        )
        target.validate_config(self.config)
        self.database = FakeDatabase()
        self.adapter = FakeAdapter(self.database)
        self.runner = target.MySQLTestRunner(
            self.config, "secret", adapter=self.adapter, run_id="testrun"
        )

    def test_sql_roundtrip_with_fake_adapter(self) -> None:
        outcome = self.runner.test_sql_roundtrip()
        self.assertEqual(outcome.metrics["inserted_rows"], 8)
        self.assertEqual(outcome.metrics["remaining_rows"], 4)

    def test_authentication_classifies_access_denied(self) -> None:
        outcome = self.runner.test_authentication()
        self.assertTrue(outcome.metrics["invalid_credentials_rejected"])
        self.assertEqual(outcome.metrics["rejection_error_type"], "RuntimeError")

    def test_transaction_with_fake_adapter(self) -> None:
        outcome = self.runner.test_transaction()
        self.assertTrue(outcome.metrics["cross_connection_visibility_checked"])
        self.assertTrue(outcome.metrics["autocommit_checked"])
        self.assertEqual(outcome.metrics["committed"], 6)
        self.assertEqual(outcome.metrics["rolled_back"], 2)

    def test_concurrency_with_fake_adapter(self) -> None:
        outcome = self.runner.test_concurrency()
        self.assertEqual(outcome.metrics["completed"], 18)
        self.assertEqual(outcome.metrics["final_count"], 18)
        self.assertEqual(outcome.metrics["duplicate_operation_ids"], 0)

    def test_status_mapping(self) -> None:
        passed = self.runner.run_case(
            "pass", lambda: target.CaseOutcome("ok", {"x": 1})
        )

        def warn() -> target.CaseOutcome:
            raise target.CaseWarning("review", {"kind": "warning"})

        def skip() -> target.CaseOutcome:
            raise target.CaseSkip("disabled")

        def fail() -> target.CaseOutcome:
            target.require(False, "broken")
            raise RuntimeError("unreachable")

        statuses = [
            passed.status,
            self.runner.run_case("warn", warn).status,
            self.runner.run_case("skip", skip).status,
            self.runner.run_case("fail", fail).status,
        ]
        self.assertEqual(statuses, ["PASS", "WARN", "SKIP", "FAIL"])

    def test_report_is_structured_atomic_and_secret_free(self) -> None:
        self.runner.results.append(
            target.TestResult(
                "case", "FAIL", 1.0, "password=secret", {"error": "secret"}
            )
        )
        self.config["connection"].update(
            {"ssl_mode": "required", "ssl_key_location": "C:/private/client.key"}
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            report = target.write_report(
                self.runner,
                self.config,
                ["case"],
                str(path),
                exit_code=1,
                duration_seconds=1.25,
                interrupted=False,
            )
            text = report.read_text(encoding="utf-8")
            payload = json.loads(text)
            self.assertNotIn("secret", text)
            self.assertNotIn("client.key", text)
            self.assertEqual(payload["schema_version"], target.REPORT_SCHEMA_VERSION)
            self.assertEqual(payload["run_id"], "testrun")
            self.assertEqual(payload["summary"]["FAIL"], 1)
            self.assertEqual(payload["exit_code"], 1)
            self.assertEqual(payload["selected_suites"], ["case"])
            self.assertFalse(list(Path(directory).glob("*.tmp-*")))

    def test_soak_stops_at_max_operations_and_bounds_samples(self) -> None:
        self.config["soak"].update(
            {
                "duration_seconds": 0.0,
                "workers": 2,
                "operation_interval_seconds": 0.001,
                "read_write_ratio": 0.5,
                "status_interval_seconds": 1.0,
                "drain_timeout_seconds": 1.0,
                "max_operations": 20,
                "max_rows": 5,
                "max_latency_samples": 7,
            }
        )
        target.validate_config(self.config)
        outcome = self.runner.test_soak()
        self.assertEqual(outcome.metrics["operations"], 20)
        self.assertEqual(outcome.metrics["successes"], 20)
        self.assertEqual(outcome.metrics["stop_reason"], "max_operations")
        self.assertLessEqual(outcome.metrics["latency_sample_count"], 7)
        self.assertLessEqual(len(self.database.tables[outcome.metrics["table"]]), 5)
        self.assertTrue(
            all(connection.closed for connection in self.adapter.connections)
        )

    def test_soak_stops_after_finite_duration(self) -> None:
        self.config["soak"].update(
            {
                "duration_seconds": 0.04,
                "workers": 1,
                "operation_interval_seconds": 0.002,
                "status_interval_seconds": 1.0,
                "drain_timeout_seconds": 1.0,
                "max_operations": 0,
            }
        )
        outcome = self.runner.test_soak()
        self.assertEqual(outcome.metrics["stop_reason"], "duration_elapsed")
        self.assertGreater(outcome.metrics["operations"], 0)
        self.assertFalse(outcome.metrics["drain_timed_out"])

    def test_soak_worker_error_stops_the_suite(self) -> None:
        self.config["soak"].update(
            {
                "duration_seconds": 1.0,
                "workers": 1,
                "operation_interval_seconds": 0.001,
                "read_write_ratio": 1.0,
                "status_interval_seconds": 1.0,
                "drain_timeout_seconds": 1.0,
                "max_operations": 0,
            }
        )
        self.runner.query_one = mock.MagicMock(
            side_effect=RuntimeError("worker failed")
        )
        with self.assertRaisesRegex(target.CaseFailure, "worker failed") as caught:
            self.runner.test_soak()
        self.assertEqual(caught.exception.metrics["stop_reason"], "worker_error")
        self.assertEqual(caught.exception.metrics["failures"], 1)

    def test_soak_ctrl_c_is_graceful(self) -> None:
        self.config["soak"].update(
            {
                "duration_seconds": 0.0,
                "workers": 1,
                "operation_interval_seconds": 0.002,
                "status_interval_seconds": 1.0,
                "drain_timeout_seconds": 1.0,
                "max_operations": 0,
            }
        )
        timer = threading.Timer(0.05, _thread.interrupt_main)
        timer.start()
        try:
            outcome = self.runner.test_soak()
        finally:
            timer.cancel()
            timer.join()
        self.assertEqual(outcome.metrics["stop_reason"], "user_interrupt")
        self.assertTrue(outcome.metrics["interrupted_by_user"])
        self.assertEqual(outcome.metrics["failures"], 0)


class CommandTests(unittest.TestCase):
    def test_help_and_suite_list_do_not_need_driver(self) -> None:
        output = io.StringIO()
        with mock.patch.object(target, "pymysql", None), redirect_stdout(output):
            self.assertEqual(target.main(["--list-suites"]), 0)
        self.assertIn("connectivity", output.getvalue())
        self.assertIn("replication and failover", output.getvalue())

    def test_missing_driver_has_clear_error(self) -> None:
        stderr = io.StringIO()
        with mock.patch.object(target, "pymysql", None), mock.patch.object(
            target.sys, "stderr", stderr
        ):
            code = target.main(
                ["--set", "authentication.mode=none", "--profile", "connectivity"]
            )
        self.assertEqual(code, 2)
        self.assertIn("PyMySQL is not installed", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
