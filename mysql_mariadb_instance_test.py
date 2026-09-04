#!/usr/bin/env python3
"""Safe MySQL/MariaDB data-plane acceptance and stability test runner."""

from __future__ import annotations

import argparse
import copy
import getpass
import json
import math
import os
import random
import re
import socket
import ssl
import sys
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutureTimeout
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

try:
    import pymysql
except ImportError:  # Keep --help and --list-suites usable before installation.
    pymysql = None  # type: ignore[assignment]


PROJECT_ROOT = Path(__file__).resolve().parent
REPORT_SCHEMA_VERSION = 1
STATUSES = ("PASS", "FAIL", "WARN", "SKIP")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DEFAULT_CONFIG: dict[str, Any] = {
    "connection": {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "mysql_instance_test",
        "connect_timeout_seconds": 5.0,
        "read_timeout_seconds": 10.0,
        "write_timeout_seconds": 10.0,
        "charset": "utf8mb4",
        "ssl_mode": "disabled",
        "ssl_ca_location": None,
        "ssl_certificate_location": None,
        "ssl_key_location": None,
        "server_hostname": None,
    },
    "authentication": {
        "username": "root",
        "password_env": "MARIADB_PASSWORD",
        "mode": "environment",
    },
    "execution": {
        "profile": "standard",
        "suites": None,
        "namespace": "zhuque_mysql_test",
        "cleanup_policy": "always",
        "fail_fast": False,
    },
    "sql": {
        "test_table_prefix": "instance_test",
        "rows": 50,
        "value_size": 256,
        "statement_timeout_seconds": 30.0,
    },
    "transaction": {
        "isolation_level": "REPEATABLE READ",
        "transaction_count": 20,
        "rollback_ratio": 0.25,
    },
    "concurrency": {
        "workers": 4,
        "connections": 4,
        "operations": 100,
        "max_in_flight": 8,
    },
    "performance": {
        "rows": 1000,
        "workers": 4,
        "batch_size": 50,
        "duration_seconds": 60.0,
        "min_tps": None,
        "max_p95_ms": None,
        "max_p99_ms": None,
        "max_latency_samples": 10000,
    },
    "permissions": {
        "enabled": False,
        "read_only_username": None,
        "read_only_password_env": None,
        "read_write_username": None,
        "read_write_password_env": None,
        "no_access_username": None,
        "no_access_password_env": None,
    },
    "replication": {
        "enabled": False,
        "primary_endpoint": None,
        "replica_endpoint": None,
        "max_replication_lag_seconds": 30.0,
        "polling_interval_seconds": 1.0,
    },
    "failover": {
        "enabled": False,
        "confirm_dedicated_environment": False,
        "endpoint": None,
        "duration_seconds": 60.0,
        "polling_interval_seconds": 1.0,
        "max_recovery_seconds": 30.0,
        "require_observed_interruption": False,
    },
    "soak": {
        "duration_seconds": 0.0,
        "workers": 3,
        "operation_interval_seconds": 1.0,
        "read_write_ratio": 0.7,
        "status_interval_seconds": 10.0,
        "drain_timeout_seconds": 30.0,
        "max_operations": 0,
        "max_rows": 10000,
        "max_latency_samples": 10000,
    },
    "expectations": {
        "expected_server_family": "any",
        "expected_version_prefix": None,
        "require_tls": False,
        "expected_charset": "utf8mb4",
    },
    "report": {"directory": "reports"},
}

PROFILES: dict[str, list[str]] = {
    "connectivity": ["connectivity"],
    "smoke": ["connectivity", "authentication", "sql_roundtrip"],
    "standard": [
        "connectivity",
        "authentication",
        "sql_roundtrip",
        "datatype",
        "transaction",
        "concurrency",
        "index",
    ],
    "performance": [
        "connectivity",
        "authentication",
        "sql_roundtrip",
        "datatype",
        "transaction",
        "concurrency",
        "index",
        "performance",
    ],
    "soak": ["connectivity", "sql_roundtrip", "soak"],
    "ha": ["connectivity", "replication", "failover"],
}
AVAILABLE_SUITES = {
    "connectivity",
    "authentication",
    "sql_roundtrip",
    "datatype",
    "transaction",
    "concurrency",
    "index",
    "permissions",
    "replication",
    "failover",
    "performance",
    "soak",
}
WRITE_SUITES = AVAILABLE_SUITES - {"connectivity", "authentication"}


class CaseSkip(Exception):
    """The selected case does not apply to the supplied configuration."""

    def __init__(self, message: str, metrics: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.metrics = dict(metrics or {})


class CaseWarning(Exception):
    """The case completed but cannot make a definitive pass assertion."""

    def __init__(self, message: str, metrics: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.metrics = dict(metrics or {})


class CaseFailure(AssertionError):
    """An assertion failure that carries measurements into the report."""

    def __init__(self, message: str, metrics: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.metrics = dict(metrics or {})


@dataclass
class CaseOutcome:
    detail: str
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class TestResult:
    name: str
    status: str
    duration_ms: float
    detail: str
    metrics: dict[str, Any] = field(default_factory=dict)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def require(
    condition: bool, message: str, metrics: Mapping[str, Any] | None = None
) -> None:
    """Assert without relying on the optimized-away ``assert`` statement."""
    if not condition:
        raise CaseFailure(message, metrics)


def deep_merge(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def parse_config_override(expression: str) -> tuple[str, str, Any]:
    if "=" not in expression or "." not in expression.split("=", 1)[0]:
        raise ValueError("--set must use SECTION.OPTION=VALUE")
    path, raw = expression.split("=", 1)
    section, option = (part.strip() for part in path.split(".", 1))
    if section not in DEFAULT_CONFIG or option not in DEFAULT_CONFIG[section]:
        raise ValueError(f"Unknown config option: {section}.{option}")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        value = raw
    return section, option, value


def load_config(path: str | None) -> dict[str, Any]:
    if not path:
        return copy.deepcopy(DEFAULT_CONFIG)
    source = Path(path).expanduser().resolve()
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Config file not found: {source}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in {source} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"Cannot read config file {source}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("Config root must be a JSON object")
    return deep_merge(DEFAULT_CONFIG, payload)


def parse_endpoint(value: str) -> tuple[str, int]:
    endpoint = value.strip()
    if endpoint.startswith("["):
        close = endpoint.find("]")
        if close < 0 or endpoint[close + 1 : close + 2] != ":":
            raise ValueError(f"Invalid endpoint {value!r}; use [IPv6]:PORT")
        host, port_text = endpoint[1:close], endpoint[close + 2 :]
    else:
        if ":" not in endpoint:
            raise ValueError(f"Invalid endpoint {value!r}; use HOST:PORT")
        host, port_text = endpoint.rsplit(":", 1)
    host = host.strip()
    if not host or len(host) > 253:
        raise ValueError(f"Invalid host in endpoint {value!r}")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(f"Invalid port in endpoint {value!r}") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"Port in endpoint {value!r} must be between 1 and 65535")
    return host, port


def _number(config: dict[str, Any], section: str, option: str) -> float:
    value = config[section].get(option)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{section}.{option} must be a number")
    return float(value)


def _integer(config: dict[str, Any], section: str, option: str) -> int:
    value = config[section].get(option)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{section}.{option} must be an integer")
    return value


def _optional_threshold(config: dict[str, Any], section: str, option: str) -> None:
    value = config[section].get(option)
    if value is None:
        return
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or float(value) <= 0
    ):
        raise ValueError(f"{section}.{option} must be null or a positive number")


def _path_or_none(config: dict[str, Any], section: str, option: str) -> str | None:
    value = config[section].get(option)
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"{section}.{option} must be null or a non-empty path string")
    return value


def validate_config(config: dict[str, Any], *, require_target: bool = True) -> None:
    if not isinstance(config, dict):
        raise ValueError("Config root must be a JSON object")
    unknown_sections = sorted(set(config) - set(DEFAULT_CONFIG))
    if unknown_sections:
        raise ValueError(f"Unknown config section(s): {', '.join(unknown_sections)}")
    for section_name, defaults in DEFAULT_CONFIG.items():
        section = config.get(section_name)
        if not isinstance(section, dict):
            raise ValueError(f"{section_name} must be a JSON object")
        unknown = sorted(set(section) - set(defaults))
        if unknown:
            raise ValueError(f"Unknown {section_name} option(s): {', '.join(unknown)}")

    connection = config["connection"]
    authentication = config["authentication"]
    execution = config["execution"]
    expectations = config["expectations"]
    host = connection.get("host")
    database = connection.get("database")
    username = authentication.get("username")
    if require_target and (not isinstance(host, str) or not host.strip()):
        raise ValueError("connection.host must be a non-empty string")
    if not isinstance(host, str) or len(host) > 253:
        raise ValueError("connection.host must be a string of at most 253 characters")
    port = _integer(config, "connection", "port")
    if not 1 <= port <= 65535:
        raise ValueError("connection.port must be between 1 and 65535")
    if not isinstance(database, str) or not database.strip() or len(database) > 64:
        raise ValueError(
            "connection.database must be a non-empty string of at most 64 characters"
        )
    if "\x00" in database:
        raise ValueError("connection.database must not contain NUL")
    if not isinstance(username, str) or not username or len(username) > 128:
        raise ValueError("authentication.username must be a non-empty string")
    if authentication.get("mode") not in {"environment", "prompt", "none"}:
        raise ValueError("authentication.mode must be environment, prompt, or none")
    password_env = authentication.get("password_env")
    if not isinstance(password_env, str) or not ENV_NAME_PATTERN.fullmatch(
        password_env
    ):
        raise ValueError(
            "authentication.password_env must be a valid environment variable name"
        )

    for option in (
        "connect_timeout_seconds",
        "read_timeout_seconds",
        "write_timeout_seconds",
    ):
        value = _number(config, "connection", option)
        if not 0 < value <= 300:
            raise ValueError(f"connection.{option} must be between 0 and 300")
    charset = connection.get("charset")
    if not isinstance(charset, str) or not re.fullmatch(r"[A-Za-z0-9_]{1,32}", charset):
        raise ValueError(
            "connection.charset must use 1-32 letters, digits, or underscores"
        )
    ssl_mode = connection.get("ssl_mode")
    if ssl_mode not in {
        "disabled",
        "preferred",
        "required",
        "verify_ca",
        "verify_identity",
    }:
        raise ValueError(
            "connection.ssl_mode must be disabled, preferred, required, verify_ca, or verify_identity"
        )
    ca = _path_or_none(config, "connection", "ssl_ca_location")
    cert = _path_or_none(config, "connection", "ssl_certificate_location")
    key = _path_or_none(config, "connection", "ssl_key_location")
    server_hostname = connection.get("server_hostname")
    if server_hostname is not None and (
        not isinstance(server_hostname, str) or not server_hostname.strip()
    ):
        raise ValueError(
            "connection.server_hostname must be null or a non-empty string"
        )
    if server_hostname is not None and server_hostname != host:
        raise ValueError(
            "connection.server_hostname must match connection.host with the PyMySQL driver"
        )
    if bool(cert) != bool(key):
        raise ValueError(
            "connection.ssl_certificate_location and ssl_key_location must be configured together"
        )
    if ssl_mode in {"verify_ca", "verify_identity"} and not ca:
        raise ValueError(
            f"connection.ssl_ca_location is required for ssl_mode={ssl_mode}"
        )
    if ssl_mode == "disabled" and any((ca, cert, key, server_hostname)):
        raise ValueError(
            "TLS certificate options require connection.ssl_mode other than disabled"
        )
    if ssl_mode == "preferred" and any((ca, cert, key, server_hostname)):
        raise ValueError(
            "TLS certificate options require ssl_mode=required, verify_ca, or verify_identity"
        )
    for option, value in (
        ("ssl_ca_location", ca),
        ("ssl_certificate_location", cert),
        ("ssl_key_location", key),
    ):
        if value and not Path(value).expanduser().is_file():
            raise ValueError(
                f"connection.{option} file not found: {Path(value).expanduser()}"
            )

    profile = execution.get("profile")
    if profile not in PROFILES:
        raise ValueError(
            f"execution.profile must be one of: {', '.join(sorted(PROFILES))}"
        )
    suites = execution.get("suites")
    if suites is not None and (
        not isinstance(suites, list)
        or not all(isinstance(item, str) for item in suites)
    ):
        raise ValueError("execution.suites must be null or an array of suite names")
    namespace = execution.get("namespace")
    prefix = config["sql"].get("test_table_prefix")
    for label, value, maximum_length in (
        ("execution.namespace", namespace, 20),
        ("sql.test_table_prefix", prefix, 13),
    ):
        if not isinstance(value, str) or not IDENTIFIER_PATTERN.fullmatch(value):
            raise ValueError(
                f"{label} must contain only letters, digits, and underscores"
            )
        if len(value) > maximum_length:
            raise ValueError(f"{label} must not exceed {maximum_length} characters")
    if execution.get("cleanup_policy") not in {"always", "on_success", "never"}:
        raise ValueError(
            "execution.cleanup_policy must be always, on_success, or never"
        )
    if not isinstance(execution.get("fail_fast"), bool):
        raise ValueError("execution.fail_fast must be true or false")

    integer_limits = {
        ("sql", "rows"): (1, 100000),
        ("sql", "value_size"): (1, 1048576),
        ("transaction", "transaction_count"): (1, 10000),
        ("concurrency", "workers"): (1, 64),
        ("concurrency", "connections"): (1, 64),
        ("concurrency", "operations"): (1, 100000),
        ("concurrency", "max_in_flight"): (1, 256),
        ("performance", "rows"): (1, 1000000),
        ("performance", "workers"): (1, 64),
        ("performance", "batch_size"): (1, 10000),
        ("performance", "max_latency_samples"): (1, 100000),
        ("soak", "workers"): (1, 64),
        ("soak", "max_operations"): (0, 1000000000),
        ("soak", "max_rows"): (1, 1000000),
        ("soak", "max_latency_samples"): (1, 100000),
    }
    for (section, option), (minimum, maximum) in integer_limits.items():
        value = _integer(config, section, option)
        if not minimum <= value <= maximum:
            raise ValueError(
                f"{section}.{option} must be between {minimum} and {maximum}"
            )
    if config["concurrency"]["workers"] > config["concurrency"]["connections"]:
        raise ValueError("concurrency.workers must not exceed concurrency.connections")
    if config["concurrency"]["max_in_flight"] < config["concurrency"]["workers"]:
        raise ValueError(
            "concurrency.max_in_flight must be at least concurrency.workers"
        )
    if config["performance"]["workers"] > config["concurrency"]["connections"]:
        raise ValueError("performance.workers must not exceed concurrency.connections")
    if config["soak"]["workers"] > config["concurrency"]["connections"]:
        raise ValueError("soak.workers must not exceed concurrency.connections")
    if config["performance"]["batch_size"] > config["performance"]["rows"]:
        raise ValueError("performance.batch_size must not exceed performance.rows")
    isolation = config["transaction"].get("isolation_level")
    if isolation not in {
        "READ UNCOMMITTED",
        "READ COMMITTED",
        "REPEATABLE READ",
        "SERIALIZABLE",
    }:
        raise ValueError(
            "transaction.isolation_level must be READ UNCOMMITTED, READ COMMITTED, "
            "REPEATABLE READ, or SERIALIZABLE"
        )
    batch_bytes = config["performance"]["batch_size"] * config["sql"]["value_size"]
    if batch_bytes > 64 * 1024 * 1024:
        raise ValueError(
            "performance batch payload exceeds the 64 MiB memory safety limit"
        )
    roundtrip_bytes = config["sql"]["rows"] * config["sql"]["value_size"]
    if roundtrip_bytes > 64 * 1024 * 1024:
        raise ValueError(
            "sql.rows * sql.value_size exceeds the 64 MiB memory safety limit"
        )

    ranges = {
        ("sql", "statement_timeout_seconds"): (0.1, 600.0),
        ("transaction", "rollback_ratio"): (0.0, 1.0),
        ("performance", "duration_seconds"): (0.1, 86400.0),
        ("replication", "max_replication_lag_seconds"): (0.1, 3600.0),
        ("replication", "polling_interval_seconds"): (0.05, 60.0),
        ("failover", "duration_seconds"): (1.0, 86400.0),
        ("failover", "polling_interval_seconds"): (0.05, 60.0),
        ("failover", "max_recovery_seconds"): (0.1, 3600.0),
        ("soak", "duration_seconds"): (0.0, 2592000.0),
        ("soak", "operation_interval_seconds"): (0.001, 3600.0),
        ("soak", "read_write_ratio"): (0.0, 1.0),
        ("soak", "status_interval_seconds"): (0.1, 3600.0),
        ("soak", "drain_timeout_seconds"): (0.1, 3600.0),
    }
    for (section, option), (minimum, maximum) in ranges.items():
        value = _number(config, section, option)
        if not minimum <= value <= maximum:
            raise ValueError(
                f"{section}.{option} must be between {minimum} and {maximum}"
            )
    for option in ("min_tps", "max_p95_ms", "max_p99_ms"):
        _optional_threshold(config, "performance", option)

    for section in ("permissions", "replication", "failover"):
        if not isinstance(config[section].get("enabled"), bool):
            raise ValueError(f"{section}.enabled must be true or false")
    permissions = config["permissions"]
    for role in ("read_only", "read_write", "no_access"):
        role_user = permissions.get(f"{role}_username")
        role_env = permissions.get(f"{role}_password_env")
        if role_user is not None and (not isinstance(role_user, str) or not role_user):
            raise ValueError(
                f"permissions.{role}_username must be null or a non-empty string"
            )
        if role_env is not None and (
            not isinstance(role_env, str) or not ENV_NAME_PATTERN.fullmatch(role_env)
        ):
            raise ValueError(
                f"permissions.{role}_password_env must be null or a valid env name"
            )
        if bool(role_user) != bool(role_env):
            raise ValueError(
                f"permissions {role} username and password_env must be configured together"
            )
    replication = config["replication"]
    for option in ("primary_endpoint", "replica_endpoint"):
        endpoint = replication.get(option)
        if endpoint is not None:
            if not isinstance(endpoint, str):
                raise ValueError(f"replication.{option} must be null or HOST:PORT")
            parse_endpoint(endpoint)
    if replication.get("enabled") and not all(
        replication.get(option) for option in ("primary_endpoint", "replica_endpoint")
    ):
        raise ValueError(
            "replication.enabled requires primary_endpoint and replica_endpoint"
        )
    failover = config["failover"]
    if not isinstance(failover.get("confirm_dedicated_environment"), bool):
        raise ValueError("failover.confirm_dedicated_environment must be true or false")
    if not isinstance(failover.get("require_observed_interruption"), bool):
        raise ValueError("failover.require_observed_interruption must be true or false")
    if failover.get("endpoint") is not None:
        if not isinstance(failover["endpoint"], str):
            raise ValueError("failover.endpoint must be null or HOST:PORT")
        parse_endpoint(failover["endpoint"])
    if failover.get("enabled") and not failover.get("confirm_dedicated_environment"):
        raise ValueError(
            "failover.enabled requires failover.confirm_dedicated_environment=true"
        )
    family = expectations.get("expected_server_family")
    if family not in {"any", "mysql", "mariadb"}:
        raise ValueError(
            "expectations.expected_server_family must be any, mysql, or mariadb"
        )
    version_prefix = expectations.get("expected_version_prefix")
    if version_prefix is not None and (
        not isinstance(version_prefix, str) or not version_prefix
    ):
        raise ValueError(
            "expectations.expected_version_prefix must be null or a non-empty string"
        )
    if not isinstance(expectations.get("require_tls"), bool):
        raise ValueError("expectations.require_tls must be true or false")
    expected_charset = expectations.get("expected_charset")
    if expected_charset is not None and (
        not isinstance(expected_charset, str)
        or not re.fullmatch(r"[A-Za-z0-9_]{1,32}", expected_charset)
    ):
        raise ValueError(
            "expectations.expected_charset must be null or a valid charset name"
        )
    report_directory = config["report"].get("directory")
    if not isinstance(report_directory, str) or not report_directory.strip():
        raise ValueError("report.directory must be a non-empty path string")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run bounded MySQL/MariaDB data-plane acceptance tests.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--config", help="JSON configuration file")
    parser.add_argument("--host", help="Database host override")
    parser.add_argument("--port", type=int, help="Database port override")
    parser.add_argument("--database", help="Dedicated test database/schema")
    parser.add_argument("--username", help="Database username override")
    parser.add_argument(
        "--password-env", help="Environment variable containing the password"
    )
    parser.add_argument("--ssl-ca", help="CA certificate path; enables verify_ca mode")
    parser.add_argument(
        "--profile", choices=sorted(PROFILES), help="Test profile override"
    )
    parser.add_argument(
        "--suites", help="Comma-separated suite names; overrides the profile"
    )
    parser.add_argument(
        "--set",
        dest="config_overrides",
        action="append",
        default=[],
        metavar="SECTION.OPTION=VALUE",
        help="Override a declared config value; VALUE accepts JSON or plain text",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        help="Override soak.duration_seconds; zero runs until Ctrl+C",
    )
    parser.add_argument("--report", help="Write the JSON report to this exact path")
    parser.add_argument(
        "--list-suites", action="store_true", help="List profiles and suites"
    )
    return parser


def apply_cli_overrides(config: dict[str, Any], args: argparse.Namespace) -> None:
    for expression in args.config_overrides:
        section, option, value = parse_config_override(expression)
        config[section][option] = value
    mapping = {
        "host": ("connection", "host"),
        "port": ("connection", "port"),
        "database": ("connection", "database"),
        "username": ("authentication", "username"),
        "password_env": ("authentication", "password_env"),
        "profile": ("execution", "profile"),
        "duration_seconds": ("soak", "duration_seconds"),
    }
    for arg_name, (section, option) in mapping.items():
        value = getattr(args, arg_name, None)
        if value is not None:
            config[section][option] = value
    if args.password_env:
        config["authentication"]["mode"] = "environment"
    if args.ssl_ca:
        config["connection"]["ssl_ca_location"] = args.ssl_ca
        config["connection"]["ssl_mode"] = "verify_ca"
    if args.suites:
        config["execution"]["suites"] = [
            item.strip() for item in args.suites.split(",") if item.strip()
        ]


def choose_suites(config: dict[str, Any]) -> list[str]:
    configured = config["execution"].get("suites")
    suites = (
        list(configured)
        if configured is not None
        else list(PROFILES[config["execution"]["profile"]])
    )
    unknown = sorted(set(suites) - AVAILABLE_SUITES)
    if unknown:
        raise ValueError(f"Unknown suite(s): {', '.join(unknown)}")
    duplicate = sorted({name for name in suites if suites.count(name) > 1})
    if duplicate:
        raise ValueError(f"Duplicate suite(s): {', '.join(duplicate)}")
    if not suites:
        raise ValueError("At least one suite must be selected")
    return suites


def resolve_password(config: dict[str, Any]) -> str:
    mode = config["authentication"]["mode"]
    if mode == "none":
        return ""
    if mode == "prompt":
        return getpass.getpass("Database password: ")
    env_name = config["authentication"]["password_env"]
    value = os.getenv(env_name)
    if value is None:
        raise ValueError(
            f"Password environment variable {env_name} is not set; "
            "set it or use --set authentication.mode=prompt"
        )
    return value


def percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    rank = max(1, math.ceil(fraction * len(ordered)))
    return ordered[rank - 1]


def database_error_code(error: BaseException) -> int | None:
    args = getattr(error, "args", ())
    if args and isinstance(args[0], int):
        return int(args[0])
    return None


def make_table_name(namespace: str, prefix: str, run_id: str, suffix: str) -> str:
    components = (
        namespace,
        prefix,
        re.sub(r"[^A-Za-z0-9_]", "_", run_id)[:10],
        re.sub(r"[^A-Za-z0-9_]", "_", suffix)[:6],
        uuid.uuid4().hex[:8],
    )
    normalized = "_".join(components)
    if not normalized or not normalized[0].isalpha():
        normalized = "t_" + normalized
    require(len(normalized) <= 64, "Generated table name exceeded the MySQL limit")
    return normalized


def quote_identifier(value: str) -> str:
    if not value or len(value) > 64 or not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Unsafe generated SQL identifier: {value!r}")
    return "`" + value + "`"


def sanitize_text(value: Any, secrets: Iterable[str]) -> str:
    text = str(value)
    for secret in sorted(secrets, key=len, reverse=True):
        if secret:
            text = text.replace(secret, "<redacted>")
    text = re.sub(
        r"(?i)(password|passwd|pwd)\s*[=:]\s*[^\s,;]+", r"\1=<redacted>", text
    )
    text = re.sub(
        r"(?i)(mysql(?:\+[^:]+)?://[^:/@\s]+:)[^@\s]+@", r"\1<redacted>@", text
    )
    return text[:2000]


def sanitized_config(config: dict[str, Any]) -> dict[str, Any]:
    safe = copy.deepcopy(config)
    safe["connection"]["ssl_key_location"] = (
        "<redacted>" if safe["connection"].get("ssl_key_location") else None
    )
    for option in (
        "read_only_password_env",
        "read_write_password_env",
        "no_access_password_env",
    ):
        if safe["permissions"].get(option):
            safe["permissions"][option] = "<configured environment variable>"
    safe["authentication"]["password_present"] = False
    return safe


def print_suite_list() -> None:
    print("Profiles:")
    for name in sorted(PROFILES):
        print(f"  {name}: {', '.join(PROFILES[name])}")
    print("Suites:")
    for name in sorted(AVAILABLE_SUITES):
        marker = " [writes isolated test tables]" if name in WRITE_SUITES else ""
        print(f"  {name}{marker}")
    print("replication and failover are skipped unless explicitly enabled")


class PyMySQLAdapter:
    """Small injectable boundary around PyMySQL connection creation."""

    def __init__(self, config: dict[str, Any], password: str) -> None:
        self.config = config
        self.password = password

    def connect(
        self,
        endpoint: tuple[str, int] | None = None,
        *,
        username: str | None = None,
        password: str | None = None,
    ) -> Any:
        if pymysql is None:
            raise RuntimeError(
                "PyMySQL is required for database suites; install requirements.txt"
            )
        connection = self.config["connection"]
        authentication = self.config["authentication"]
        host, port = endpoint or (connection["host"], connection["port"])
        ssl_mode = connection["ssl_mode"]
        ssl_options: dict[str, Any] | None = None
        if ssl_mode == "required":
            ssl_options = {"check_hostname": False, "verify_mode": ssl.CERT_NONE}
        return pymysql.connect(
            host=host,
            port=port,
            user=username if username is not None else authentication["username"],
            password=password if password is not None else self.password,
            database=connection["database"],
            charset=connection["charset"],
            connect_timeout=max(1, math.ceil(connection["connect_timeout_seconds"])),
            read_timeout=connection["read_timeout_seconds"],
            write_timeout=connection["write_timeout_seconds"],
            autocommit=False,
            ssl=ssl_options,
            ssl_ca=connection.get("ssl_ca_location"),
            ssl_cert=connection.get("ssl_certificate_location"),
            ssl_key=connection.get("ssl_key_location"),
            ssl_disabled=ssl_mode == "disabled",
            ssl_verify_cert=ssl_mode in {"verify_ca", "verify_identity"},
            ssl_verify_identity=ssl_mode == "verify_identity",
        )


class MySQLTestRunner:
    def __init__(
        self,
        config: dict[str, Any],
        password: str,
        *,
        adapter: Any | None = None,
        run_id: str | None = None,
    ) -> None:
        self.config = config
        self.password = password
        self.adapter = adapter or PyMySQLAdapter(config, password)
        self.run_id = run_id or (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + "_"
            + uuid.uuid4().hex[:8]
        )
        self.started_at = utc_now()
        self.results: list[TestResult] = []
        self._tables: list[tuple[tuple[str, int] | None, str]] = []
        self._table_lock = threading.Lock()
        self._secrets = {password} if password else set()
        private_key_path = config["connection"].get("ssl_key_location")
        if private_key_path:
            self._secrets.add(str(private_key_path))
        self.cleanup_errors: list[str] = []
        self.cleaned_tables: list[str] = []

    @contextmanager
    def connection(
        self,
        endpoint: tuple[str, int] | None = None,
        *,
        username: str | None = None,
        password: str | None = None,
    ) -> Any:
        connection = self.adapter.connect(
            endpoint, username=username, password=password
        )
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def execute(connection: Any, sql_text: str, params: Sequence[Any] = ()) -> int:
        with connection.cursor() as cursor:
            return int(cursor.execute(sql_text, tuple(params)))

    @staticmethod
    def executemany(
        connection: Any, sql_text: str, rows: Sequence[Sequence[Any]]
    ) -> int:
        with connection.cursor() as cursor:
            return int(cursor.executemany(sql_text, rows))

    @staticmethod
    def query_one(connection: Any, sql_text: str, params: Sequence[Any] = ()) -> Any:
        with connection.cursor() as cursor:
            cursor.execute(sql_text, tuple(params))
            return cursor.fetchone()

    @staticmethod
    def query_all(
        connection: Any, sql_text: str, params: Sequence[Any] = ()
    ) -> list[Any]:
        with connection.cursor() as cursor:
            cursor.execute(sql_text, tuple(params))
            return list(cursor.fetchall())

    @staticmethod
    def query_mapping(
        connection: Any, sql_text: str, params: Sequence[Any] = ()
    ) -> dict[str, Any]:
        with connection.cursor() as cursor:
            cursor.execute(sql_text, tuple(params))
            row = cursor.fetchone()
            columns = [str(item[0]).lower() for item in (cursor.description or [])]
        return dict(zip(columns, row or ()))

    def new_table(self, suffix: str) -> str:
        return make_table_name(
            self.config["execution"]["namespace"],
            self.config["sql"]["test_table_prefix"],
            self.run_id,
            suffix,
        )

    def create_table(
        self,
        table: str,
        definition: str,
        endpoint: tuple[str, int] | None = None,
    ) -> None:
        sql_text = (
            f"CREATE TABLE {quote_identifier(table)} ({definition}) ENGINE=InnoDB"
        )
        with self.connection(endpoint) as connection:
            self.execute(connection, sql_text)
            connection.commit()
        with self._table_lock:
            self._tables.append((endpoint, table))

    def cleanup(self, successful: bool) -> None:
        policy = self.config["execution"]["cleanup_policy"]
        if policy == "never" or (policy == "on_success" and not successful):
            return
        with self._table_lock:
            tables = list(reversed(self._tables))
        for endpoint, table in tables:
            try:
                with self.connection(endpoint) as connection:
                    self.execute(
                        connection, f"DROP TABLE IF EXISTS {quote_identifier(table)}"
                    )
                    connection.commit()
                self.cleaned_tables.append(table)
            except Exception as exc:
                self.cleanup_errors.append(sanitize_text(exc, self._secrets))

    def run_case(self, name: str, case: Callable[[], CaseOutcome]) -> TestResult:
        started = time.monotonic()
        try:
            outcome = case()
            result = TestResult(
                name,
                "PASS",
                (time.monotonic() - started) * 1000,
                outcome.detail,
                outcome.metrics,
            )
        except CaseSkip as exc:
            result = TestResult(
                name,
                "SKIP",
                (time.monotonic() - started) * 1000,
                sanitize_text(exc, self._secrets),
                exc.metrics,
            )
        except CaseWarning as exc:
            result = TestResult(
                name,
                "WARN",
                (time.monotonic() - started) * 1000,
                sanitize_text(exc, self._secrets),
                exc.metrics,
            )
        except CaseFailure as exc:
            result = TestResult(
                name,
                "FAIL",
                (time.monotonic() - started) * 1000,
                sanitize_text(exc, self._secrets),
                exc.metrics,
            )
        except Exception as exc:
            result = TestResult(
                name,
                "FAIL",
                (time.monotonic() - started) * 1000,
                f"{type(exc).__name__}: {sanitize_text(exc, self._secrets)}",
                {},
            )
        self.results.append(result)
        print(f"[{result.status}] {name}: {result.detail}")
        return result

    def _server_facts(self, connection: Any) -> dict[str, Any]:
        row = self.query_one(
            connection,
            "SELECT @@version, @@version_comment, @@character_set_connection",
        )
        require(row is not None and len(row) >= 3, "Server facts query returned no row")
        version, comment, charset = (str(row[0]), str(row[1]), str(row[2]))
        combined = (version + " " + comment).lower()
        family = "mariadb" if "mariadb" in combined else "mysql"
        cipher_row = self.query_one(connection, "SHOW STATUS LIKE 'Ssl_cipher'")
        tls_cipher = str(cipher_row[1]) if cipher_row and len(cipher_row) > 1 else ""
        return {
            "server_family": family,
            "server_version": version,
            "version_comment": comment,
            "connection_charset": charset,
            "tls_enabled": bool(tls_cipher),
            "tls_cipher": tls_cipher or None,
        }

    def _assert_expectations(self, facts: dict[str, Any]) -> None:
        expectations = self.config["expectations"]
        family = expectations["expected_server_family"]
        require(
            family == "any" or facts["server_family"] == family,
            f"Expected server family {family}, got {facts['server_family']}",
            facts,
        )
        version_prefix = expectations["expected_version_prefix"]
        require(
            version_prefix is None
            or facts["server_version"].startswith(version_prefix),
            f"Server version {facts['server_version']} does not start with {version_prefix}",
            facts,
        )
        expected_charset = expectations["expected_charset"]
        require(
            expected_charset is None
            or facts["connection_charset"].lower() == expected_charset.lower(),
            f"Expected connection charset {expected_charset}, got {facts['connection_charset']}",
            facts,
        )
        require(
            not expectations["require_tls"] or facts["tls_enabled"],
            "TLS is required but the database session has no TLS cipher",
            facts,
        )

    def test_connectivity(self) -> CaseOutcome:
        connection_config = self.config["connection"]
        endpoint = (connection_config["host"], connection_config["port"])
        tcp_started = time.monotonic()
        with socket.create_connection(
            endpoint, timeout=connection_config["connect_timeout_seconds"]
        ):
            pass
        tcp_ms = (time.monotonic() - tcp_started) * 1000
        sql_started = time.monotonic()
        with self.connection() as connection:
            row = self.query_one(connection, "SELECT %s", (1,))
            require(
                row is not None and int(row[0]) == 1,
                "SELECT 1 returned an unexpected value",
            )
            facts = self._server_facts(connection)
        sql_ms = (time.monotonic() - sql_started) * 1000
        self._assert_expectations(facts)
        metrics = dict(
            facts, tcp_connect_ms=round(tcp_ms, 3), sql_roundtrip_ms=round(sql_ms, 3)
        )
        return CaseOutcome(
            "TCP, handshake, authentication, and SELECT 1 succeeded", metrics
        )

    def test_authentication(self) -> CaseOutcome:
        username = self.config["authentication"]["username"]
        with self.connection() as connection:
            facts = self._server_facts(connection)
        rejected = False
        error_code: int | None = None
        error_type: str | None = None
        try:
            bad = self.adapter.connect(
                None, username=username, password="invalid_" + uuid.uuid4().hex
            )
        except Exception as exc:
            error_type = type(exc).__name__
            error_code = database_error_code(exc)
            error_text = sanitize_text(exc, self._secrets).lower()
            rejected = error_code in {1044, 1045, 1698} or "access denied" in error_text
        else:
            bad.close()
        metrics = {
            "invalid_credentials_rejected": rejected,
            "rejection_error_type": error_type,
            "rejection_error_code": error_code,
            "tls_enabled": facts["tls_enabled"],
            "tls_cipher": facts["tls_cipher"],
        }
        require(
            rejected, "The server accepted an intentionally invalid password", metrics
        )
        require(
            not self.config["expectations"]["require_tls"] or facts["tls_enabled"],
            "Authentication succeeded without required TLS",
            metrics,
        )
        return CaseOutcome(
            "Valid credentials accepted and invalid credentials rejected", metrics
        )

    def test_sql_roundtrip(self) -> CaseOutcome:
        table = self.new_table("roundtrip")
        quoted = quote_identifier(table)
        self.create_table(
            table,
            "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
            "marker VARCHAR(191) NOT NULL UNIQUE, payload LONGBLOB NOT NULL, "
            "counter_value INT NOT NULL DEFAULT 0",
        )
        row_count = self.config["sql"]["rows"]
        value_size = self.config["sql"]["value_size"]
        rows: list[tuple[str, bytes, int]] = []
        for index in range(row_count):
            header = f"{self.run_id}:{index}:".encode("ascii")
            payload = (header + b"x" * value_size)[:value_size]
            rows.append((f"{self.run_id}_{index}", payload, index))
        with self.connection() as connection:
            self.executemany(
                connection,
                f"INSERT INTO {quoted} (marker, payload, counter_value) VALUES (%s, %s, %s)",
                rows,
            )
            connection.commit()
            selected = self.query_all(
                connection,
                f"SELECT marker, payload, counter_value FROM {quoted} ORDER BY counter_value",
            )
            require(
                len(selected) == row_count,
                f"Expected {row_count} rows, got {len(selected)}",
            )
            for expected, actual in zip(rows, selected):
                require(
                    str(actual[0]) == expected[0]
                    and bytes(actual[1]) == expected[1]
                    and int(actual[2]) == expected[2],
                    "A selected row did not match its parameterized INSERT values",
                )
            updated = min(3, row_count)
            for index in range(updated):
                self.execute(
                    connection,
                    f"UPDATE {quoted} SET counter_value=%s WHERE marker=%s",
                    (100000 + index, rows[index][0]),
                )
            deleted = row_count // 2
            for index in range(row_count - deleted, row_count):
                self.execute(
                    connection,
                    f"DELETE FROM {quoted} WHERE marker=%s",
                    (rows[index][0],),
                )
            connection.commit()
            remaining_row = self.query_one(connection, f"SELECT COUNT(*) FROM {quoted}")
        remaining = int(remaining_row[0]) if remaining_row else -1
        require(remaining == row_count - deleted, "DELETE final count did not match")
        metrics = {
            "inserted_rows": row_count,
            "selected_rows": len(selected),
            "updated_rows": updated,
            "deleted_rows": deleted,
            "remaining_rows": remaining,
            "value_size_bytes": value_size,
            "table": table,
        }
        return CaseOutcome(
            "Parameterized INSERT/SELECT/UPDATE/DELETE roundtrip passed", metrics
        )

    def test_datatype(self) -> CaseOutcome:
        table = self.new_table("datatype")
        quoted = quote_identifier(table)
        self.create_table(
            table,
            "id BIGINT NOT NULL PRIMARY KEY, unicode_text TEXT NOT NULL, "
            "nullable_value VARCHAR(64) NULL, signed_value BIGINT NOT NULL, "
            "float_value DOUBLE NOT NULL, decimal_value DECIMAL(20,6) NOT NULL, "
            "datetime_value DATETIME(6) NOT NULL, text_value TEXT NOT NULL, "
            "blob_value LONGBLOB NOT NULL, binary_value VARBINARY(64) NOT NULL",
        )
        unicode_value = "\u4e2d\u6587 MariaDB \U0001f680"
        decimal_value = Decimal("123456789.123456")
        datetime_value = datetime(2026, 1, 2, 3, 4, 5, 654321)
        blob_value = b"\x00\xffmysql\x00mariadb"
        binary_value = bytes(range(32))
        params = (
            1,
            unicode_value,
            None,
            -9223372036854770000,
            3.141592653589793,
            decimal_value,
            datetime_value,
            "line1\nline2\ttext",
            blob_value,
            binary_value,
        )
        with self.connection() as connection:
            self.execute(
                connection,
                f"INSERT INTO {quoted} (id, unicode_text, nullable_value, signed_value, "
                "float_value, decimal_value, datetime_value, text_value, blob_value, "
                "binary_value) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                params,
            )
            connection.commit()
            row = self.query_one(
                connection,
                f"SELECT id, unicode_text, nullable_value, signed_value, float_value, "
                f"decimal_value, datetime_value, text_value, blob_value, binary_value FROM {quoted}",
            )
            facts = self._server_facts(connection)
        require(row is not None and len(row) == 10, "Datatype row was not returned")
        checks = [
            int(row[0]) == 1,
            str(row[1]) == unicode_value,
            row[2] is None,
            int(row[3]) == params[3],
            math.isclose(float(row[4]), float(params[4]), rel_tol=1e-12),
            Decimal(str(row[5])) == decimal_value,
            row[6] == datetime_value,
            str(row[7]) == params[7],
            bytes(row[8]) == blob_value,
            bytes(row[9]) == binary_value,
        ]
        require(all(checks), "One or more datatype values changed during roundtrip")
        metrics = {
            "types_checked": [
                "utf8mb4",
                "emoji",
                "NULL",
                "BIGINT",
                "DOUBLE",
                "DECIMAL",
                "DATETIME(6)",
                "TEXT",
                "BLOB",
                "VARBINARY",
            ],
            "server_family": facts["server_family"],
            "server_version": facts["server_version"],
            "table": table,
        }
        return CaseOutcome(
            "Text, numeric, temporal, NULL, and binary types roundtripped", metrics
        )

    def test_transaction(self) -> CaseOutcome:
        table = self.new_table("transaction")
        quoted = quote_identifier(table)
        self.create_table(
            table,
            "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
            "marker VARCHAR(191) NOT NULL UNIQUE, value_text VARCHAR(255) NOT NULL",
        )
        isolation = self.config["transaction"]["isolation_level"]
        with self.connection() as writer, self.connection() as observer:
            self.execute(writer, f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation}")
            writer.begin()
            self.execute(
                writer,
                f"INSERT INTO {quoted} (marker, value_text) VALUES (%s, %s)",
                (f"{self.run_id}_visibility", "uncommitted"),
            )
            before = self.query_one(
                observer,
                f"SELECT COUNT(*) FROM {quoted} WHERE marker=%s",
                (f"{self.run_id}_visibility",),
            )
            require(
                before and int(before[0]) == 0,
                "Uncommitted row was visible to another connection",
            )
            writer.commit()
        with self.connection() as observer:
            after = self.query_one(
                observer,
                f"SELECT COUNT(*) FROM {quoted} WHERE marker=%s",
                (f"{self.run_id}_visibility",),
            )
            require(after and int(after[0]) == 1, "Committed row was not visible")

        with self.connection() as connection:
            connection.begin()
            self.execute(
                connection,
                f"INSERT INTO {quoted} (marker, value_text) VALUES (%s, %s)",
                (f"{self.run_id}_rollback", "must disappear"),
            )
            connection.rollback()
            rolled_back = self.query_one(
                connection,
                f"SELECT COUNT(*) FROM {quoted} WHERE marker=%s",
                (f"{self.run_id}_rollback",),
            )
            require(
                rolled_back and int(rolled_back[0]) == 0, "ROLLBACK left a row behind"
            )

            connection.begin()
            try:
                self.execute(
                    connection,
                    f"INSERT INTO {quoted} (marker, value_text) VALUES (%s, %s)",
                    (f"{self.run_id}_partial_a", "first"),
                )
                self.execute(
                    connection,
                    f"INSERT INTO {quoted} (marker, value_text) VALUES (%s, %s)",
                    (f"{self.run_id}_partial_b", "second"),
                )
                raise RuntimeError("intentional transaction test exception")
            except RuntimeError:
                connection.rollback()
            partial = self.query_one(
                connection,
                f"SELECT COUNT(*) FROM {quoted} WHERE marker IN (%s, %s)",
                (f"{self.run_id}_partial_a", f"{self.run_id}_partial_b"),
            )
            require(
                partial and int(partial[0]) == 0,
                "Exception rollback left partial records",
            )

        autocommit_marker = f"{self.run_id}_autocommit"
        with self.connection() as connection:
            connection.autocommit(True)
            self.execute(
                connection,
                f"INSERT INTO {quoted} (marker, value_text) VALUES (%s, %s)",
                (autocommit_marker, "immediately visible"),
            )
        with self.connection() as observer:
            autocommit_row = self.query_one(
                observer,
                f"SELECT COUNT(*) FROM {quoted} WHERE marker=%s",
                (autocommit_marker,),
            )
        require(
            autocommit_row and int(autocommit_row[0]) == 1,
            "Autocommit row was not visible from a new connection",
        )

        transaction_count = self.config["transaction"]["transaction_count"]
        rollback_count = int(
            transaction_count * self.config["transaction"]["rollback_ratio"]
        )
        committed_count = 0
        with self.connection() as connection:
            for index in range(transaction_count):
                connection.begin()
                self.execute(
                    connection,
                    f"INSERT INTO {quoted} (marker, value_text) VALUES (%s, %s)",
                    (f"{self.run_id}_batch_{index}", str(index)),
                )
                if index < rollback_count:
                    connection.rollback()
                else:
                    connection.commit()
                    committed_count += 1
            batch_count = self.query_one(
                connection,
                f"SELECT COUNT(*) FROM {quoted} WHERE marker LIKE %s",
                (f"{self.run_id}_batch_%",),
            )
        require(
            batch_count and int(batch_count[0]) == committed_count,
            "Transaction commit/rollback final count mismatch",
        )
        metrics = {
            "isolation_level": isolation,
            "transactions": transaction_count,
            "committed": committed_count,
            "rolled_back": rollback_count,
            "cross_connection_visibility_checked": True,
            "exception_atomicity_checked": True,
            "autocommit_checked": True,
            "table": table,
        }
        return CaseOutcome(
            "Commit, rollback, isolation visibility, and exception atomicity passed",
            metrics,
        )

    def test_concurrency(self) -> CaseOutcome:
        table = self.new_table("concurrency")
        quoted = quote_identifier(table)
        self.create_table(
            table,
            "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, worker_id INT NOT NULL, "
            "operation_id BIGINT NOT NULL UNIQUE, payload VARBINARY(255) NOT NULL",
        )
        settings = self.config["concurrency"]
        operations = settings["operations"]
        workers = settings["workers"]
        allocations = [
            list(range(worker, operations, workers)) for worker in range(workers)
        ]
        lock = threading.Lock()
        errors: list[str] = []
        completed = 0

        def worker(worker_id: int, indexes: list[int]) -> int:
            local_completed = 0
            with self.connection() as connection:
                for operation_id in indexes:
                    try:
                        self.execute(
                            connection,
                            f"INSERT INTO {quoted} (worker_id, operation_id, payload) "
                            "VALUES (%s, %s, %s)",
                            (
                                worker_id,
                                operation_id,
                                f"op-{operation_id}".encode("ascii"),
                            ),
                        )
                        connection.commit()
                        row = self.query_one(
                            connection,
                            f"SELECT worker_id FROM {quoted} WHERE operation_id=%s",
                            (operation_id,),
                        )
                        require(
                            row is not None and int(row[0]) == worker_id,
                            "Concurrent readback mismatch",
                        )
                        local_completed += 1
                    except Exception as exc:
                        try:
                            connection.rollback()
                        except Exception:
                            pass
                        with lock:
                            if len(errors) < 20:
                                errors.append(sanitize_text(exc, self._secrets))
            return local_completed

        started = time.monotonic()
        timeout = max(
            self.config["sql"]["statement_timeout_seconds"],
            self.config["connection"]["read_timeout_seconds"]
            * math.ceil(operations / workers),
        )
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="mysql-concurrency"
        ) as pool:
            futures = [
                pool.submit(worker, index, chunk)
                for index, chunk in enumerate(allocations)
            ]
            try:
                for future in as_completed(futures, timeout=timeout):
                    completed += future.result()
            except FutureTimeout as exc:
                for future in futures:
                    future.cancel()
                raise CaseFailure(
                    f"Concurrency suite exceeded its {timeout:.1f}s bounded timeout",
                    {"completed": completed, "errors": errors},
                ) from exc
        elapsed = time.monotonic() - started
        with self.connection() as connection:
            count_row = self.query_one(connection, f"SELECT COUNT(*) FROM {quoted}")
            duplicate_row = self.query_one(
                connection,
                f"SELECT COUNT(*) FROM (SELECT operation_id FROM {quoted} "
                "GROUP BY operation_id HAVING COUNT(*) > 1) AS duplicates",
            )
        final_count = int(count_row[0]) if count_row else -1
        duplicate_count = int(duplicate_row[0]) if duplicate_row else -1
        metrics = {
            "workers": workers,
            "connection_limit": settings["connections"],
            "max_in_flight": settings["max_in_flight"],
            "operations": operations,
            "completed": completed,
            "final_count": final_count,
            "duplicate_operation_ids": duplicate_count,
            "errors": errors,
            "deadlock_count": sum("deadlock" in error.lower() for error in errors),
            "lock_wait_timeout_count": sum(
                "lock wait" in error.lower() for error in errors
            ),
            "duration_seconds": round(elapsed, 3),
            "table": table,
        }
        require(
            not errors, f"Concurrent workers reported {len(errors)} error(s)", metrics
        )
        require(
            completed == operations,
            "Some concurrent operations did not complete",
            metrics,
        )
        require(
            final_count == operations,
            "Concurrent final row count indicates data loss",
            metrics,
        )
        require(duplicate_count == 0, "Duplicate operation IDs were stored", metrics)
        return CaseOutcome(
            "Bounded independent connections completed without loss or duplicates",
            metrics,
        )

    def test_index(self) -> CaseOutcome:
        table = self.new_table("index")
        index_name = ("idx_lookup_" + uuid.uuid4().hex[:8])[:64]
        quoted = quote_identifier(table)
        quoted_index = quote_identifier(index_name)
        self.create_table(
            table,
            "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
            "lookup_value VARCHAR(191) NOT NULL, payload VARBINARY(255) NOT NULL",
        )
        row_count = max(50, self.config["sql"]["rows"])
        rows = [
            (f"lookup-{index % 17}", f"payload-{index}".encode("ascii"))
            for index in range(row_count)
        ]
        target = "lookup-7"
        with self.connection() as connection:
            self.executemany(
                connection,
                f"INSERT INTO {quoted} (lookup_value, payload) VALUES (%s, %s)",
                rows,
            )
            connection.commit()
            before_plan = self.query_mapping(
                connection,
                f"EXPLAIN SELECT payload FROM {quoted} WHERE lookup_value=%s",
                (target,),
            )
            started = time.monotonic()
            for _ in range(20):
                self.query_all(
                    connection,
                    f"SELECT payload FROM {quoted} WHERE lookup_value=%s",
                    (target,),
                )
            without_index_ms = (time.monotonic() - started) * 1000 / 20
            self.execute(
                connection, f"CREATE INDEX {quoted_index} ON {quoted} (lookup_value)"
            )
            connection.commit()
            after_plan = self.query_mapping(
                connection,
                f"EXPLAIN SELECT payload FROM {quoted} WHERE lookup_value=%s",
                (target,),
            )
            started = time.monotonic()
            for _ in range(20):
                self.query_all(
                    connection,
                    f"SELECT payload FROM {quoted} WHERE lookup_value=%s",
                    (target,),
                )
            with_index_ms = (time.monotonic() - started) * 1000 / 20
        selected_key = after_plan.get("key")
        access_type = str(after_plan.get("type", "")).upper()
        index_used = selected_key == index_name or access_type not in {"", "ALL"}
        metrics = {
            "rows": row_count,
            "index_name": index_name,
            "index_used": index_used,
            "explain_before": before_plan,
            "explain_after": after_plan,
            "without_index_average_ms": round(without_index_ms, 3),
            "with_index_average_ms": round(with_index_ms, 3),
            "table": table,
        }
        require(index_used, "EXPLAIN did not select the generated index", metrics)
        return CaseOutcome(
            "Generated index is usable; before/after latency was recorded", metrics
        )

    def _role_secret(self, role: str) -> tuple[str, str] | None:
        permissions = self.config["permissions"]
        username = permissions.get(f"{role}_username")
        env_name = permissions.get(f"{role}_password_env")
        if not username or not env_name:
            return None
        password = os.getenv(env_name)
        if password is None:
            raise CaseFailure(
                f"Configured permissions secret environment variable {env_name} is not set"
            )
        if password:
            self._secrets.add(password)
        return str(username), password

    def test_permissions(self) -> CaseOutcome:
        if not self.config["permissions"]["enabled"]:
            raise CaseSkip(
                "Permissions checks are disabled; no dedicated role accounts were used"
            )
        roles = {
            role: self._role_secret(role)
            for role in ("read_only", "read_write", "no_access")
        }
        if not any(roles.values()):
            raise CaseSkip("No dedicated permissions accounts are configured")
        table = self.new_table("permissions")
        quoted = quote_identifier(table)
        self.create_table(
            table,
            "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, marker VARCHAR(191) NOT NULL UNIQUE",
        )
        with self.connection() as connection:
            self.execute(
                connection,
                f"INSERT INTO {quoted} (marker) VALUES (%s)",
                (f"{self.run_id}_owner",),
            )
            connection.commit()
        metrics: dict[str, Any] = {"roles": {}, "table": table}

        read_only = roles["read_only"]
        if read_only:
            with self.connection(
                username=read_only[0], password=read_only[1]
            ) as connection:
                row = self.query_one(connection, f"SELECT COUNT(*) FROM {quoted}")
                require(
                    row is not None and int(row[0]) >= 1,
                    "Read-only account could not read the test table",
                )
                rejected = False
                try:
                    self.execute(
                        connection,
                        f"INSERT INTO {quoted} (marker) VALUES (%s)",
                        (f"{self.run_id}_readonly",),
                    )
                    connection.commit()
                except Exception:
                    rejected = True
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                require(
                    rejected,
                    "Configured read-only account was able to write the test table",
                )
            metrics["roles"]["read_only"] = "read_allowed_write_rejected"

        read_write = roles["read_write"]
        if read_write:
            marker = f"{self.run_id}_readwrite"
            with self.connection(
                username=read_write[0], password=read_write[1]
            ) as connection:
                self.execute(
                    connection, f"INSERT INTO {quoted} (marker) VALUES (%s)", (marker,)
                )
                connection.commit()
                row = self.query_one(
                    connection,
                    f"SELECT COUNT(*) FROM {quoted} WHERE marker=%s",
                    (marker,),
                )
                require(
                    row is not None and int(row[0]) == 1,
                    "Read-write account readback failed",
                )
                self.execute(
                    connection, f"DELETE FROM {quoted} WHERE marker=%s", (marker,)
                )
                connection.commit()
            metrics["roles"]["read_write"] = "read_write_delete_allowed"

        no_access = roles["no_access"]
        if no_access:
            rejected = False
            try:
                with self.connection(
                    username=no_access[0], password=no_access[1]
                ) as connection:
                    self.query_one(connection, f"SELECT COUNT(*) FROM {quoted}")
            except Exception:
                rejected = True
            require(rejected, "Configured no-access account could read the test table")
            metrics["roles"]["no_access"] = "connection_or_read_rejected"
        return CaseOutcome(
            "Explicitly configured role-account permissions matched expectations",
            metrics,
        )

    def test_replication(self) -> CaseOutcome:
        settings = self.config["replication"]
        if not settings["enabled"]:
            raise CaseSkip(
                "Replication is disabled or endpoints were not explicitly configured"
            )
        primary = parse_endpoint(settings["primary_endpoint"])
        replica = parse_endpoint(settings["replica_endpoint"])
        table = self.new_table("replication")
        quoted = quote_identifier(table)
        self.create_table(
            table,
            "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, marker VARCHAR(191) NOT NULL UNIQUE, "
            "payload VARBINARY(255) NOT NULL",
            primary,
        )
        marker = f"{self.run_id}_replicated"
        payload = uuid.uuid4().hex.encode("ascii")
        written_at = time.monotonic()
        with self.connection(primary) as connection:
            self.execute(
                connection,
                f"INSERT INTO {quoted} (marker, payload) VALUES (%s, %s)",
                (marker, payload),
            )
            connection.commit()
        deadline = written_at + settings["max_replication_lag_seconds"]
        replicated = False
        poll_errors: list[str] = []
        polls = 0
        while time.monotonic() <= deadline:
            polls += 1
            try:
                with self.connection(replica) as connection:
                    row = self.query_one(
                        connection,
                        f"SELECT payload FROM {quoted} WHERE marker=%s",
                        (marker,),
                    )
                if row is not None and bytes(row[0]) == payload:
                    replicated = True
                    break
            except Exception as exc:
                if len(poll_errors) < 10:
                    poll_errors.append(sanitize_text(exc, self._secrets))
            time.sleep(settings["polling_interval_seconds"])
        lag = time.monotonic() - written_at
        metrics = {
            "primary_endpoint": f"{primary[0]}:{primary[1]}",
            "replica_endpoint": f"{replica[0]}:{replica[1]}",
            "replicated": replicated,
            "observed_lag_seconds": round(lag, 3),
            "polls": polls,
            "poll_errors": poll_errors,
            "table": table,
        }
        require(
            replicated,
            "Replica did not expose the committed row within the configured lag limit",
            metrics,
        )
        return CaseOutcome(
            "Primary write became byte-for-byte visible on the replica", metrics
        )

    def test_failover(self) -> CaseOutcome:
        settings = self.config["failover"]
        if not settings["enabled"]:
            raise CaseSkip(
                "Failover observation is disabled and no switch will be triggered"
            )
        endpoint = (
            parse_endpoint(settings["endpoint"])
            if settings["endpoint"]
            else (self.config["connection"]["host"], self.config["connection"]["port"])
        )
        table = self.new_table("failover")
        quoted = quote_identifier(table)
        self.create_table(
            table,
            "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, marker VARCHAR(191) NOT NULL UNIQUE",
            endpoint,
        )
        seed = f"{self.run_id}_seed"
        with self.connection(endpoint) as connection:
            self.execute(
                connection, f"INSERT INTO {quoted} (marker) VALUES (%s)", (seed,)
            )
            connection.commit()
        started = time.monotonic()
        deadline = started + settings["duration_seconds"]
        outage_started: float | None = None
        recovery_seconds: list[float] = []
        errors: list[str] = []
        error_count = 0
        probes = 0
        successes = 0
        while time.monotonic() < deadline:
            probes += 1
            try:
                with self.connection(endpoint) as connection:
                    row = self.query_one(
                        connection,
                        f"SELECT COUNT(*) FROM {quoted} WHERE marker=%s",
                        (seed,),
                    )
                require(
                    row is not None and int(row[0]) == 1,
                    "Seed row missing after reconnect",
                )
                successes += 1
                if outage_started is not None:
                    recovery_seconds.append(time.monotonic() - outage_started)
                    outage_started = None
            except CaseFailure:
                raise
            except Exception as exc:
                error_count += 1
                if outage_started is None:
                    outage_started = time.monotonic()
                if len(errors) < 20:
                    errors.append(sanitize_text(exc, self._secrets))
            time.sleep(settings["polling_interval_seconds"])
        if outage_started is not None:
            recovery_seconds.append(time.monotonic() - outage_started)
        max_recovery = max(recovery_seconds, default=0.0)
        metrics = {
            "endpoint": f"{endpoint[0]}:{endpoint[1]}",
            "observation_seconds": round(time.monotonic() - started, 3),
            "probes": probes,
            "successful_probes": successes,
            "connection_errors": error_count,
            "error_samples": errors,
            "recovery_windows_seconds": [round(value, 3) for value in recovery_seconds],
            "max_recovery_seconds": round(max_recovery, 3),
            "seed_row_consistent": True,
            "table": table,
        }
        require(
            successes > 0,
            "No successful database probe occurred during failover observation",
            metrics,
        )
        require(
            max_recovery <= settings["max_recovery_seconds"],
            "Observed recovery time exceeded failover.max_recovery_seconds",
            metrics,
        )
        if error_count == 0:
            message = "No connection interruption was observed; an external switch was not proven"
            if settings["require_observed_interruption"]:
                raise CaseFailure(message, metrics)
            raise CaseWarning(message, metrics)
        return CaseOutcome(
            "Observed interruption, reconnect, and committed seed consistency", metrics
        )

    def test_performance(self) -> CaseOutcome:
        table = self.new_table("performance")
        quoted = quote_identifier(table)
        self.create_table(
            table,
            "id BIGINT NOT NULL PRIMARY KEY, lookup_value BIGINT NOT NULL, payload LONGBLOB NOT NULL, "
            "INDEX idx_lookup_value (lookup_value)",
        )
        settings = self.config["performance"]
        total_rows = settings["rows"]
        workers = settings["workers"]
        batch_size = settings["batch_size"]
        value_size = self.config["sql"]["value_size"]
        payload = b"p" * value_size
        deadline = time.monotonic() + settings["duration_seconds"]
        write_latencies: deque[float] = deque(maxlen=settings["max_latency_samples"])
        read_latencies: deque[float] = deque(maxlen=settings["max_latency_samples"])
        latency_lock = threading.Lock()

        def write_worker(worker_id: int) -> int:
            indexes = range(worker_id, total_rows, workers)
            completed = 0
            with self.connection() as connection:
                for offset in range(0, len(indexes), batch_size):
                    if time.monotonic() > deadline:
                        raise CaseFailure("Performance write exceeded duration_seconds")
                    chunk = indexes[offset : offset + batch_size]
                    rows = [(index, index % 101, payload) for index in chunk]
                    began = time.monotonic()
                    self.executemany(
                        connection,
                        f"INSERT INTO {quoted} (id, lookup_value, payload) VALUES (%s, %s, %s)",
                        rows,
                    )
                    connection.commit()
                    elapsed_ms = (time.monotonic() - began) * 1000
                    with latency_lock:
                        write_latencies.append(elapsed_ms / max(1, len(chunk)))
                    completed += len(chunk)
            return completed

        began = time.monotonic()
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="mysql-performance-write"
        ) as pool:
            written = sum(
                future.result()
                for future in as_completed(
                    [pool.submit(write_worker, worker) for worker in range(workers)]
                )
            )
        write_seconds = time.monotonic() - began
        require(written == total_rows, "Performance write count mismatch")

        def read_worker(worker_id: int) -> int:
            completed = 0
            with self.connection() as connection:
                for index in range(worker_id, total_rows, workers):
                    if time.monotonic() > deadline:
                        raise CaseFailure("Performance read exceeded duration_seconds")
                    began_read = time.monotonic()
                    row = self.query_one(
                        connection,
                        f"SELECT payload FROM {quoted} WHERE id=%s",
                        (index,),
                    )
                    elapsed_ms = (time.monotonic() - began_read) * 1000
                    require(
                        row is not None and len(bytes(row[0])) == value_size,
                        "Performance readback mismatch",
                    )
                    with latency_lock:
                        read_latencies.append(elapsed_ms)
                    completed += 1
            return completed

        read_started = time.monotonic()
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="mysql-performance-read"
        ) as pool:
            read = sum(
                future.result()
                for future in as_completed(
                    [pool.submit(read_worker, worker) for worker in range(workers)]
                )
            )
        read_seconds = time.monotonic() - read_started
        elapsed = time.monotonic() - began
        combined = list(write_latencies) + list(read_latencies)
        total_operations = written + read
        tps = total_operations / max(elapsed, 0.000001)
        p50 = percentile(combined, 0.50)
        p95 = percentile(combined, 0.95)
        p99 = percentile(combined, 0.99)
        metrics = {
            "rows_written": written,
            "rows_read": read,
            "batch_size": batch_size,
            "workers": workers,
            "value_size_bytes": value_size,
            "write_seconds": round(write_seconds, 3),
            "read_seconds": round(read_seconds, 3),
            "duration_seconds": round(elapsed, 3),
            "operations_per_second": round(tps, 3),
            "average_latency_ms": round(sum(combined) / max(1, len(combined)), 3),
            "p50_ms": round(p50, 3),
            "p95_ms": round(p95, 3),
            "p99_ms": round(p99, 3),
            "error_rate": 0.0,
            "latency_sample_count": len(combined),
            "table": table,
        }
        require(read == total_rows, "Performance read count mismatch", metrics)
        minimum = settings["min_tps"]
        require(
            minimum is None or tps >= minimum,
            f"TPS {tps:.2f} is below configured minimum {minimum}",
            metrics,
        )
        maximum_p95 = settings["max_p95_ms"]
        require(
            maximum_p95 is None or p95 <= maximum_p95,
            f"p95 {p95:.2f}ms exceeds {maximum_p95}ms",
            metrics,
        )
        maximum_p99 = settings["max_p99_ms"]
        require(
            maximum_p99 is None or p99 <= maximum_p99,
            f"p99 {p99:.2f}ms exceeds {maximum_p99}ms",
            metrics,
        )
        return CaseOutcome(
            "Bounded batch-write and indexed-read performance sample completed", metrics
        )

    def test_soak(self) -> CaseOutcome:
        table = self.new_table("soak")
        quoted = quote_identifier(table)
        self.create_table(
            table,
            "id BIGINT NOT NULL PRIMARY KEY, marker VARCHAR(191) NOT NULL, "
            "payload VARBINARY(255) NOT NULL",
        )
        settings = self.config["soak"]
        stop = threading.Event()
        worker_failed = threading.Event()
        lock = threading.Lock()
        latencies: deque[float] = deque(maxlen=settings["max_latency_samples"])
        error_samples: deque[str] = deque(maxlen=20)
        counters = {
            "operations": 0,
            "successes": 0,
            "failures": 0,
            "reads": 0,
            "writes": 0,
        }
        started = time.monotonic()
        stop_reason = "unknown"
        interrupted = False
        drain_timed_out = False

        def claim_operation() -> int | None:
            with lock:
                maximum = settings["max_operations"]
                if maximum and counters["operations"] >= maximum:
                    return None
                operation_id = counters["operations"]
                counters["operations"] += 1
                return operation_id

        def worker(worker_id: int) -> None:
            try:
                with self.connection() as connection:
                    while not stop.is_set():
                        operation_id = claim_operation()
                        if operation_id is None:
                            stop.set()
                            return
                        began_operation = time.monotonic()
                        is_read = random.random() < settings["read_write_ratio"]
                        try:
                            if is_read:
                                row = self.query_one(
                                    connection, f"SELECT COUNT(*) FROM {quoted}"
                                )
                                require(
                                    row is not None and int(row[0]) >= 0,
                                    "Soak read returned no count",
                                )
                            else:
                                self.execute(
                                    connection,
                                    f"INSERT INTO {quoted} (id, marker, payload) VALUES (%s, %s, %s) "
                                    "ON DUPLICATE KEY UPDATE marker=VALUES(marker), payload=VALUES(payload)",
                                    (
                                        operation_id % settings["max_rows"],
                                        f"{self.run_id}_{worker_id}_{operation_id}",
                                        f"soak-{operation_id}".encode("ascii"),
                                    ),
                                )
                                connection.commit()
                            elapsed_ms = (time.monotonic() - began_operation) * 1000
                            with lock:
                                counters["successes"] += 1
                                counters["reads" if is_read else "writes"] += 1
                                latencies.append(elapsed_ms)
                        except Exception as exc:
                            try:
                                connection.rollback()
                            except Exception:
                                pass
                            with lock:
                                counters["failures"] += 1
                                error_samples.append(sanitize_text(exc, self._secrets))
                            worker_failed.set()
                            stop.set()
                            return
                        if stop.wait(settings["operation_interval_seconds"]):
                            return
            except Exception as exc:
                with lock:
                    counters["failures"] += 1
                    error_samples.append(sanitize_text(exc, self._secrets))
                worker_failed.set()
                stop.set()

        pool = ThreadPoolExecutor(
            max_workers=settings["workers"], thread_name_prefix="mysql-soak"
        )
        futures = [pool.submit(worker, index) for index in range(settings["workers"])]
        next_status = started + settings["status_interval_seconds"]
        try:
            while not stop.is_set():
                now = time.monotonic()
                if (
                    settings["duration_seconds"] > 0
                    and now - started >= settings["duration_seconds"]
                ):
                    stop_reason = "duration_elapsed"
                    stop.set()
                    break
                if worker_failed.is_set():
                    stop_reason = "worker_error"
                    stop.set()
                    break
                if now >= next_status:
                    with lock:
                        snapshot = dict(counters)
                        status_samples = list(latencies)
                    duration = max(now - started, 0.000001)
                    error_rate = snapshot["failures"] / max(1, snapshot["operations"])
                    average_latency = sum(status_samples) / max(1, len(status_samples))
                    print(
                        "[STATUS] soak "
                        f"operations={snapshot['operations']} successes={snapshot['successes']} "
                        f"failures={snapshot['failures']} tps={snapshot['successes'] / duration:.2f} "
                        f"error_rate={error_rate:.4f} avg_latency_ms={average_latency:.2f} "
                        f"p95_ms={percentile(status_samples, 0.95):.2f}"
                    )
                    next_status = now + settings["status_interval_seconds"]
                time.sleep(min(0.05, settings["operation_interval_seconds"]))
            if stop_reason == "unknown":
                if worker_failed.is_set():
                    stop_reason = "worker_error"
                elif settings["max_operations"]:
                    stop_reason = "max_operations"
                else:
                    stop_reason = "stopped"
        except KeyboardInterrupt:
            interrupted = True
            stop_reason = "user_interrupt"
            stop.set()
        finally:
            drain_deadline = time.monotonic() + settings["drain_timeout_seconds"]
            for future in futures:
                remaining = drain_deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    future.result(timeout=remaining)
                except FutureTimeout:
                    drain_timed_out = True
                    error_samples.append("A soak worker exceeded drain_timeout_seconds")
                    break
                except Exception as exc:
                    with lock:
                        counters["failures"] += 1
                        error_samples.append(sanitize_text(exc, self._secrets))
                    worker_failed.set()
            pool.shutdown(wait=False)
        duration = time.monotonic() - started
        with lock:
            final = dict(counters)
            samples = list(latencies)
            errors = list(error_samples)
        metrics = {
            **final,
            "workers": settings["workers"],
            "duration_seconds": round(duration, 3),
            "operations_per_second": round(
                final["successes"] / max(duration, 0.000001), 3
            ),
            "error_rate": round(final["failures"] / max(1, final["operations"]), 6),
            "average_latency_ms": round(sum(samples) / max(1, len(samples)), 3),
            "p50_ms": round(percentile(samples, 0.50), 3),
            "p95_ms": round(percentile(samples, 0.95), 3),
            "p99_ms": round(percentile(samples, 0.99), 3),
            "latency_sample_count": len(samples),
            "latency_sample_limit": settings["max_latency_samples"],
            "database_row_limit": settings["max_rows"],
            "stop_reason": stop_reason,
            "interrupted_by_user": interrupted,
            "drain_timed_out": drain_timed_out,
            "error_samples": errors,
            "table": table,
        }
        require(
            not worker_failed.is_set(),
            "A soak worker failed; new work was stopped",
            metrics,
        )
        require(
            not drain_timed_out,
            "Soak workers did not drain within the configured timeout",
            metrics,
        )
        return CaseOutcome(
            "Soak workers stopped and connections drained cleanly", metrics
        )

    def suite_methods(self) -> dict[str, Callable[[], CaseOutcome]]:
        return {
            "connectivity": self.test_connectivity,
            "authentication": self.test_authentication,
            "sql_roundtrip": self.test_sql_roundtrip,
            "datatype": self.test_datatype,
            "transaction": self.test_transaction,
            "concurrency": self.test_concurrency,
            "index": self.test_index,
            "permissions": self.test_permissions,
            "replication": self.test_replication,
            "failover": self.test_failover,
            "performance": self.test_performance,
            "soak": self.test_soak,
        }


def _sanitize_value(value: Any, secrets: Iterable[str]) -> Any:
    if isinstance(value, dict):
        return {str(key): _sanitize_value(item, secrets) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_value(item, secrets) for item in value]
    if isinstance(value, str):
        return sanitize_text(value, secrets)
    return value


def write_report(
    runner: MySQLTestRunner,
    config: dict[str, Any],
    selected_suites: Sequence[str],
    report_path: str | None,
    *,
    exit_code: int,
    duration_seconds: float,
    interrupted: bool,
) -> Path:
    if report_path:
        destination = Path(report_path).expanduser().resolve()
    else:
        directory = Path(config["report"]["directory"]).expanduser()
        if not directory.is_absolute():
            directory = PROJECT_ROOT / directory
        destination = directory.resolve() / f"mysql-mariadb-report-{runner.run_id}.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.is_dir():
        raise ValueError(f"Report path is a directory: {destination}")
    summary = {status: 0 for status in STATUSES}
    for result in runner.results:
        summary[result.status] += 1
    summary["total"] = len(runner.results)
    safe_config = sanitized_config(config)
    safe_config["authentication"]["password_present"] = (
        config["authentication"]["mode"] != "none"
    )
    payload = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "tool": "mysql-mariadb-instance-tester",
        "run_id": runner.run_id,
        "started_at": runner.started_at,
        "completed_at": utc_now(),
        "target": {
            "host": config["connection"]["host"],
            "port": config["connection"]["port"],
            "database": config["connection"]["database"],
            "username": config["authentication"]["username"],
        },
        "selected_suites": list(selected_suites),
        "summary": summary,
        "results": [asdict(result) for result in runner.results],
        "duration": round(duration_seconds, 6),
        "exit_code": exit_code,
        "interrupted": interrupted,
        "sanitized_config": safe_config,
        "cleanup": {
            "policy": config["execution"]["cleanup_policy"],
            "generated_tables": [table for _endpoint, table in runner._tables],
            "cleaned_tables": runner.cleaned_tables,
            "retained_tables": [
                table
                for _endpoint, table in runner._tables
                if table not in set(runner.cleaned_tables)
            ],
            "errors": runner.cleanup_errors,
        },
        "safety": {
            "control_plane_api_used": False,
            "business_tables_modified": False,
            "generated_test_tables_only": True,
        },
    }
    payload = _sanitize_value(payload, runner._secrets)
    temporary = destination.with_name(destination.name + ".tmp-" + uuid.uuid4().hex)
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(str(temporary), str(destination))
    finally:
        if temporary.exists():
            temporary.unlink()
    return destination


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.list_suites:
        print_suite_list()
        return 0
    try:
        config = load_config(args.config)
        apply_cli_overrides(config, args)
        validate_config(config)
        selected_suites = choose_suites(config)
        if pymysql is None:
            raise ValueError(
                "PyMySQL is not installed; install dependencies with "
                "python -m pip install -r requirements.txt"
            )
        password = resolve_password(config)
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    runner = MySQLTestRunner(config, password)
    started = time.monotonic()
    interrupted = False
    hard_interrupted = False
    methods = runner.suite_methods()
    for suite in selected_suites:
        try:
            result = runner.run_case(suite, methods[suite])
        except KeyboardInterrupt:
            interrupted = True
            hard_interrupted = True
            runner.results.append(
                TestResult(
                    suite, "FAIL", 0.0, "Interrupted by user before suite cleanup", {}
                )
            )
            print(f"[FAIL] {suite}: interrupted by user", file=sys.stderr)
            break
        if result.status == "FAIL" and config["execution"]["fail_fast"]:
            break
        if result.metrics.get("interrupted_by_user"):
            interrupted = True
            break

    successful = not hard_interrupted and not any(
        result.status == "FAIL" for result in runner.results
    )
    runner.cleanup(successful)
    if runner.cleanup_errors:
        runner.results.append(
            TestResult(
                "cleanup",
                "FAIL",
                0.0,
                f"Failed to remove {len(runner.cleanup_errors)} generated test table(s)",
                {"errors": runner.cleanup_errors},
            )
        )
    exit_code = (
        130
        if hard_interrupted
        else (1 if any(result.status == "FAIL" for result in runner.results) else 0)
    )
    duration = time.monotonic() - started
    try:
        report = write_report(
            runner,
            config,
            selected_suites,
            args.report,
            exit_code=exit_code,
            duration_seconds=duration,
            interrupted=interrupted,
        )
    except (OSError, ValueError) as exc:
        print(f"Report error: {sanitize_text(exc, runner._secrets)}", file=sys.stderr)
        return exit_code or 1
    summary = {
        status: sum(result.status == status for result in runner.results)
        for status in STATUSES
    }
    print(
        "Summary: "
        + " ".join(f"{status}={summary[status]}" for status in STATUSES)
        + f" duration={duration:.2f}s"
    )
    print(f"Report: {report}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
