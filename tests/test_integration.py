from __future__ import annotations

import copy
import os
import unittest

import mysql_mariadb_instance_test as target


@unittest.skipUnless(
    os.getenv("MARIADB_INTEGRATION") == "1",
    "Set MARIADB_INTEGRATION=1 for the dedicated database integration test",
)
class IntegrationTests(unittest.TestCase):
    def test_connectivity_roundtrip_and_transaction(self) -> None:
        required = ["MARIADB_HOST", "MARIADB_DATABASE", "MARIADB_USER"]
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            self.fail("Missing integration setting(s): " + ", ".join(missing))
        password_env = os.getenv("MARIADB_PASSWORD_ENV", "MARIADB_PASSWORD")
        if password_env not in os.environ:
            self.fail(f"Password environment variable {password_env} is not set")
        config = copy.deepcopy(target.DEFAULT_CONFIG)
        config["connection"].update(
            {
                "host": os.environ["MARIADB_HOST"],
                "port": int(os.getenv("MARIADB_PORT", "3306")),
                "database": os.environ["MARIADB_DATABASE"],
            }
        )
        config["authentication"].update(
            {
                "username": os.environ["MARIADB_USER"],
                "password_env": password_env,
                "mode": "environment",
            }
        )
        ssl_ca = os.getenv("MARIADB_SSL_CA")
        if ssl_ca:
            config["connection"].update(
                {"ssl_mode": "verify_ca", "ssl_ca_location": ssl_ca}
            )
        config["execution"]["cleanup_policy"] = "always"
        config["sql"]["rows"] = 5
        config["transaction"]["transaction_count"] = 4
        target.validate_config(config)
        runner = target.MySQLTestRunner(config, os.environ[password_env])
        try:
            outcomes = [
                runner.test_connectivity(),
                runner.test_sql_roundtrip(),
                runner.test_transaction(),
            ]
            self.assertEqual(len(outcomes), 3)
        finally:
            runner.cleanup(successful=True)
        self.assertFalse(runner.cleanup_errors)


if __name__ == "__main__":
    unittest.main()
