"""Tests for the variable-length monitor_thresholds.ini format (v2).

Covers:
- 3-part sections (db/os) — no path, no metric_name
- 4+ part cloud sections — path + metric_name required for API calls
- Exact (source, path, rule_id) lookup
- Legacy (source, rule_id) fallback when path is omitted
- Derived metadata (namespace, resource_provider, resource_type)
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from monitoring.threshold_checker import ThresholdChecker


@pytest.fixture
def v2_ini(tmp_path: Path) -> ThresholdChecker:
    ini = tmp_path / "thresholds.ini"
    ini.write_text(textwrap.dedent("""\
        [metric.os.cpu_utilization]
        critical = 90
        warning  = 80
        operator = >
        unit     = percent
        window   = 1
        enabled  = true

        [metric.db.active_connections]
        critical = 800
        operator = >
        unit     = count
        window   = 1
        enabled  = true

        [metric.aws.cloudwatch.RDS.CPUUtilization]
        metric_name = CPUUtilization
        critical    = 90
        operator    = >
        unit        = percent
        window      = 1
        enabled     = true

        [metric.aws.pi.RDS.os_memory_free_avg]
        metric_name = os.memory.free.avg
        critical    = 268435456
        operator    = <
        unit        = bytes
        window      = 1
        enabled     = false

        [metric.azure.azuremonitor.DBforMySQL.flexibleServers.cpu_percent]
        metric_name = cpu_percent
        critical    = 90
        operator    = >
        unit        = percent
        window      = 1
        enabled     = true

        [metric.gcp.cloudmonitoring.cloudsql.database.cpu_utilization]
        metric_name = cloudsql.googleapis.com/database/cpu/utilization
        critical    = 0.90
        operator    = >
        unit        = ratio
        window      = 1
        enabled     = true
    """), encoding="utf-8")
    return ThresholdChecker(config_path=ini)


class TestIniV2Parsing:
    def test_os_rule_has_empty_path(self, v2_ini):
        rule = v2_ini.get_rule("os", "cpu_utilization")
        assert rule is not None
        assert rule.path == ()
        assert rule.api == ""
        assert rule.metric_name == "cpu_utilization"

    def test_aws_cloudwatch_rule_metadata(self, v2_ini):
        rule = v2_ini.get_rule(
            "aws", "CPUUtilization", path=("cloudwatch", "RDS"),
        )
        assert rule is not None
        assert rule.api == "cloudwatch"
        assert rule.namespace == "AWS/RDS"
        assert rule.metric_name == "CPUUtilization"
        assert rule.service_type == ""

    def test_aws_pi_rule_disabled_by_default(self, v2_ini):
        rule = v2_ini.get_rule(
            "aws", "os_memory_free_avg", path=("pi", "RDS"),
        )
        assert rule is not None
        assert rule.enabled is False
        assert rule.metric_name == "os.memory.free.avg"
        assert rule.service_type == "RDS"

    def test_azure_resource_provider_derived(self, v2_ini):
        rule = v2_ini.get_rule(
            "azure", "cpu_percent",
            path=("azuremonitor", "DBforMySQL", "flexibleServers"),
        )
        assert rule is not None
        assert rule.resource_provider == "Microsoft.DBforMySQL/flexibleServers"
        assert rule.metric_name == "cpu_percent"

    def test_gcp_resource_type_derived(self, v2_ini):
        rule = v2_ini.get_rule(
            "gcp", "cpu_utilization",
            path=("cloudmonitoring", "cloudsql", "database"),
        )
        assert rule is not None
        assert rule.resource_type == "cloudsql_database"
        assert rule.metric_name == (
            "cloudsql.googleapis.com/database/cpu/utilization"
        )

    def test_legacy_lookup_without_path(self, v2_ini):
        """get_rule(source, metric) with no path still finds cloud rules."""
        rule = v2_ini.get_rule("aws", "CPUUtilization")
        assert rule is not None
        assert rule.api == "cloudwatch"

    def test_exact_path_required_when_path_given(self, v2_ini):
        assert v2_ini.get_rule(
            "aws", "CPUUtilization", path=("pi", "RDS"),
        ) is None

    def test_list_rules_filter_by_api(self, v2_ini):
        pi_rules = v2_ini.list_rules(source="aws", api="pi", enabled_only=False)
        assert len(pi_rules) == 1
        assert pi_rules[0].metric == "os_memory_free_avg"

    def test_list_rules_filter_by_path(self, v2_ini):
        rules = v2_ini.list_rules(
            source="azure",
            path=("azuremonitor", "DBforMySQL", "flexibleServers"),
        )
        assert len(rules) == 1
        assert rules[0].metric == "cpu_percent"


class TestIniV2Evaluation:
    def test_check_with_path_fires(self, v2_ini):
        alert = v2_ini.check(
            "aws", "CPUUtilization", 95.0,
            instance_id="t", path=("cloudwatch", "RDS"),
        )
        assert alert is not None
        assert "cloudwatch" in alert.message

    def test_check_without_path_fires_legacy(self, v2_ini):
        alert = v2_ini.check("aws", "CPUUtilization", 95.0, instance_id="t")
        assert alert is not None

    def test_check_many_with_path(self, v2_ini):
        alerts = v2_ini.check_many(
            "gcp", {"cpu_utilization": 0.95},
            instance_id="t",
            path=("cloudmonitoring", "cloudsql", "database"),
        )
        assert len(alerts) == 1
        assert "cloudmonitoring" in alerts[0].message

    def test_disabled_pi_rule_never_fires(self, v2_ini):
        for _ in range(5):
            alert = v2_ini.check(
                "aws", "os_memory_free_avg", 1.0,
                instance_id="t", path=("pi", "RDS"),
            )
        assert alert is None

    def test_section_id_property(self, v2_ini):
        rule = v2_ini.get_rule(
            "azure", "cpu_percent",
            path=("azuremonitor", "DBforMySQL", "flexibleServers"),
        )
        assert rule.section_id == (
            "metric.azure.azuremonitor.DBforMySQL.flexibleServers.cpu_percent"
        )


class TestProductionIni:
    """Smoke tests against the real monitor_thresholds.ini shipped with the tool."""

    @pytest.fixture(scope="class")
    def prod_checker(self):
        return ThresholdChecker()

    def test_at_least_100_rules(self, prod_checker):
        assert len(prod_checker.all_rules()) >= 100

    def test_all_cloud_rules_have_metric_name(self, prod_checker):
        for rule in prod_checker.all_rules():
            if rule.source in ("aws", "azure", "gcp"):
                assert rule.metric_name, (
                    f"{rule.section_id} missing metric_name"
                )

    def test_aws_cw_enabled_count(self, prod_checker):
        cw = prod_checker.list_rules(source="aws", api="cloudwatch")
        assert len(cw) >= 10

    def test_azure_all_resource_providers_seeded(self, prod_checker):
        """All 8 Azure resource-provider variants must have at least one rule."""
        providers = {
            r.resource_provider
            for r in prod_checker.all_rules()
            if r.source == "azure" and r.resource_provider
        }
        expected = {
            "Microsoft.Sql/servers",
            "Microsoft.DBforMySQL/flexibleServers",
            "Microsoft.DBforMySQL/servers",
            "Microsoft.DBforPostgreSQL/flexibleServers",
            "Microsoft.DBforPostgreSQL/servers",
            "Microsoft.DBforMariaDB/servers",
            "Microsoft.DocumentDB/databaseAccounts",
            "Microsoft.Cache/Redis",
        }
        assert expected.issubset(providers), f"Missing: {expected - providers}"

    def test_gcp_rules_use_full_metric_type_uri(self, prod_checker):
        for rule in prod_checker.list_rules(source="gcp"):
            assert rule.metric_name.startswith("cloudsql.googleapis.com/"), (
                f"{rule.section_id} metric_name should be a full GCP URI"
            )
