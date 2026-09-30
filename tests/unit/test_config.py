import pytest
from pydantic import ValidationError

from tests.conftest import make_config
from weather_platform.config import DEFAULT_CONFIG_PATH, WindowAnchor, load_pipeline_config


def test_valid_config_and_dbt_vars(tmp_path):
    cfg = make_config(tmp_path)
    dbt_vars = cfg.dbt_vars()
    assert dbt_vars["window_anchor"] == "latest_common"
    assert dbt_vars["window_lookback_days"] == 30
    assert dbt_vars["rejected_qflags"] == ["*"]
    assert "stations" not in dbt_vars  # stations travel as data, not vars


def test_station_needs_exactly_one_selector(tmp_path):
    with pytest.raises(ValidationError, match="exactly one"):
        make_config(tmp_path, stations=[{"city": "X"}])
    with pytest.raises(ValidationError, match="exactly one"):
        make_config(
            tmp_path,
            stations=[
                {
                    "city": "X",
                    "station_id": "CA000000001",
                    "match": {"country_code": "CA", "name_pattern": "X*"},
                }
            ],
        )


def test_invalid_station_id_rejected(tmp_path):
    with pytest.raises(ValidationError):
        make_config(tmp_path, stations=[{"city": "X", "station_id": "not-an-id"}])


def test_duplicate_city_rejected(tmp_path):
    with pytest.raises(ValidationError, match="Duplicate city"):
        make_config(
            tmp_path,
            stations=[
                {"city": "Alpha", "station_id": "CA000000001"},
                {"city": "alpha", "station_id": "CA000000002"},
            ],
        )


def test_fixed_window_requires_end_date(tmp_path):
    with pytest.raises(ValidationError, match="end_date"):
        make_config(tmp_path, analysis_window={"anchor": "fixed"})
    cfg = make_config(tmp_path, analysis_window={"anchor": "fixed", "end_date": "2024-01-31"})
    assert cfg.analysis_window.anchor is WindowAnchor.FIXED
    assert cfg.dbt_vars()["window_end_date"] == "2024-01-31"


def test_unknown_keys_are_rejected(tmp_path):
    with pytest.raises(ValidationError):
        make_config(tmp_path, quality={"min_completness_pct": 50})  # typo


def test_repo_config_is_valid():
    cfg = load_pipeline_config(DEFAULT_CONFIG_PATH)
    assert len(cfg.stations) >= 5
