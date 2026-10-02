from weather_platform.transform.dbt_runner import history_fingerprint

_VARS = {"window_anchor": "latest_common", "window_lookback_days": 730}


def test_fingerprint_tracks_unit_correction_seed(tmp_path):
    seed = tmp_path / "source_unit_corrections.csv"
    seed.write_text("element,source_flag,scale_factor,unit,evidence\n")
    empty = history_fingerprint(_VARS, tmp_path)
    assert history_fingerprint(_VARS, tmp_path) == empty  # stable

    seed.write_text(seed.read_text() + "WSFG,C,0.0277778,m/s,evidence\n")
    assert history_fingerprint(_VARS, tmp_path) != empty


def test_fingerprint_ignores_vars_that_dont_affect_history(tmp_path):
    base = history_fingerprint(_VARS, tmp_path)
    assert history_fingerprint({**_VARS, "min_completeness_pct": 50}, tmp_path) == base
    assert history_fingerprint({**_VARS, "window_lookback_days": 365}, tmp_path) != base
