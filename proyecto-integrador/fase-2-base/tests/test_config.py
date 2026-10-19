from territorial_pipeline.config import get_profile


def test_full_profile_exceeds_big_data_requirement():
    profile = get_profile("full")
    assert profile.solicitudes == 50_000
    assert profile.eventos > 10_000_000


def test_small_profile_is_fast_validation_mode():
    profile = get_profile("small")
    assert profile.solicitudes == 5_000
    assert profile.eventos < 500_000
