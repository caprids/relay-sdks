"""``media_track_properties``: a media track's default delivery timeout (the "Media publishing" section of
``spec/behavior.md``), mirrored by ``ts/tests/media-track-properties.test.ts``.
"""

from typing import Any, cast

import pytest

from moq_core import KeyValuePair
from mux_relay._validation import track_property_pairs
from mux_relay.media import media_track_properties

SUBGROUP_DELIVERY_TIMEOUT = 0x06


def timeout_ms(properties: list[KeyValuePair]) -> int | bytes:
    assert len(properties) == 1
    assert properties[0].type == SUBGROUP_DELIVERY_TIMEOUT
    return properties[0].value


def test_audio_gets_a_500_ms_subgroup_timeout_and_no_object_timeout() -> None:
    assert media_track_properties("audio") == [KeyValuePair(SUBGROUP_DELIVERY_TIMEOUT, 500)]


def test_audio_ignores_the_keyframe_interval_and_target_latency() -> None:
    properties = media_track_properties("audio", keyframe_interval_us=2_000_000, target_latency_us=8_000_000)
    assert timeout_ms(properties) == 500


def test_video_gets_one_keyframe_interval_by_default() -> None:
    assert timeout_ms(media_track_properties("video", keyframe_interval_us=1_000_000)) == 1000


def test_video_gets_the_target_latency_less_the_keyframe_interval() -> None:
    properties = media_track_properties("video", keyframe_interval_us=500_000, target_latency_us=3_000_000)
    assert timeout_ms(properties) == 2500


def test_video_never_gets_less_than_one_keyframe_interval() -> None:
    properties = media_track_properties("video", keyframe_interval_us=1_000_000, target_latency_us=1_200_000)
    assert timeout_ms(properties) == 1000


def test_an_explicit_subgroup_timeout_replaces_the_default_for_either_kind() -> None:
    assert timeout_ms(media_track_properties("audio", subgroup_timeout_us=250_000)) == 250
    properties = media_track_properties("video", keyframe_interval_us=1_000_000, subgroup_timeout_us=4_000_000)
    assert timeout_ms(properties) == 4000


def test_rounds_up_to_whole_milliseconds() -> None:
    assert timeout_ms(media_track_properties("audio", subgroup_timeout_us=1)) == 1
    assert timeout_ms(media_track_properties("video", keyframe_interval_us=33_334)) == 34


def test_returns_properties_register_track_accepts() -> None:
    properties = media_track_properties("video", keyframe_interval_us=2_000_000)
    assert track_property_pairs(properties) == [KeyValuePair(SUBGROUP_DELIVERY_TIMEOUT, 2000)]


def test_refuses_a_kind_that_is_not_a_string_with_type_error_and_another_string_with_value_error() -> None:
    with pytest.raises(TypeError):
        media_track_properties(cast(Any, 1))
    with pytest.raises(ValueError):
        media_track_properties(cast(Any, "data"))


def test_refuses_video_without_a_keyframe_interval() -> None:
    with pytest.raises(TypeError):
        media_track_properties("video")
    with pytest.raises(TypeError):
        media_track_properties("video", subgroup_timeout_us=1_000_000)


def test_refuses_a_target_latency_below_the_keyframe_interval() -> None:
    with pytest.raises(ValueError):
        media_track_properties("video", keyframe_interval_us=1_000_000, target_latency_us=999_999)


@pytest.mark.parametrize("name", ["keyframe_interval_us", "target_latency_us", "subgroup_timeout_us"])
def test_checks_each_duration(name: str) -> None:
    base: Any = {"keyframe_interval_us": 1, "target_latency_us": 1}
    wrong: tuple[Any, ...] = ("1000", True, 1.0, 1.5, float("nan"), float("inf"))
    for value in wrong:
        with pytest.raises(TypeError):
            media_track_properties("video", **{**base, name: value})
    for value in (0, -1, 2_147_483_647_001):
        with pytest.raises(ValueError):
            media_track_properties("video", **{**base, name: value})
    media_track_properties("audio", **{name: 2_147_483_647_000})
