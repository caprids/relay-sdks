"""``media_subscribe_parameters``: a viewer's subscriber priority for a media track (the "Media subscribing" section of
``spec/behavior.md``), mirrored by ``ts/tests/media-subscribe-parameters.test.ts``.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from moq_core import MessageParameters, NextObjectFilter
from moq_core.control_messages import ControlMessageDeframer, Subscribe, decode_control_message, encode_subscribe
from mux_relay.media import media_subscribe_parameters, media_track_properties

DEFAULT_PUBLISHER_PRIORITY = 0x0E
REPO = Path(__file__).resolve().parents[4]
PRIORITY = json.loads((REPO / "spec" / "protocol.json").read_text())["mediaSubscribeParameters"]["subscriberPriority"]


def test_puts_audio_ahead_of_video_with_the_subscriber_priorities_spec_protocol_json_pins() -> None:
    assert PRIORITY == {"audio": 16, "video": 96}
    assert media_subscribe_parameters("audio") == MessageParameters(subscriber_priority=PRIORITY["audio"])
    assert media_subscribe_parameters("video") == MessageParameters(subscriber_priority=PRIORITY["video"])


def test_matches_the_publisher_priority_media_track_properties_gives_the_same_kind() -> None:
    def publisher_priority(properties: list[Any]) -> int:
        return next(p.value for p in properties if p.type == DEFAULT_PUBLISHER_PRIORITY)

    audio = media_track_properties("audio")
    video = media_track_properties("video", keyframe_interval_us=1_000_000)
    assert media_subscribe_parameters("audio").subscriber_priority == publisher_priority(audio)
    assert media_subscribe_parameters("video").subscriber_priority == publisher_priority(video)


def test_copies_the_overrides_onto_a_new_result_and_takes_an_explicit_priority_from_0_to_255() -> None:
    overrides = MessageParameters(location_filter=NextObjectFilter())
    result = media_subscribe_parameters("video", overrides)
    assert result == MessageParameters(location_filter=NextObjectFilter(), subscriber_priority=96)
    assert result is not overrides
    assert overrides == MessageParameters(location_filter=NextObjectFilter())
    assert media_subscribe_parameters("audio", MessageParameters(subscriber_priority=0)).subscriber_priority == 0
    assert media_subscribe_parameters("video", MessageParameters(subscriber_priority=255)).subscriber_priority == 255


def test_returns_a_new_object_each_time() -> None:
    media_subscribe_parameters("audio").subscriber_priority = 1
    assert media_subscribe_parameters("audio") == MessageParameters(subscriber_priority=16)


def test_is_carried_by_subscribe_as_subscriber_priority() -> None:
    request = Subscribe(
        request_id=2, track_namespace=["room"], track_name="audio", parameters=media_subscribe_parameters("audio")
    )
    [frame] = ControlMessageDeframer().push(encode_subscribe(request))
    decoded = decode_control_message(frame)
    assert isinstance(decoded, Subscribe)
    assert decoded.parameters.subscriber_priority == 16


def test_refuses_a_kind_that_is_not_a_string_with_a_type_error_and_another_string_with_a_value_error() -> None:
    wrong: Any = 1
    with pytest.raises(TypeError):
        media_subscribe_parameters(wrong)
    with pytest.raises(ValueError):
        media_subscribe_parameters("data")  # ty: ignore[invalid-argument-type]


def test_refuses_overrides_that_are_not_message_parameters() -> None:
    wrong: Any = {"subscriber_priority": 16}
    with pytest.raises(TypeError):
        media_subscribe_parameters("audio", wrong)


def test_refuses_a_subscriber_priority_that_is_not_an_integer_from_0_to_255() -> None:
    wrong: tuple[Any, ...] = ("16", True, 1.0, float("nan"))
    for value in wrong:
        with pytest.raises(TypeError):
            media_subscribe_parameters("audio", MessageParameters(subscriber_priority=value))
    for value in (-1, 256):
        with pytest.raises(ValueError):
            media_subscribe_parameters("audio", MessageParameters(subscriber_priority=value))
