"""Format regressions moved from the Video.js example."""

import base64
import json

import pytest

from moq_media import (
    AudioTrackInfo,
    VideoTrackInfo,
    annexb_to_avcc,
    avc_codec_string,
    build_avcc,
    build_msf_catalog,
    compose_player_url,
    encode_name_component,
    encode_namespace_name,
    extract_parameter_sets,
    iter_annexb_nal_units,
    loc_properties,
)


class TestAnnexB:
    def test_splits_three_and_four_byte_start_codes(self) -> None:
        stream = b"\x00\x00\x00\x01\x67\xaa" + b"\x00\x00\x01\x68\xbb\xcc" + b"\x00\x00\x00\x01\x65\x01\x02"

        assert list(iter_annexb_nal_units(stream)) == [b"\x67\xaa", b"\x68\xbb\xcc", b"\x65\x01\x02"]

    def test_converts_an_access_unit_to_length_prefixed_nals_without_parameter_sets(self) -> None:
        stream = b"\x00\x00\x00\x01\x67\xaa" + b"\x00\x00\x00\x01\x68\xbb" + b"\x00\x00\x00\x01\x09\xf0"
        stream += b"\x00\x00\x00\x01\x06\x05\xff" + b"\x00\x00\x01\x65\x88\x84"

        assert annexb_to_avcc(stream) == b"\x00\x00\x00\x03\x06\x05\xff" + b"\x00\x00\x00\x03\x65\x88\x84"
        assert annexb_to_avcc(stream, drop_parameter_sets=False).startswith(b"\x00\x00\x00\x02\x67\xaa")

    def test_builds_an_avcc_and_derives_the_codec_string(self) -> None:
        sps = bytes.fromhex("6742c014da05067e7840000003004000000f23c5")
        pps = bytes.fromhex("68ce3c80")
        found_sps, found_pps = extract_parameter_sets(b"\x00\x00\x00\x01" + sps + b"\x00\x00\x00\x01" + pps)

        assert found_sps == [sps]
        assert found_pps == [pps]

        avcc = build_avcc(found_sps, found_pps)

        assert avcc[0] == 1
        assert avcc[1:4] == sps[1:4]
        assert avcc[4] == 0xFF  # 4-byte NAL lengths
        assert avcc[5] == 0xE1  # one SPS
        assert avcc[6:8] == len(sps).to_bytes(2, "big")
        assert avcc[8 : 8 + len(sps)] == sps
        assert avcc[8 + len(sps)] == 1  # one PPS
        assert avcc.endswith(len(pps).to_bytes(2, "big") + pps)
        # Constrained Baseline, level 2.0: profile 0x42, constraints 0xC0, level 0x14.
        assert avc_codec_string(avcc) == "avc1.42C014"

        with pytest.raises(ValueError):
            build_avcc([], [pps])


class TestCatalog:
    def test_matches_the_video_js_builder_shape(self) -> None:
        avcc = bytes.fromhex("0142c014ffe1000a6742c014da05067e784001000468ce3c80")
        document = build_msf_catalog(
            ["mattypoo", "tv"],
            video=VideoTrackInfo(
                codec="avc1.42C014", width=640, height=360, framerate=30, bitrate=1_500_000, init_data=avcc
            ),
            audio=AudioTrackInfo(bitrate=96_000),
            generated_at_ms=1_700_000_000_000,
        )
        catalog = json.loads(document)

        assert catalog["version"] == "draft-01"
        assert catalog["isComplete"] is True
        assert catalog["generatedAt"] == 1_700_000_000_000
        video, audio = catalog["tracks"]
        assert video == {
            "namespace": "mattypoo/tv",
            "packaging": "loc",
            "isLive": True,
            "initRef": "video-init",
            "name": "video",
            "role": "video",
            "codec": "avc1.42C014",
            "width": 640,
            "height": 360,
            "framerate": 30,
            "bitrate": 1_500_000,
        }
        assert audio == {
            "namespace": "mattypoo/tv",
            "packaging": "loc",
            "isLive": True,
            "name": "audio",
            "role": "audio",
            "codec": "opus",
            "samplerate": 48_000,
            "channelConfig": "2",
            "bitrate": 96_000,
        }
        assert catalog["initDataList"] == [
            {"id": "video-init", "type": "inline", "data": base64.b64encode(avcc).decode()}
        ]

    def test_omits_absent_fields_and_the_init_data_list(self) -> None:
        catalog = json.loads(build_msf_catalog(["ns"], video=VideoTrackInfo(codec="vp8", width=320, height=180)))

        assert "generatedAt" not in catalog
        assert "initDataList" not in catalog
        assert "initRef" not in catalog["tracks"][0]
        assert "framerate" not in catalog["tracks"][0]


class TestLocProperties:
    def test_carries_timestamp_timescale_and_config_on_keyframes(self) -> None:
        keyframe = loc_properties(1_000_000, video_config=b"\x01\x42")
        delta = loc_properties(1_033_333)

        assert [(p.type, p.value) for p in keyframe] == [(0x10, 1_000_000), (0x08, 1_000_000), (0x0D, b"\x01\x42")]
        assert [(p.type, p.value) for p in delta] == [(0x10, 1_033_333), (0x08, 1_000_000)]


class TestPlayerUrl:
    def test_encodes_names_like_the_video_js_parser_expects(self) -> None:
        assert encode_name_component("relay_sdk") == "relay_sdk"
        assert encode_name_component("relay-sdk") == "relay.2dsdk"
        assert encode_name_component("a/b") == "a.2fb"
        assert encode_namespace_name(["mattypoo", "tv"], "catalog") == "mattypoo-tv--catalog"

    def test_composes_the_moqt_source_url_with_the_token_in_the_fragment(self) -> None:
        url = compose_player_url("https://sjc.relay.mux.global", ["mattypoo", "relay-sdk-example"], token="a b/c")

        assert url == "moqt://sjc.relay.mux.global/#msf:mattypoo-relay.2dsdk.2dexample--catalog&c4m=a%20b%2Fc"
        assert (
            compose_player_url("moqt://relay.example.com/moq?x=1", ["ns"])
            == "moqt://relay.example.com/moq?x=1#msf:ns--catalog"
        )

        with pytest.raises(ValueError):
            compose_player_url("http://relay.example.com", ["ns"])

        with pytest.raises(ValueError):
            compose_player_url("https://relay.example.com", [])
