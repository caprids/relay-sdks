# Public vocabulary

Stage 1 media format names. Python exports these from `moq_media`; TypeScript exports them from
`@mux/moq-media`. Other product API vocabulary will be recorded separately (#73).

| Concept | TypeScript | Python |
| --- | --- | --- |
| Video description | `VideoTrackInfo` | `VideoTrackInfo` |
| Audio description | `AudioTrackInfo` | `AudioTrackInfo` |
| Data description | `DataTrackInfo` | `DataTrackInfo` |
| Catalog input | `MsfCatalogInput` | `build_msf_catalog` keyword arguments |
| Build independent catalog bytes | `buildMsfCatalog` | `build_msf_catalog` |
| LOC track properties | `locTrackProperties` | `loc_track_properties` |
| LOC object properties from microseconds | `locProperties` / `LocPropertiesOptions` | `loc_properties` keyword arguments |
| Compose source URL | `composeMoqSource` / `ComposeMoqSourceOptions` | `compose_player_url` keyword arguments |
| Escape UTF-8 component | `encodeNameComponent` | `encode_name_component` |
| Encode namespace plus track name | `encodeNamespaceName` | `encode_namespace_name` |
| H.264 configuration record | `H264Config` | `H264Config` |
| Configuration from Annex B | `H264Config.fromAnnexb` | `H264Config.from_annexb` |
| Configuration bytes / codec string | `H264Config.avcc` / `.codec` | `H264Config.avcc` / `.codec` |
| Configuration's access-unit conversion | `H264Config.toAvcc` | `H264Config.to_avcc` |
| Split Annex B NALs | `iterAnnexbNalUnits` | `iter_annexb_nal_units` |
| NAL type | `nalUnitType` | `nal_unit_type` |
| Extract SPS/PPS | `extractParameterSets` | `extract_parameter_sets` |
| Build avcC | `buildAvcc` | `build_avcc` |
| Codec string from avcC header | `avcCodecString` | `avc_codec_string` |
| Convert Annex B access unit | `annexbToAvcc` | `annexb_to_avcc` |

Constants use the same spelling in both languages: `LOC_DOCUMENT`, `MSF_DOCUMENT`,
`MSF_CATALOG_VERSION`, `MICROSECONDS_PER_SECOND`, `CATALOG_TRACK`, `VIDEO_TRACK`, `AUDIO_TRACK`,
`LOC_TIMESTAMP`, `LOC_TIMESCALE`, `LOC_VIDEO_CONFIG`, and `LOC_AUDIO_CONFIG`.
Python also retains the example's `TRACK_TIMESCALE_PROPERTIES` as a tuple; new code should call
`loc_track_properties()` for fresh properties.

Track fields use camelCase in TypeScript and snake_case in Python (`initData` / `init_data`,
`channelConfig` / `channel_config`). Catalog generation time is `generatedAt` / `generated_at_ms`.
No publishing-handle or public-reader names have been committed yet.
