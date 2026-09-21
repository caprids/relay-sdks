# Media fixtures

`mediabunny-avc.mp4` is an original FFmpeg-generated test pattern: four 32×32 frames, AVC High
profile, 10 fps, keyframes at frames 0 and 2, no B-frames or audio. It exercises Mediabunny's real
MP4 demuxer and encoder-provided avcC, without requiring WebCodecs, FFmpeg, or a relay at test time.

Regenerate from the repository root with FFmpeg and libx264 installed:

```sh
ffmpeg -hide_banner -loglevel error -f lavfi -i 'testsrc2=size=32x32:rate=10' \
  -frames:v 4 -c:v libx264 -profile:v high -pix_fmt yuv420p -g 2 -bf 0 \
  -fflags +bitexact -flags:v +bitexact -map_metadata -1 -movflags +faststart \
  ts/tests/fixtures/mediabunny-avc.mp4
```

`mediabunny-avc-bframes.mp4` uses eight frames with one B-frame between reference frames and a
keyframe every three frames. It tests decode-order pacing, preserved presentation timestamps,
and skipping a keyframe while relay demand is absent:

```sh
ffmpeg -hide_banner -loglevel error -f lavfi -i 'testsrc2=size=32x32:rate=10' \
  -frames:v 8 -c:v libx264 -profile:v high -pix_fmt yuv420p -g 3 -bf 1 \
  -x264-params b-adapt=0:scenecut=0 -fflags +bitexact -flags:v +bitexact \
  -map_metadata -1 -movflags +faststart ts/tests/fixtures/mediabunny-avc-bframes.mp4
```
