#!/bin/bash
# go install github.com/gotesttools/gotestfmt/v2/cmd/gotestfmt@latest

cd /workspace
pwd

CGO_ENABLED=1 \
EGRESS_CONFIG_FILE=/out/config.yaml \
go test -tags=integration ./test \
  -run '^TestEgress/IsolatedAudioRecording/[0-9]+/IsolatedAudio_StaggeredJoinOffsetsOrder$' \
  -v -count=1 -timeout 30m
