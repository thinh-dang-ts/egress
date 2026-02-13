#!/bin/bash
# go install github.com/gotesttools/gotestfmt/v2/cmd/gotestfmt@latest

cd /workspace
pwd

# CGO_ENABLED=1 \
# EGRESS_CONFIG_FILE=/out/config.yaml \
# go test -tags=integration ./test \
#   # -run '^TestEgress/IsolatedAudioRecording/[0-9]+/IsolatedAudio_StaggeredJoinOffsetsOrder$' \
#   -run 'TestMergeRecordingFromEtcDataProducesOutputInTmp' \
#   -v -count=1 -timeout 30m

CGO_ENABLED=1 go test -c -v -race -tags=integration -o test.test ./test
go test ./test -run TestMergeRecordingFromEtcDataProducesOutputInTmp -v -count=1