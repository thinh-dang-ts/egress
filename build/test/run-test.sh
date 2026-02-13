#!/bin/bash
go install github.com/gotesttools/gotestfmt/v2/cmd/gotestfmt@latest

cd /workspace
pwd

# Rebuild the egress handler binary so code changes are picked up
echo "Building egress handler binary..."
CGO_ENABLED=1 GODEBUG=disablethp=1 go build -tags deadlock -o /bin/egress ./cmd/server || exit 1

# Build and run integration tests
# TEST_PATTERN='TestEgress/IsolatedAudioRecording/4/IsolatedAudio_MergeToStereo'
TEST_PATTERN='TestMergeRecordingFromEtcDataProducesOutputInTmp'
CGO_ENABLED=1 go test -c -v -race -tags=integration -o test.test ./test && go tool test2json -p egress ./test.test $TEST_PATTERN -test.v -test.timeout 30m 2>&1 | "$HOME"/go/bin/gotestfmt