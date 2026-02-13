// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build cgo

package merge

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/egress/pkg/types"
)

func installFakeGstLaunch(t *testing.T) string {
	t.Helper()

	binDir := t.TempDir()
	logPath := path.Join(t.TempDir(), "gst_args.log")
	binPath := path.Join(binDir, "gst-launch-1.0")

	script := `#!/bin/sh
set -eu
if [ -n "${TEST_GST_LOG:-}" ]; then
  printf '%s\n' "$*" >> "$TEST_GST_LOG"
fi
if [ "${TEST_GST_FAIL:-0}" = "1" ]; then
  exit 1
fi
out=""
for arg in "$@"; do
  case "$arg" in
    location=*) out="${arg#location=}" ;;
  esac
done
if [ -n "$out" ]; then
  mkdir -p "$(dirname "$out")"
  printf 'merged-audio' > "$out"
fi
`
	if err := os.WriteFile(binPath, []byte(script), 0o755); err != nil {
		t.Fatalf("failed to write fake gst-launch-1.0: %v", err)
	}

	t.Setenv("TEST_GST_LOG", logPath)
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
	return logPath
}

func installFakeFfmpeg(t *testing.T) string {
	t.Helper()

	binDir := t.TempDir()
	logPath := path.Join(t.TempDir(), "ffmpeg_args.log")
	binPath := path.Join(binDir, "ffmpeg")

	script := `#!/bin/sh
set -eu
if [ -n "${TEST_FFMPEG_LOG:-}" ]; then
  printf '%s\n' "$*" >> "$TEST_FFMPEG_LOG"
fi
if [ "${TEST_FFMPEG_FAIL:-0}" = "1" ]; then
  exit 1
fi
in=""
prev=""
last=""
for arg in "$@"; do
  if [ "$prev" = "-i" ]; then
    in="$arg"
  fi
  prev="$arg"
  last="$arg"
done
if [ -n "$last" ]; then
  mkdir -p "$(dirname "$last")"
  if [ -n "$in" ] && [ -f "$in" ]; then
    cat "$in" > "$last"
  else
    printf 'filled-audio' > "$last"
  fi
fi
`
	if err := os.WriteFile(binPath, []byte(script), 0o755); err != nil {
		t.Fatalf("failed to write fake ffmpeg: %v", err)
	}

	t.Setenv("TEST_FFMPEG_LOG", logPath)
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
	return logPath
}

func TestNearestOpusRate(t *testing.T) {
	tests := []struct {
		name       string
		sampleRate int32
		expected   int32
	}{
		{name: "exact supported", sampleRate: 48000, expected: 48000},
		{name: "rounds down", sampleRate: 44100, expected: 48000},
		{name: "rounds up", sampleRate: 10000, expected: 8000},
		{name: "middle value", sampleRate: 20000, expected: 16000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := nearestOpusRate(tc.sampleRate); got != tc.expected {
				t.Fatalf("nearestOpusRate(%d) = %d, want %d", tc.sampleRate, got, tc.expected)
			}
		})
	}
}

func TestNormalizeS3ObjectKey(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		input  string
		want   string
	}{
		{
			name:   "plain key unchanged",
			bucket: "egress-integration",
			input:  "room/session/manifest.json",
			want:   "room/session/manifest.json",
		},
		{
			name:   "path-style url",
			bucket: "egress-integration",
			input:  "http://minio:9000/egress-integration/room/session/manifest.json",
			want:   "room/session/manifest.json",
		},
		{
			name:   "path-style url with leading slash key",
			bucket: "egress-integration",
			input:  "http://minio:9000/egress-integration//home/egress/uploads/manifest.json",
			want:   "/home/egress/uploads/manifest.json",
		},
		{
			name:   "virtual-hosted style url",
			bucket: "egress-integration",
			input:  "https://egress-integration.s3.us-east-1.amazonaws.com/room/session/manifest.json",
			want:   "room/session/manifest.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeS3ObjectKey(tt.bucket, tt.input)
			if got != tt.want {
				t.Fatalf("normalizeS3ObjectKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalizeGCSObjectKey(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		input  string
		want   string
	}{
		{
			name:   "plain key unchanged",
			bucket: "gcs-bucket",
			input:  "room/session/manifest.json",
			want:   "room/session/manifest.json",
		},
		{
			name:   "virtual-hosted style url",
			bucket: "gcs-bucket",
			input:  "https://gcs-bucket.storage.googleapis.com/room/session/manifest.json",
			want:   "room/session/manifest.json",
		},
		{
			name:   "path-style url",
			bucket: "gcs-bucket",
			input:  "https://storage.googleapis.com/gcs-bucket/room/session/manifest.json",
			want:   "room/session/manifest.json",
		},
		{
			name:   "signed path-style url",
			bucket: "gcs-bucket",
			input:  "https://storage.googleapis.com/gcs-bucket/room/session/manifest.json?X-Goog-Algorithm=GOOG4-RSA-SHA256",
			want:   "room/session/manifest.json",
		},
		{
			name:   "url-encoded key",
			bucket: "gcs-bucket",
			input:  "https://gcs-bucket.storage.googleapis.com/room%2Fsession%2Fmanifest.json",
			want:   "room/session/manifest.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeGCSObjectKey(tt.bucket, tt.input)
			if got != tt.want {
				t.Fatalf("normalizeGCSObjectKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestMergedManifestObjectKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "manifest json key",
			input: "room/session/manifest.json",
			want:  "room/session/manifest_merged.json",
		},
		{
			name:  "egress manifest key",
			input: "room/session/egress_manifest.json",
			want:  "room/session/egress_manifest_merged.json",
		},
		{
			name:  "no extension",
			input: "room/session/manifest",
			want:  "room/session/manifest_merged",
		},
		{
			name:  "filename only",
			input: "manifest.json",
			want:  "manifest_merged.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergedManifestObjectKey(tt.input)
			if got != tt.want {
				t.Fatalf("mergedManifestObjectKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestMixedAudioFilename(t *testing.T) {
	tests := []struct {
		name     string
		roomName string
		format   types.AudioRecordingFormat
		want     string
	}{
		{
			name:     "ogg with simple room name",
			roomName: "egress-test",
			format:   types.AudioRecordingFormatOGGOpus,
			want:     "room_mix_egress-test.ogg",
		},
		{
			name:     "wav with spaces and symbols",
			roomName: "Team / Sync #1",
			format:   types.AudioRecordingFormatWAVPCM,
			want:     "room_mix_Team_Sync_1.wav",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mixedAudioFilename(tt.roomName, tt.format)
			if got != tt.want {
				t.Fatalf("mixedAudioFilename(%q, %q) = %q, want %q", tt.roomName, tt.format, got, tt.want)
			}
		})
	}
}

func TestDownloadManifestLocal(t *testing.T) {
	tmpDir := t.TempDir()
	manifestPath := path.Join(tmpDir, "manifest.json")

	manifest := &config.AudioRecordingManifest{
		RoomName:   "room-a",
		SessionID:  "session-a",
		SampleRate: 32000,
		Formats:    []types.AudioRecordingFormat{types.AudioRecordingFormatOGGOpus},
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	if err = os.WriteFile(manifestPath, data, 0o644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	w := &MergeWorker{}
	got, err := w.downloadManifest(context.Background(), manifestPath, tmpDir)
	if err != nil {
		t.Fatalf("downloadManifest() error = %v", err)
	}

	if got.RoomName != manifest.RoomName {
		t.Fatalf("RoomName = %q, want %q", got.RoomName, manifest.RoomName)
	}
	if got.SessionID != manifest.SessionID {
		t.Fatalf("SessionID = %q, want %q", got.SessionID, manifest.SessionID)
	}
}

func TestDownloadParticipantFilesLocalPrefersOgg(t *testing.T) {
	manifest := &config.AudioRecordingManifest{
		Participants: []*config.ParticipantRecordingInfo{
			{
				ParticipantID: "p1",
				Artifacts: []*config.AudioArtifact{
					{Format: types.AudioRecordingFormatWAVPCM, StorageURI: "/tmp/p1.wav", Filename: "p1.wav"},
					{Format: types.AudioRecordingFormatOGGOpus, StorageURI: "/tmp/p1.ogg", Filename: "p1.ogg"},
				},
			},
			{
				ParticipantID: "p2",
				Artifacts:     nil,
			},
			{
				ParticipantID: "p3",
				Artifacts: []*config.AudioArtifact{
					{Format: types.AudioRecordingFormatWAVPCM, StorageURI: "/tmp/p3.wav", Filename: "p3.wav"},
				},
			},
		},
	}

	w := &MergeWorker{}
	files, err := w.downloadParticipantFiles(context.Background(), manifest, t.TempDir())
	if err != nil {
		t.Fatalf("downloadParticipantFiles() error = %v", err)
	}

	if len(files) != 2 {
		t.Fatalf("downloadParticipantFiles() returned %d files, want 2", len(files))
	}
	if files["p1"] != "/tmp/p1.ogg" {
		t.Fatalf("p1 file = %q, want /tmp/p1.ogg", files["p1"])
	}
	if files["p3"] != "/tmp/p3.wav" {
		t.Fatalf("p3 file = %q, want /tmp/p3.wav", files["p3"])
	}
}

func TestUploadMergedFilesLocal(t *testing.T) {
	tmpDir := t.TempDir()
	manifestPath := path.Join(tmpDir, "out", "manifest.json")

	oggInput := path.Join(tmpDir, "input.ogg")
	wavInput := path.Join(tmpDir, "input.wav")

	if err := os.WriteFile(oggInput, []byte("ogg-data"), 0o644); err != nil {
		t.Fatalf("failed to create ogg input: %v", err)
	}
	if err := os.WriteFile(wavInput, []byte("wav-data"), 0o644); err != nil {
		t.Fatalf("failed to create wav input: %v", err)
	}

	mergedFiles := map[types.AudioRecordingFormat]string{
		types.AudioRecordingFormatOGGOpus: oggInput,
		types.AudioRecordingFormatWAVPCM:  wavInput,
	}

	manifest := &config.AudioRecordingManifest{
		RoomName:  "room-1",
		SessionID: "session-1",
	}

	w := &MergeWorker{}
	if err := w.uploadMergedFiles(context.Background(), manifest, mergedFiles, manifestPath); err != nil {
		t.Fatalf("uploadMergedFiles() error = %v", err)
	}

	if manifest.RoomMix == nil {
		t.Fatal("expected RoomMix to be initialized")
	}
	if manifest.RoomMix.Status != config.AudioRecordingStatusCompleted {
		t.Fatalf("RoomMix.Status = %q, want %q", manifest.RoomMix.Status, config.AudioRecordingStatusCompleted)
	}
	if len(manifest.RoomMix.Artifacts) != 2 {
		t.Fatalf("artifact count = %d, want 2", len(manifest.RoomMix.Artifacts))
	}

	artifactsByFormat := make(map[types.AudioRecordingFormat]*config.AudioArtifact)
	for _, a := range manifest.RoomMix.Artifacts {
		artifactsByFormat[a.Format] = a
	}

	if artifactsByFormat[types.AudioRecordingFormatOGGOpus] == nil {
		t.Fatal("expected OGG artifact")
	}
	if artifactsByFormat[types.AudioRecordingFormatWAVPCM] == nil {
		t.Fatal("expected WAV artifact")
	}

	if _, err := os.Stat(path.Join(path.Dir(manifestPath), "room_mix_room-1.ogg")); err != nil {
		t.Fatalf("expected copied room_mix_room-1.ogg: %v", err)
	}
	if _, err := os.Stat(path.Join(path.Dir(manifestPath), "room_mix_room-1.wav")); err != nil {
		t.Fatalf("expected copied room_mix_room-1.wav: %v", err)
	}
}

func TestUpdateManifestWithMergeResultsLocal(t *testing.T) {
	tmpDir := t.TempDir()
	manifestPath := path.Join(tmpDir, "manifest.json")

	manifest := &config.AudioRecordingManifest{
		RoomName:  "room-2",
		SessionID: "session-2",
	}
	manifest.InitRoomMix()
	manifest.SetRoomMixStatus(config.AudioRecordingStatusCompleted, "")

	w := &MergeWorker{}
	if err := w.updateManifestWithMergeResults(context.Background(), manifestPath, manifest); err != nil {
		t.Fatalf("updateManifestWithMergeResults() error = %v", err)
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed reading manifest: %v", err)
	}

	var got config.AudioRecordingManifest
	if err = json.Unmarshal(data, &got); err != nil {
		t.Fatalf("failed unmarshalling written manifest: %v", err)
	}

	if got.RoomMix == nil {
		t.Fatal("written manifest missing room_mix")
	}
	if got.RoomMix.Status != config.AudioRecordingStatusCompleted {
		t.Fatalf("written room_mix.status = %q, want %q", got.RoomMix.Status, config.AudioRecordingStatusCompleted)
	}
}

func TestRunMergePipelineUsesNearestOpusRate(t *testing.T) {
	logPath := installFakeGstLaunch(t)

	w := &MergeWorker{}
	alignment := &AlignmentResult{
		Alignments: []*AlignmentInfo{
			{
				ParticipantID: "p1",
				Offset:        10 * time.Millisecond,
			},
		},
	}
	outPath := path.Join(t.TempDir(), "room_mix.ogg")
	err := w.runMergePipeline(
		context.Background(),
		map[string]string{"p1": "/tmp/p1.ogg"},
		alignment,
		types.AudioRecordingFormatOGGOpus,
		outPath,
		44100,
	)
	if err != nil {
		t.Fatalf("runMergePipeline() error = %v", err)
	}

	argsLog, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read gst log: %v", err)
	}
	logText := string(argsLog)
	if !strings.Contains(logText, "audio/x-raw,rate=48000,channels=1,format=S16LE") {
		t.Fatalf("expected adjusted opus sample rate in args, got: %s", logText)
	}
	if !strings.Contains(logText, "interleave name=interleave !") {
		t.Fatalf("expected interleave output chain in args, got: %s", logText)
	}
	if !strings.Contains(logText, "opusenc ! oggmux !") {
		t.Fatalf("expected opus/ogg encoder chain in args, got: %s", logText)
	}
	if _, err = os.Stat(outPath); err != nil {
		t.Fatalf("expected output file to be written: %v", err)
	}
}

func TestRunMergePipelineUsesAlignmentOrderForChannels(t *testing.T) {
	logPath := installFakeGstLaunch(t)

	w := &MergeWorker{}
	alignment := &AlignmentResult{
		Alignments: []*AlignmentInfo{
			{ParticipantID: "p2", Offset: 20 * time.Millisecond},
			{ParticipantID: "p1", Offset: 10 * time.Millisecond},
		},
	}
	outPath := path.Join(t.TempDir(), "room_mix.wav")
	err := w.runMergePipeline(
		context.Background(),
		map[string]string{
			"p1": "/tmp/p1.ogg",
			"p2": "/tmp/p2.ogg",
		},
		alignment,
		types.AudioRecordingFormatWAVPCM,
		outPath,
		32000,
	)
	if err != nil {
		t.Fatalf("runMergePipeline() error = %v", err)
	}

	argsLog, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read gst log: %v", err)
	}
	logText := string(argsLog)

	idxP2 := strings.Index(logText, "location=/tmp/p2.ogg")
	idxP1 := strings.Index(logText, "location=/tmp/p1.ogg")
	if idxP2 < 0 || idxP1 < 0 {
		t.Fatalf("expected both participant inputs in args, got: %s", logText)
	}
	if idxP2 >= idxP1 {
		t.Fatalf("expected p2 to appear before p1 following alignment order, got: %s", logText)
	}

	if !strings.Contains(logText, "audio/x-raw,rate=32000,channels=2,format=S16LE") {
		t.Fatalf("expected 2-channel output caps in args, got: %s", logText)
	}
	if !strings.Contains(logText, "interleave name=interleave !") {
		t.Fatalf("expected interleave output chain in args, got: %s", logText)
	}
	if _, err = os.Stat(outPath); err != nil {
		t.Fatalf("expected output file to be written: %v", err)
	}
}

func TestRunMergePipelineReturnsErrorOnCommandFailure(t *testing.T) {
	installFakeGstLaunch(t)
	t.Setenv("TEST_GST_FAIL", "1")

	w := &MergeWorker{}
	err := w.runMergePipeline(
		context.Background(),
		map[string]string{"p1": "/tmp/p1.ogg"},
		&AlignmentResult{},
		types.AudioRecordingFormatWAVPCM,
		path.Join(t.TempDir(), "room_mix.wav"),
		32000,
	)
	if err == nil {
		t.Fatal("expected runMergePipeline to fail")
	}
	if !strings.Contains(err.Error(), "gst-launch failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPrepareParticipantFilesForMergeGapFillsOgg(t *testing.T) {
	logPath := installFakeFfmpeg(t)

	tmpDir := t.TempDir()
	oggInput := path.Join(tmpDir, "p1.ogg")
	wavInput := path.Join(tmpDir, "p2.wav")
	if err := os.WriteFile(oggInput, []byte("ogg-content"), 0o644); err != nil {
		t.Fatalf("failed to write ogg input: %v", err)
	}
	if err := os.WriteFile(wavInput, []byte("wav-content"), 0o644); err != nil {
		t.Fatalf("failed to write wav input: %v", err)
	}

	w := &MergeWorker{}
	prepared := w.prepareParticipantFilesForMerge(
		context.Background(),
		map[string]string{
			"p1": oggInput,
			"p2": wavInput,
		},
		map[string]bool{
			"p1": true,
		},
		tmpDir,
		44100,
	)

	if prepared["p2"] != wavInput {
		t.Fatalf("expected non-ogg input to be unchanged, got %q", prepared["p2"])
	}

	filledPath := prepared["p1"]
	if filledPath == "" {
		t.Fatal("expected filled path for ogg input")
	}
	if filledPath == oggInput {
		t.Fatalf("expected ogg input to be gap-filled, got original path %q", filledPath)
	}
	if !strings.Contains(filledPath, path.Join(tmpDir, "gapfill")) {
		t.Fatalf("expected filled path to be in gapfill dir, got %q", filledPath)
	}
	if _, err := os.Stat(filledPath); err != nil {
		t.Fatalf("expected filled file to exist: %v", err)
	}

	argsLog, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read ffmpeg log: %v", err)
	}
	logText := string(argsLog)
	if !strings.Contains(logText, fmt.Sprintf("-i %s", oggInput)) {
		t.Fatalf("expected ffmpeg to process ogg input, got: %s", logText)
	}
	if !strings.Contains(logText, "aresample=async=1:first_pts=0") {
		t.Fatalf("expected async resample filter in ffmpeg args, got: %s", logText)
	}
	if strings.Contains(logText, wavInput) {
		t.Fatalf("did not expect ffmpeg to process wav input, got: %s", logText)
	}
}

func TestPrepareParticipantFilesForMergeGapFillsFormatWithNoExtension(t *testing.T) {
	logPath := installFakeFfmpeg(t)

	tmpDir := t.TempDir()
	oggInputNoExt := path.Join(tmpDir, "p1")
	if err := os.WriteFile(oggInputNoExt, []byte("ogg-content"), 0o644); err != nil {
		t.Fatalf("failed to write ogg input without extension: %v", err)
	}

	w := &MergeWorker{}
	prepared := w.prepareParticipantFilesForMerge(
		context.Background(),
		map[string]string{"p1": oggInputNoExt},
		map[string]bool{"p1": true},
		tmpDir,
		44100,
	)

	filledPath := prepared["p1"]
	if filledPath == "" {
		t.Fatal("expected filled path for ogg input without extension")
	}
	if filledPath == oggInputNoExt {
		t.Fatalf("expected input without extension to be gap-filled, got original path %q", filledPath)
	}

	argsLog, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read ffmpeg log: %v", err)
	}
	logText := string(argsLog)
	if !strings.Contains(logText, fmt.Sprintf("-i %s", oggInputNoExt)) {
		t.Fatalf("expected ffmpeg to process input without extension, got: %s", logText)
	}
}

func TestPrepareParticipantFilesForMergeFallsBackOnFfmpegFailure(t *testing.T) {
	installFakeFfmpeg(t)
	t.Setenv("TEST_FFMPEG_FAIL", "1")

	tmpDir := t.TempDir()
	oggInput := path.Join(tmpDir, "p1.ogg")
	if err := os.WriteFile(oggInput, []byte("ogg-content"), 0o644); err != nil {
		t.Fatalf("failed to write ogg input: %v", err)
	}

	w := &MergeWorker{}
	prepared := w.prepareParticipantFilesForMerge(
		context.Background(),
		map[string]string{"p1": oggInput},
		map[string]bool{"p1": true},
		tmpDir,
		44100,
	)

	if prepared["p1"] != oggInput {
		t.Fatalf("expected fallback to original path, got %q", prepared["p1"])
	}
}

func TestProcessJobLocalMergesAndUpdatesManifest(t *testing.T) {
	installFakeGstLaunch(t)

	tmpDir := t.TempDir()
	manifestDir := path.Join(tmpDir, "manifest")
	if err := os.MkdirAll(manifestDir, 0o755); err != nil {
		t.Fatalf("failed to create manifest dir: %v", err)
	}
	manifestPath := path.Join(manifestDir, "manifest.json")

	p1File := path.Join(tmpDir, "p1.ogg")
	p2File := path.Join(tmpDir, "p2.ogg")
	if err := os.WriteFile(p1File, []byte("p1"), 0o644); err != nil {
		t.Fatalf("failed to write p1 file: %v", err)
	}
	if err := os.WriteFile(p2File, []byte("p2"), 0o644); err != nil {
		t.Fatalf("failed to write p2 file: %v", err)
	}

	manifest := &config.AudioRecordingManifest{
		RoomName:   "room-e2e",
		SessionID:  "session-e2e",
		StartedAt:  time.Now().UnixNano(),
		SampleRate: 44100,
		Formats: []types.AudioRecordingFormat{
			types.AudioRecordingFormatOGGOpus,
			types.AudioRecordingFormatWAVPCM,
		},
		Participants: []*config.ParticipantRecordingInfo{
			{
				ParticipantID: "p1",
				JoinedAt:      time.Now().Add(-5 * time.Second).UnixNano(),
				Artifacts: []*config.AudioArtifact{
					{
						Format:     types.AudioRecordingFormatOGGOpus,
						Filename:   "p1.ogg",
						StorageURI: p1File,
						DurationMs: 1000,
					},
				},
			},
			{
				ParticipantID: "p2",
				JoinedAt:      time.Now().Add(-4 * time.Second).UnixNano(),
				Artifacts: []*config.AudioArtifact{
					{
						Format:     types.AudioRecordingFormatOGGOpus,
						Filename:   "p2.ogg",
						StorageURI: p2File,
						DurationMs: 1000,
					},
				},
			},
		},
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	if err = os.WriteFile(manifestPath, manifestData, 0o644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	w := &MergeWorker{
		config: &MergeWorkerConfig{
			WorkerID: "test-worker",
			TmpDir:   path.Join(tmpDir, "work"),
		},
	}
	job := &MergeJob{
		ID:           "job-1",
		ManifestPath: manifestPath,
		SessionID:    "session-e2e",
	}
	if err = w.processJob(context.Background(), job); err != nil {
		t.Fatalf("processJob() error = %v", err)
	}

	updatedData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed reading updated manifest: %v", err)
	}
	var updated config.AudioRecordingManifest
	if err = json.Unmarshal(updatedData, &updated); err != nil {
		t.Fatalf("failed to unmarshal updated manifest: %v", err)
	}
	if updated.RoomMix == nil {
		t.Fatal("expected room_mix in updated manifest")
	}
	if updated.RoomMix.Status != config.AudioRecordingStatusCompleted {
		t.Fatalf("room_mix.status = %q, want %q", updated.RoomMix.Status, config.AudioRecordingStatusCompleted)
	}
	if len(updated.RoomMix.Artifacts) != 2 {
		t.Fatalf("room_mix artifact count = %d, want 2", len(updated.RoomMix.Artifacts))
	}

	if _, err = os.Stat(path.Join(manifestDir, "room_mix_room-e2e.ogg")); err != nil {
		t.Fatalf("expected room_mix_room-e2e.ogg output: %v", err)
	}
	if _, err = os.Stat(path.Join(manifestDir, "room_mix_room-e2e.wav")); err != nil {
		t.Fatalf("expected room_mix_room-e2e.wav output: %v", err)
	}
}
