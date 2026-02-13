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

package test

// func TestMergeRecordingFromEtcDataProducesOutputInTmp(t *testing.T) {
// 	if _, err := exec.LookPath("gst-launch-1.0"); err != nil {
// 		t.Skipf("gst-launch-1.0 not found: %v", err)
// 	}
// 	ffmpegLogPath := configureRealFfmpegReport(t, "/usr/bin/ffmpeg")
// 	logger.InitFromConfig(&logger.Config{Level: "debug"}, "test")

// 	wd, err := os.Getwd()
// 	if err != nil {
// 		t.Fatalf("failed to get cwd: %v", err)
// 	}
// 	repoRoot := filepath.Dir(wd)
// 	dataDir := filepath.Join(repoRoot, "etc", "data")

// 	manifestPath := filepath.Join(dataDir, "manifest.json")
// 	manifestData, err := os.ReadFile(manifestPath)
// 	if err != nil {
// 		t.Fatalf("failed to read source manifest %s: %v", manifestPath, err)
// 	}

// 	var manifest config.AudioRecordingManifest
// 	if err = json.Unmarshal(manifestData, &manifest); err != nil {
// 		t.Fatalf("failed to unmarshal source manifest: %v", err)
// 	}

// 	if len(manifest.Formats) == 0 {
// 		manifest.Formats = []types.AudioRecordingFormat{types.AudioRecordingFormatOGGOpus}
// 	}
// 	manifest.RoomMix = nil
// 	manifest.Status = config.AudioRecordingStatusMerging

// 	extensionlessInputDir := t.TempDir()
// 	extensionlessInputs := make([]string, 0, len(manifest.Participants))

// 	for _, participant := range manifest.Participants {
// 		for artifactIndex, artifact := range participant.Artifacts {
// 			localPath, resolveErr := resolveArtifactInputPath(dataDir, artifact.StorageURI)
// 			if resolveErr != nil {
// 				t.Fatalf("failed to resolve artifact input path for participant %s: %v", participant.ParticipantID, resolveErr)
// 			}

// 			// Use extensionless input names to simulate storage keys like "file1"/"file2".
// 			noExtName := fmt.Sprintf("%s_%s_%d_input", participant.ParticipantID, artifact.Format, artifactIndex)
// 			noExtPath := filepath.Join(extensionlessInputDir, noExtName)
// 			if copyErr := copyFile(localPath, noExtPath); copyErr != nil {
// 				t.Fatalf("failed to copy artifact input for participant %s: %v", participant.ParticipantID, copyErr)
// 			}

// 			artifact.StorageURI = noExtPath
// 			extensionlessInputs = append(extensionlessInputs, noExtPath)
// 		}
// 	}

// 	outputDir := filepath.Join(repoRoot, "tmp", "merge-recording-test")
// 	if err = os.MkdirAll(outputDir, 0o755); err != nil {
// 		t.Fatalf("failed to create output dir %s: %v", outputDir, err)
// 	}

// 	preparedManifestPath := filepath.Join(outputDir, "manifest.json")
// 	preparedManifestData, err := manifest.ToJSON()
// 	if err != nil {
// 		t.Fatalf("failed to marshal prepared manifest: %v", err)
// 	}
// 	if err = os.WriteFile(preparedManifestPath, preparedManifestData, 0o644); err != nil {
// 		t.Fatalf("failed to write prepared manifest: %v", err)
// 	}

// 	job := &merge.MergeJob{
// 		ID:           fmt.Sprintf("merge-recording-test-%d", time.Now().UnixNano()),
// 		ManifestPath: preparedManifestPath,
// 		SessionID:    manifest.SessionID,
// 	}

// 	cfg := &merge.MergeWorkerConfig{
// 		WorkerID: "merge-recording-test",
// 		TmpDir:   filepath.Join(repoRoot, "tmp"),
// 	}

// 	if err = merge.ProcessMergeJob(context.Background(), cfg, nil, job); err != nil {
// 		t.Fatalf("merge job failed: %v", err)
// 	}

// 	updatedManifestData, err := os.ReadFile(preparedManifestPath)
// 	if err != nil {
// 		t.Fatalf("failed to read updated manifest: %v", err)
// 	}

// 	var updatedManifest config.AudioRecordingManifest
// 	if err = json.Unmarshal(updatedManifestData, &updatedManifest); err != nil {
// 		t.Fatalf("failed to unmarshal updated manifest: %v", err)
// 	}
// 	if updatedManifest.RoomMix == nil {
// 		t.Fatal("updated manifest has no room_mix")
// 	}
// 	if len(updatedManifest.RoomMix.Artifacts) == 0 {
// 		t.Fatal("updated manifest has no room_mix artifacts")
// 	}

// 	for _, artifact := range updatedManifest.RoomMix.Artifacts {
// 		if _, err = os.Stat(artifact.StorageURI); err != nil {
// 			t.Fatalf("merged artifact not found at %s: %v", artifact.StorageURI, err)
// 		}
// 		t.Logf("merged output ready: %s (%s)", artifact.StorageURI, artifact.Format)
// 	}

// 	// Assert the merge pre-processing path executed RTP-gap filling through ffmpeg.
// 	ffmpegArgs, err := os.ReadFile(ffmpegLogPath)
// 	if err != nil {
// 		t.Fatalf("failed to read ffmpeg args log: %v", err)
// 	}
// 	if !strings.Contains(string(ffmpegArgs), "aresample=async=1:first_pts=0") {
// 		t.Fatalf("expected ffmpeg gap-fill filter to be used, got: %s", string(ffmpegArgs))
// 	}
// 	for _, inPath := range extensionlessInputs {
// 		if !strings.Contains(string(ffmpegArgs), fmt.Sprintf("-i %s", inPath)) {
// 			t.Fatalf("expected ffmpeg to process extensionless input %s, got: %s", inPath, string(ffmpegArgs))
// 		}
// 	}
// }

// func resolveArtifactInputPath(dataDir, storageURI string) (string, error) {
// 	candidates := []string{
// 		storageURI,
// 		filepath.Join(dataDir, storageURI),
// 		filepath.Join(dataDir, storageURI+".ogg"),
// 		filepath.Join(dataDir, filepath.Base(storageURI)),
// 		filepath.Join(dataDir, filepath.Base(storageURI)+".ogg"),
// 	}

// 	for _, candidate := range candidates {
// 		if candidate == "" {
// 			continue
// 		}
// 		if _, err := os.Stat(candidate); err == nil {
// 			return candidate, nil
// 		}
// 	}

// 	return "", fmt.Errorf("no local file found for storage_uri=%q in %s", storageURI, dataDir)
// }

// func copyFile(src, dst string) error {
// 	data, err := os.ReadFile(src)
// 	if err != nil {
// 		return err
// 	}
// 	return os.WriteFile(dst, data, 0o644)
// }

// func configureRealFfmpegReport(t *testing.T, ffmpegPath string) string {
// 	t.Helper()

// 	if _, err := os.Stat(ffmpegPath); err != nil {
// 		lookedUpPath, lookupErr := exec.LookPath("ffmpeg")
// 		if lookupErr != nil {
// 			t.Skipf("ffmpeg not found at %s and not in PATH: %v", ffmpegPath, err)
// 		}
// 		ffmpegPath = lookedUpPath
// 	}

// 	binDir := t.TempDir()
// 	binPath := filepath.Join(binDir, "ffmpeg")
// 	logPath := filepath.Join(t.TempDir(), "ffmpeg_args.log")

// 	script := fmt.Sprintf(`#!/bin/sh
// set -eu
// if [ -n "${TEST_FFMPEG_LOG:-}" ]; then
//   ts="$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)"
//   printf '%%s\t%%s\t%%s\t%%s\n' "$ts" "INFO" "ffmpeg-wrapper" "$*" >> "$TEST_FFMPEG_LOG"
// fi
// exec %s "$@"
// `, ffmpegPath)
// 	if err := os.WriteFile(binPath, []byte(script), 0o755); err != nil {
// 		t.Fatalf("failed to write ffmpeg wrapper: %v", err)
// 	}

// 	t.Setenv("TEST_FFMPEG_LOG", logPath)
// 	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
// 	t.Logf("using ffmpeg binary: %s", ffmpegPath)
// 	t.Logf("ffmpeg args log path: %s", logPath)

// 	return logPath
// }
