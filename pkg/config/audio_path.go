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

package config

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/livekit/egress/pkg/types"
)

// AudioPathBuilder builds storage paths for audio recordings
type AudioPathBuilder struct {
	RoomName          string
	RoomID            string
	SessionID         string
	Environment       string
	PathTemplate      string
	ManifestFilename  string
}

// DefaultPathTemplate is the default path template for audio recordings
const DefaultPathTemplate = "{room_name}/{session_id}"

// DefaultManifestFilename is the default manifest filename
const DefaultManifestFilename = "manifest.json"

// NewAudioPathBuilder creates a new AudioPathBuilder
func NewAudioPathBuilder(roomName, roomID, sessionID string) *AudioPathBuilder {
	return &AudioPathBuilder{
		RoomName:         roomName,
		RoomID:           roomID,
		SessionID:        sessionID,
		PathTemplate:     DefaultPathTemplate,
		ManifestFilename: DefaultManifestFilename,
	}
}

// WithEnvironment sets the environment prefix
func (b *AudioPathBuilder) WithEnvironment(env string) *AudioPathBuilder {
	b.Environment = env
	return b
}

// WithPathTemplate sets a custom path template
func (b *AudioPathBuilder) WithPathTemplate(template string) *AudioPathBuilder {
	if template != "" {
		b.PathTemplate = template
	}
	return b
}

// WithManifestFilename sets a custom manifest filename
func (b *AudioPathBuilder) WithManifestFilename(filename string) *AudioPathBuilder {
	if filename != "" {
		b.ManifestFilename = filename
	}
	return b
}

// BuildBasePath builds the base storage path using the template
func (b *AudioPathBuilder) BuildBasePath() string {
	timestamp := time.Now()
	utc := fmt.Sprintf("%s%03d", timestamp.Format("20060102150405"), timestamp.UnixMilli()%1000)

	replacements := map[string]string{
		"{env}":        b.Environment,
		"{room_name}":  sanitizePath(b.RoomName),
		"{room_id}":    sanitizePath(b.RoomID),
		"{session_id}": sanitizePath(b.SessionID),
		"{time}":       timestamp.Format("2006-01-02T150405"),
		"{utc}":        utc,
		"{date}":       timestamp.Format("2006-01-02"),
		"{year}":       timestamp.Format("2006"),
		"{month}":      timestamp.Format("01"),
		"{day}":        timestamp.Format("02"),
	}

	result := b.PathTemplate
	for template, value := range replacements {
		result = strings.Replace(result, template, value, -1)
	}

	// Clean up any double slashes or leading slashes
	result = path.Clean(result)
	if strings.HasPrefix(result, "/") {
		result = result[1:]
	}

	return result
}

// BuildParticipantPath builds the path for a participant's recording
func (b *AudioPathBuilder) BuildParticipantPath(participantID, participantIdentity string, format types.AudioRecordingFormat) string {
	basePath := b.BuildBasePath()
	timestamp := time.Now().Format("20060102T150405")
	ext := GetFileExtensionForFormat(format)

	// Sanitize participant identity for use in filename
	safeIdentity := sanitizeFilename(participantIdentity)
	safeID := sanitizeFilename(participantID)

	filename := fmt.Sprintf("%s_%s_%s%s", safeIdentity, safeID, timestamp, ext)
	return path.Join(basePath, "participants", filename)
}

// BuildMixedAudioPath builds the path for the mixed room audio
func (b *AudioPathBuilder) BuildMixedAudioPath(format types.AudioRecordingFormat) string {
	basePath := b.BuildBasePath()
	timestamp := time.Now().Format("20060102T150405")
	ext := GetFileExtensionForFormat(format)

	filename := fmt.Sprintf("room_mix_%s%s", timestamp, ext)
	return path.Join(basePath, filename)
}

// BuildManifestPath builds the path for the manifest file
func (b *AudioPathBuilder) BuildManifestPath() string {
	basePath := b.BuildBasePath()
	return path.Join(basePath, b.ManifestFilename)
}

// BuildTempFilePath builds a temporary file path for local processing
func (b *AudioPathBuilder) BuildTempFilePath(tmpDir, participantID string, format types.AudioRecordingFormat) string {
	ext := GetFileExtensionForFormat(format)
	timestamp := time.Now().Format("20060102T150405")
	filename := fmt.Sprintf("%s_%s%s", participantID, timestamp, ext)
	return path.Join(tmpDir, filename)
}

// sanitizePath removes or replaces characters that are unsafe for storage paths
func sanitizePath(s string) string {
	// Replace common problematic characters
	replacer := strings.NewReplacer(
		" ", "_",
		":", "_",
		"\\", "_",
		"<", "_",
		">", "_",
		"|", "_",
		"\"", "_",
		"?", "_",
		"*", "_",
	)
	return replacer.Replace(s)
}

// sanitizeFilename creates a safe filename from a string
func sanitizeFilename(s string) string {
	// First sanitize as a path
	s = sanitizePath(s)

	// Then remove any remaining slashes
	s = strings.ReplaceAll(s, "/", "_")

	// Remove any non-alphanumeric characters except underscore, dash, and dot
	reg := regexp.MustCompile(`[^a-zA-Z0-9_\-\.]`)
	s = reg.ReplaceAllString(s, "_")

	// Collapse multiple underscores
	reg = regexp.MustCompile(`_+`)
	s = reg.ReplaceAllString(s, "_")

	// Trim leading/trailing underscores
	s = strings.Trim(s, "_")

	// Ensure non-empty
	if s == "" {
		s = "unknown"
	}

	return s
}

// ParsePathTemplate validates a path template
func ParsePathTemplate(template string) error {
	if template == "" {
		return nil
	}

	// Check for valid placeholders
	validPlaceholders := map[string]bool{
		"{env}":        true,
		"{room_name}":  true,
		"{room_id}":    true,
		"{session_id}": true,
		"{time}":       true,
		"{utc}":        true,
		"{date}":       true,
		"{year}":       true,
		"{month}":      true,
		"{day}":        true,
	}

	// Find all placeholders in template
	reg := regexp.MustCompile(`\{[^}]+\}`)
	matches := reg.FindAllString(template, -1)

	for _, match := range matches {
		if !validPlaceholders[match] {
			return fmt.Errorf("invalid placeholder in path template: %s", match)
		}
	}

	return nil
}

// ExpandPathTemplate expands a path template with provided values
func ExpandPathTemplate(template string, values map[string]string) string {
	if template == "" {
		return ""
	}

	result := template
	for placeholder, value := range values {
		result = strings.Replace(result, placeholder, sanitizePath(value), -1)
	}

	return path.Clean(result)
}
