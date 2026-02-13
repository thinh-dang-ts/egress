package config

import "testing"

func TestAudioManifestUpdateParticipantJoinedAt(t *testing.T) {
	manifest := &AudioRecordingManifest{
		Participants: []*ParticipantRecordingInfo{
			{
				ParticipantID: "p1",
				JoinedAt:      100,
			},
		},
	}

	manifest.UpdateParticipantJoinedAt("p1", 200)

	p := manifest.GetParticipant("p1")
	if p == nil {
		t.Fatal("expected participant p1")
	}
	if p.JoinedAt != 200 {
		t.Fatalf("joined_at = %d, want %d", p.JoinedAt, 200)
	}
}
