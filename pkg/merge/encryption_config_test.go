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

package merge

import (
	"testing"

	"github.com/livekit/egress/pkg/config"
)

func TestCloneEncryptionConfigNil(t *testing.T) {
	if got := cloneEncryptionConfig(nil); got != nil {
		t.Fatalf("cloneEncryptionConfig(nil) = %#v, want nil", got)
	}
}

func TestCloneEncryptionConfigReturnsCopy(t *testing.T) {
	in := &config.EncryptionConfig{
		Mode:      config.EncryptionModeAES,
		MasterKey: "master-1",
	}

	out := cloneEncryptionConfig(in)
	if out == nil {
		t.Fatal("expected non-nil clone")
	}
	if out == in {
		t.Fatal("expected clone to be a different pointer")
	}
	if out.Mode != in.Mode || out.MasterKey != in.MasterKey {
		t.Fatalf("unexpected clone content: got %#v, want %#v", out, in)
	}

	out.MasterKey = "changed"
	if in.MasterKey != "master-1" {
		t.Fatalf("mutating clone should not affect input, input=%#v", in)
	}
}
