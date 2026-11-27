// Copyright 2026 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServiceUnavailableConfig(t *testing.T) {
	for _, tc := range []struct {
		name         string
		yaml         string
		wantOverload time.Duration
		wantShutdown time.Duration
	}{
		{
			name:         "defaults",
			yaml:         "redis: {address: localhost:6379}\n",
			wantOverload: DefaultOverloadRetryAfter,
			wantShutdown: DefaultShutdownRetryAfter,
		},
		{
			name: "configured",
			yaml: "redis: {address: localhost:6379}\n" +
				"service_unavailable:\n  overload_retry_after: 5s\n  shutdown_retry_after: 2m\n",
			wantOverload: 5 * time.Second,
			wantShutdown: 2 * time.Minute,
		},
		{
			name: "disabled",
			yaml: "redis: {address: localhost:6379}\n" +
				"service_unavailable:\n  overload_retry_after: -1s\n",
			wantOverload: 0, // no Retry-After header
			wantShutdown: DefaultShutdownRetryAfter,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := NewConfig(tc.yaml)
			require.NoError(t, err)
			require.NoError(t, conf.Init())
			require.Equal(t, tc.wantOverload, conf.ServiceUnavailable.OverloadRetryAfter)
			require.Equal(t, tc.wantShutdown, conf.ServiceUnavailable.ShutdownRetryAfter)
		})
	}
}
