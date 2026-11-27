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

package sip

import (
	"strconv"
	"time"

	"github.com/livekit/sipgo/sip"

	"github.com/livekit/sip/pkg/stats"
)

const (
	unavailableShutdownReason = "Shutting down"
	unavailableOverloadReason = "Overloaded"
)

// unavailableReason says why the service refuses new calls.
type unavailableReason struct {
	reason     string            // reason phrase of the 503
	term       stats.Termination // how the refused INVITE is counted
	retryAfter time.Duration     // Retry-After of the 503, none when zero
}

// unavailable reports why the service must refuse new calls, or nil when it accepts them.
// A stopping service refuses new calls so that it can drain. An overloaded one refuses them
// to protect the calls it already has. In both cases the sender can retry on another server.
func (cm *callManager) unavailable() *unavailableReason {
	switch cm.admissionHealth() {
	case stats.HealthStopped:
		return &unavailableReason{
			reason:     unavailableShutdownReason,
			term:       stats.ServerError("shutting-down"),
			retryAfter: cm.conf.ServiceUnavailable.ShutdownRetryAfter,
		}
	case stats.HealthUnderLoad:
		return &unavailableReason{
			reason:     unavailableOverloadReason,
			term:       stats.ServerError("overloaded"),
			retryAfter: cm.conf.ServiceUnavailable.OverloadRetryAfter,
		}
	}
	return nil
}

// admissionHealth is the health that decides whether new calls are accepted.
func (cm *callManager) admissionHealth() stats.HealthStatus {
	if cm.health != nil {
		return cm.health()
	}
	h := cm.mon.Health()
	if h == stats.HealthUnderLoad && !cm.mon.CPUSampled() {
		// Right after start no CPU sample exists yet and the load reads as full.
		// Refusing calls then would turn every restart into a second of 503s.
		return stats.HealthOK
	}
	return h
}

// newUnavailableResponse builds the 503 for req, with a Retry-After header when one is set.
func newUnavailableResponse(req *sip.Request, u *unavailableReason) *sip.Response {
	r := newResponse(req, sip.StatusServiceUnavailable, u.reason, nil)
	if h := retryAfterHeader(u.retryAfter); h != nil {
		r.AppendHeader(h)
	}
	return r
}

// retryAfterHeader returns the Retry-After header for d, rounded up to whole seconds
// as RFC 3261 requires, or nil when d is not positive.
func retryAfterHeader(d time.Duration) sip.Header {
	if d <= 0 {
		return nil
	}
	sec := int64((d + time.Second - 1) / time.Second)
	return sip.NewHeader("Retry-After", strconv.FormatInt(sec, 10))
}

// isOutOfDialog reports whether req was sent outside of a dialog, that is without a To tag.
func isOutOfDialog(req *sip.Request) bool {
	to := req.To()
	return to == nil || to.Params.GetOr("tag", "") == ""
}
