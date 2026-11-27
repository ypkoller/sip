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
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/icholy/digest"
	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/rpc"
	"github.com/livekit/sipgo/sip"

	"github.com/livekit/sip/pkg/config"
	"github.com/livekit/sip/pkg/stats"
)

func TestRetryAfterHeader(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string // "" means no header
	}{
		{0, ""},
		{-time.Second, ""},
		{time.Millisecond, "1"},
		{time.Second, "1"},
		{1500 * time.Millisecond, "2"},
		{60 * time.Second, "60"},
	} {
		h := retryAfterHeader(tc.d)
		if tc.want == "" {
			require.Nil(t, h, "%v", tc.d)
			continue
		}
		require.NotNil(t, h, "%v", tc.d)
		require.Equal(t, "Retry-After", h.Name())
		require.Equal(t, tc.want, h.Value(), "%v", tc.d)
	}
}

func TestUnavailable(t *testing.T) {
	var health stats.HealthStatus
	cm := &callManager{
		conf: &config.Config{ServiceUnavailable: config.ServiceUnavailableConfig{
			OverloadRetryAfter: 10 * time.Second,
			ShutdownRetryAfter: 60 * time.Second,
		}},
		health: func() stats.HealthStatus { return health },
	}
	for _, h := range []stats.HealthStatus{stats.HealthOK, stats.HealthNotStarted, stats.HealthDisabled} {
		health = h
		require.Nil(t, cm.unavailable(), h.String())
	}

	health = stats.HealthStopped
	u := cm.unavailable()
	require.NotNil(t, u)
	require.Equal(t, unavailableShutdownReason, u.reason)
	require.Equal(t, stats.ServerError("shutting-down"), u.term)
	require.Equal(t, 60*time.Second, u.retryAfter)

	health = stats.HealthUnderLoad
	u = cm.unavailable()
	require.NotNil(t, u)
	require.Equal(t, unavailableOverloadReason, u.reason)
	require.Equal(t, stats.ServerError("overloaded"), u.term)
	require.Equal(t, 10*time.Second, u.retryAfter)
}

// TestUnavailableBeforeFirstCPUSample checks that a service that just started accepts calls.
// Before the first CPU sample Monitor.Health reports HealthUnderLoad, which is not a real overload.
func TestUnavailableBeforeFirstCPUSample(t *testing.T) {
	conf := &config.Config{NodeID: "test-node", MaxCpuUtilization: 0.9}
	mon, err := stats.NewMonitor(conf)
	require.NoError(t, err)
	require.NoError(t, mon.Start(conf))
	t.Cleanup(mon.Stop)
	cm := &callManager{conf: conf, mon: mon}

	if !mon.CPUSampled() {
		require.Equal(t, stats.HealthUnderLoad, mon.Health(), "no sample yet, the idle CPU reads as zero")
		require.Nil(t, cm.unavailable(), "an unknown load must not refuse calls")
	}
	require.Eventually(t, mon.CPUSampled, 5*time.Second, 50*time.Millisecond)

	mon.Shutdown()
	require.NotNil(t, cm.unavailable(), "shutdown refuses calls with or without a CPU sample")
}

// TestInboundUnavailable checks that an overloaded or stopping service refuses new
// calls with a 503 and the configured Retry-After, and keeps serving calls in progress.
func TestInboundUnavailable(t *testing.T) {
	const (
		authPrefix = "auth-" // the test handler asks for credentials for these called users
		username   = "user"
		password   = "secret"
	)
	cases := []struct {
		name       string
		reason     string
		term       string
		retryAfter string
		// enter puts the service in the unavailable state, leave (if set) takes it back.
		enter func(st *serviceTest, health *atomic.Int64)
		leave func(st *serviceTest, health *atomic.Int64)
	}{
		{
			name:       "shutdown",
			reason:     unavailableShutdownReason,
			term:       "shutting-down",
			retryAfter: "60",
			enter: func(st *serviceTest, _ *atomic.Int64) {
				// The real path: Service.Stop calls Monitor.Shutdown on SIGTERM.
				st.Manager.mon.Shutdown()
			},
		},
		{
			name:       "overload",
			reason:     unavailableOverloadReason,
			term:       "overloaded",
			retryAfter: "10",
			enter: func(st *serviceTest, health *atomic.Int64) {
				health.Store(int64(stats.HealthUnderLoad))
			},
			leave: func(st *serviceTest, health *atomic.Int64) {
				health.Store(int64(stats.HealthOK))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := NewServiceTest(t, nil)
			st.Manager.conf.ServiceUnavailable = config.ServiceUnavailableConfig{
				OverloadRetryAfter: 10 * time.Second,
				ShutdownRetryAfter: 60 * time.Second,
			}
			var health atomic.Int64
			if tc.leave != nil {
				// CPU load can't be forced in a test, so the overload case overrides the health.
				health.Store(int64(stats.HealthOK))
				st.Manager.health = func() stats.HealthStatus { return stats.HealthStatus(health.Load()) }
			}
			st.Handler.(*TestHandler).GetAuthCredentialsFunc = func(_ context.Context, call *rpc.SIPCall) (AuthInfo, error) {
				if call.To != nil && strings.HasPrefix(call.To.User, authPrefix) {
					return AuthInfo{Result: AuthPassword, Auth: InboundAuth{Username: username, Password: password}}, nil
				}
				return AuthInfo{Result: AuthAccept}, nil
			}

			invite := func(t *testing.T, call *sipUADialogTest, headers ...sip.Header) (*sip.Request, *sip.Response) {
				t.Helper()
				req, offer, err := call.Invite(call.localSDP)
				require.NoError(t, err)
				call.SetLocalSDP(offer)
				for _, h := range headers {
					req.AppendHeader(h)
				}
				return req, st.TestUA.TransactionRequest(t, req, true)
			}
			established := func(t *testing.T, call *sipUADialogTest, resp *sip.Response) {
				t.Helper()
				require.Equal(t, sip.StatusOK, resp.StatusCode)
				tag, ok := resp.To().Params.Get("tag")
				require.True(t, ok, "200 OK must carry a To tag")
				call.SetRemoteTag(LocalTag(tag))
				call.SetRouteSet(resp, true)
				t.Cleanup(func() {
					st.TestUA.TransactionRequest(t, call.NewRequest(sip.BYE), true)
				})
			}
			requireUnavailable := func(t *testing.T, resp *sip.Response) {
				t.Helper()
				require.Equal(t, sip.StatusServiceUnavailable, resp.StatusCode)
				require.Equal(t, tc.reason, resp.Reason)
				h := resp.GetHeader("Retry-After")
				require.NotNil(t, h, "503 must carry Retry-After")
				require.Equal(t, tc.retryAfter, h.Value())
			}
			options := func(t *testing.T, req *sip.Request) *sip.Response {
				t.Helper()
				return st.TestUA.TransactionRequest(t, req, true)
			}

			// Calls in progress, set up while the service accepts calls.
			inCall, _ := st.CreateInboundCall(t)
			outCall, _, _ := st.CreateOutboundCall(t)

			// A call on a password trunk that got its 407 challenge before the state change.
			challenged := newTestCall(st.TestUA, false)
			challenged.remoteUser = authPrefix + challenged.remoteUser
			challengeReq, challengeResp := invite(t, challenged)
			require.Equal(t, sip.StatusProxyAuthRequired, challengeResp.StatusCode)
			chalHdr := challengeResp.GetHeader("Proxy-Authenticate")
			require.NotNil(t, chalHdr)
			chal, err := digest.ParseChallenge(chalHdr.Value())
			require.NoError(t, err)
			cred, err := digest.Digest(chal, digest.Options{
				Method:   "INVITE",
				URI:      challengeReq.Recipient.String(),
				Username: username,
				Password: password,
			})
			require.NoError(t, err)

			reasonLabels := map[string]string{"dir": "in", "reason": tc.term}
			refusedBefore := gatherCounter(t, "livekit_sip_invite_error", reasonLabels)

			tc.enter(st, &health)

			// A new INVITE is refused.
			refused := newTestCall(st.TestUA, false)
			_, resp := invite(t, refused)
			requireUnavailable(t, resp)

			// A new INVITE on a password trunk gets a 503, not a new challenge.
			newAuth := newTestCall(st.TestUA, false)
			newAuth.remoteUser = authPrefix + newAuth.remoteUser
			_, resp = invite(t, newAuth)
			requireUnavailable(t, resp)

			// The challenged call: a retry without credentials would start over, so it is refused.
			_, resp = invite(t, challenged)
			requireUnavailable(t, resp)
			// The retry with credentials answers the challenge that was sent, so the call goes on.
			_, resp = invite(t, challenged, sip.NewHeader("Proxy-Authorization", cred.String()))
			established(t, challenged, resp)

			// Re-INVITEs of calls in progress are accepted.
			req, _, err := inCall.Invite(inCall.localSDP)
			require.NoError(t, err)
			require.Equal(t, sip.StatusOK, st.TestUA.TransactionRequest(t, req, true).StatusCode, "inbound re-INVITE")
			req, _, err = outCall.Invite(outCall.localSDP)
			require.NoError(t, err)
			require.Equal(t, sip.StatusOK, st.TestUA.TransactionRequest(t, req, false).StatusCode, "outbound re-INVITE")

			// An out-of-dialog OPTIONS probes for new calls and is refused.
			probe := newTestCall(st.TestUA, false)
			requireUnavailable(t, options(t, probe.NewRequest(sip.OPTIONS)))
			// An in-dialog OPTIONS keeps a call in progress alive and is answered.
			require.Equal(t, sip.StatusOK, options(t, inCall.NewRequest(sip.OPTIONS)).StatusCode)

			require.Equal(t, refusedBefore+3, gatherCounter(t, "livekit_sip_invite_error", reasonLabels),
				"each refused INVITE is counted under its reason")

			if tc.leave == nil {
				return
			}
			tc.leave(st, &health)

			// The 503 was not cached: a retry of the refused dialog is accepted now.
			_, resp = invite(t, refused)
			established(t, refused, resp)
			require.Equal(t, sip.StatusOK, options(t, probe.NewRequest(sip.OPTIONS)).StatusCode)
		})
	}
}

// TestInboundUnavailableNoRetryAfter checks that a disabled retry time sends the 503 without Retry-After.
// Config.Init maps a negative configured value to zero, see TestServiceUnavailableConfig.
func TestInboundUnavailableNoRetryAfter(t *testing.T) {
	st := NewServiceTest(t, nil)
	st.Manager.conf.ServiceUnavailable = config.ServiceUnavailableConfig{OverloadRetryAfter: 0}
	st.Manager.health = func() stats.HealthStatus { return stats.HealthUnderLoad }

	call := newTestCall(st.TestUA, false)
	req, _, err := call.Invite(nil)
	require.NoError(t, err)
	resp := st.TestUA.TransactionRequest(t, req, true)
	require.Equal(t, sip.StatusServiceUnavailable, resp.StatusCode)
	require.Nil(t, resp.GetHeader("Retry-After"))

	resp = st.TestUA.TransactionRequest(t, newTestCall(st.TestUA, false).NewRequest(sip.OPTIONS), true)
	require.Equal(t, sip.StatusServiceUnavailable, resp.StatusCode)
	require.Nil(t, resp.GetHeader("Retry-After"))
}
