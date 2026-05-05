package insecureserverbind

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRestrictionApplies(t *testing.T) {
	t.Parallel()

	cases := []struct {
		insecure, withoutPassword, allowDangerous bool
		want                                      bool
		name                                      string
	}{
		{true, true, false, true, "all_restriction_flags"},
		{true, true, true, false, "escape_hatch"},
		{true, false, false, false, "no_without_password"},
		{false, true, false, false, "no_insecure"},
		{false, false, false, false, "neither"},
		{false, false, true, false, "neither_plus_escape"},
		{true, false, true, false, "insecure_escape_no_nopass"},
		{false, true, true, false, "nopass_escape_no_insecure"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := RestrictionApplies(tc.insecure, tc.withoutPassword, tc.allowDangerous)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestValidateListenAddressIfRestricted(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateListenAddressIfRestricted(
		false, true, false, "http://0.0.0.0:0",
	), "when restriction does not apply, bad address is ignored")

	require.NoError(t, ValidateListenAddressIfRestricted(
		true, true, true, "http://0.0.0.0:0",
	), "escape hatch skips validation")

	err := ValidateListenAddressIfRestricted(true, true, false, "http://0.0.0.0:0")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDisallowedPublicBind)

	require.NoError(t, ValidateListenAddressIfRestricted(
		true, true, false, "http://127.0.0.1:0",
	))
}

func TestValidateListenerAddrIfRestricted(t *testing.T) {
	t.Parallel()

	pub := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 80}

	require.NoError(t, ValidateListenerAddrIfRestricted(false, true, false, pub))

	require.NoError(t, ValidateListenerAddrIfRestricted(true, true, true, pub))

	err := ValidateListenerAddrIfRestricted(true, true, false, pub)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDisallowedPublicBind)

	require.NoError(t, ValidateListenerAddrIfRestricted(
		true, true, false,
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1},
	))
}

func TestParseListenHost(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in        string
		wantHost  string
		wantUnix  bool
		wantError bool
	}{
		{"http://127.0.0.1:51515", "127.0.0.1", false, false},
		{"https://127.0.0.1:51515", "127.0.0.1", false, false},
		{"127.0.0.1:51515", "127.0.0.1", false, false},
		{"http://LOCALHOST:0", "LOCALHOST", false, false},
		{"http://[::1]:123", "::1", false, false},
		{"unix:/tmp/kopia.sock", "", true, false},
		{"http://unix:/wrong", "", true, false},
		{"http://:51515", "", false, false},
		{"http://0.0.0.0:0", "0.0.0.0", false, false},
		{"http://192.0.2.1:1", "192.0.2.1", false, false},
		{"http://example.com:80", "example.com", false, false},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			host, isUnix, err := ParseListenHost(tc.in)
			if tc.wantError {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantHost, host)
			require.Equal(t, tc.wantUnix, isUnix)
		})
	}
}

func TestValidateListenAddressFlag(t *testing.T) {
	t.Parallel()

	ok := []string{
		"http://127.0.0.1:51515",
		"http://localhost:0",
		"http://LOCALHOST:9999",
		"http://[::1]:123",
		"http://127.0.0.2:1",
		"unix:/tmp/foo.sock",
		"https://127.0.0.1:1",
	}

	for _, addr := range ok {
		t.Run(addr, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, ValidateListenAddressFlag(addr))
		})
	}

	bad := []string{
		"http://0.0.0.0:0",
		"http://:51515",
		"http://192.0.2.1:80",
		"http://example.com:80",
	}

	for _, addr := range bad {
		t.Run(addr, func(t *testing.T) {
			t.Parallel()

			err := ValidateListenAddressFlag(addr)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrDisallowedPublicBind)
			require.ErrorContains(t, err, AllowDangerousUnauthenticatedNetworkFlag)
		})
	}
}

func TestValidateListenerAddr(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateListenerAddr(&net.UnixAddr{Name: "/tmp/x", Net: "unix"}))

	require.NoError(t, ValidateListenerAddr(&net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 51515,
	}))

	require.NoError(t, ValidateListenerAddr(&net.TCPAddr{
		IP:   net.ParseIP("127.0.0.2"),
		Port: 1,
	}))

	require.NoError(t, ValidateListenerAddr(&net.TCPAddr{
		IP:   net.ParseIP("::1"),
		Port: 1,
	}))

	err := ValidateListenerAddr(&net.TCPAddr{
		IP:   net.ParseIP("192.0.2.1"),
		Port: 80,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDisallowedPublicBind)
	require.ErrorContains(t, err, AllowDangerousUnauthenticatedNetworkFlag)

	err = ValidateListenerAddr(&net.TCPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: 51515,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDisallowedPublicBind)

	err = ValidateListenerAddr(&net.TCPAddr{
		Port: 51515,
	})
	require.Error(t, err)
}

type stubAddr struct {
	network, s string
}

func (a stubAddr) Network() string { return a.network }
func (a stubAddr) String() string  { return a.s }

func TestValidateListenerAddr_unknownType(t *testing.T) {
	t.Parallel()

	err := ValidateListenerAddr(stubAddr{network: "tcp", s: "192.0.2.1:80"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDisallowedPublicBind)
}
