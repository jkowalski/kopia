package pushover_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/pushover"
)

func TestPushover(t *testing.T) {
	ctx := testlogging.Context(t)

	mux := http.NewServeMux()

	var requests []*http.Request
	var requestBodies []bytes.Buffer

	mux.HandleFunc("/some-path", func(w http.ResponseWriter, r *http.Request) {
		var b bytes.Buffer
		io.Copy(&b, r.Body)

		requestBodies = append(requestBodies, b)
		requests = append(requests, r)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	p, err := notification.GetProvider(ctx, "pushover", &pushover.Options{
		AppToken: "app-token1",
		UserKey:  "user-key1",
		Endpoint: server.URL + "/some-path",
	})
	require.NoError(t, err)

	require.NoError(t, p.Send(ctx, notification.Message{Subject: "Test", MarkdownBody: `
This is a test.

* one
* two
* three

# Header
## Subheader

- a
- b
- c`}))

	require.Len(t, requests, 1)
	require.Equal(t, "application/json", requests[0].Header.Get("Content-Type"))

	var body map[string]interface{}

	require.NoError(t, json.NewDecoder(&requestBodies[0]).Decode(&body))

	require.Equal(t, "app-token1", body["token"])
	require.Equal(t, "user-key1", body["user"])
	require.Equal(t, "1", body["html"])
	require.Equal(t, "Test\n\n<p>This is a test.</p>\n\n<ul>\n<li>one</li>\n<li>two</li>\n<li>three</li>\n</ul>\n\n<h1>Header</h1>\n\n<h2>Subheader</h2>\n\n<ul>\n<li>a</li>\n<li>b</li>\n<li>c</li>\n</ul>\n", body["message"])
}
