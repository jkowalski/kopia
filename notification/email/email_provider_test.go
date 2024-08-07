package email_test

import (
	"strings"
	"testing"

	smtpmock "github.com/mocktools/go-smtp-mock/v2"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/email"
)

func TestEmailProvider(t *testing.T) {
	ctx := testlogging.Context(t)

	srv := smtpmock.New(smtpmock.ConfigurationAttr{
		LogServerActivity: true,
		LogToStdout:       true,
	})
	require.NoError(t, srv.Start())
	defer srv.Stop()

	p, err := notification.GetProvider(ctx, "email", &email.Options{
		SMTPServer: "localhost",
		SMTPPort:   srv.PortNumber(),
		From:       "some-user@example.com",
		To:         "another-user@example.com",
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

	require.Len(t, srv.Messages(), 1)
	msg := srv.Messages()[0]

	lines := strings.Join([]string{
		"Subject: Test",
		"MIME-version: 1.0;",
		"Content-Type: text/html; charset=\"UTF-8\";",
		"",
		"<p>This is a test.</p>",
		"",
		"<ul>",
		"<li>one</li>",
		"<li>two</li>",
		"<li>three</li>",
		"</ul>",
		"",
		"<h1>Header</h1>",
		"",
		"<h2>Subheader</h2>",
		"",
		"<ul>",
		"<li>a</li>",
		"<li>b</li>",
		"<li>c</li>",
		"</ul>",
		"",
	}, "\r\n")

	require.Equal(t, lines, msg.MsgRequest())
}
