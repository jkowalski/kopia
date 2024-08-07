// Package email provides email notification support.
package email

import (
	"context"
	"fmt"
	"net/smtp"
	"strings"

	"github.com/gomarkdown/markdown"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification"
)

// ProviderType defines the type of the email notification provider.
const ProviderType = "email"

const defaultSMTPPort = 587

type emailProvider struct {
	opt Options
}

func (p *emailProvider) Send(ctx context.Context, msg notification.Message) error {
	var auth smtp.Auth

	if p.opt.SMTPUsername != "" {
		auth = smtp.PlainAuth(p.opt.SMTPIdentity, p.opt.SMTPUsername, p.opt.SMTPPassword, p.opt.SMTPServer)
	}

	htmlBody := markdown.ToHTML([]byte(msg.MarkdownBody), nil, nil)

	//nolint:wrapcheck
	return smtp.SendMail(
		fmt.Sprintf("%v:%d", p.opt.SMTPServer, p.opt.SMTPPort),
		auth,
		p.opt.From,
		strings.Split(p.opt.To, ","), []byte("Subject: "+msg.Subject+"\r\n"+"MIME-version: 1.0;\r\nContent-Type: text/html; charset=\"UTF-8\";\r\n\r\n"+string(htmlBody)))
}

func (p *emailProvider) Summary() string {
	return fmt.Sprintf("SMTP server: %q, Mail from: %q Mail to: %q", p.opt.SMTPServer, p.opt.From, p.opt.To)
}

func init() {
	notification.RegisterProvider(ProviderType, func(ctx context.Context, options *Options) (notification.Provider, error) {
		if err := options.applyDefaultsAndValidate(); err != nil {
			return nil, errors.Wrap(err, "invalid notification configuration")
		}

		return &emailProvider{
			opt: *options,
		}, nil
	})
}
