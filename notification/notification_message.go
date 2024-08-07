package notification

// Message represents a notification message.
type Message struct {
	Subject      string `json:"subject"`
	MarkdownBody string `json:"body"`
}
