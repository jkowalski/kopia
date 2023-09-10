package osservice

// InstallOptions provides options for installing the service.
type InstallOptions struct {
	Username        string // empty==install as administrator ("root" or "LOCAL SYSTEM")
	DisplayName     string
	Description     string
	AutoStart       bool
	Reinstall       bool
	GetPasswordFunc func() (string, error)
}
