package notification

const notificationConfigManifestType = "notificationProfile"

// ProfileConfig is a struct that represents the configuration for a single notification profile.
type ProfileConfig struct {
	Profile string     `json:"profile"`
	Method  MethodInfo `json:"method"`
	Events  []string   `json:"events"`
}
