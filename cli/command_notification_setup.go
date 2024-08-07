package cli

type commandNotificationConfigure struct {
	configEmail    commandNotificationConfigureEmail
	configPushover commandNotificationConfigurePushover
}

func (c *commandNotificationConfigure) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("setup", "Setup notifications").Alias("configure")
	c.configEmail.setup(svc, cmd)
	c.configPushover.setup(svc, cmd)
}
