package cli

type commandNotification struct {
	config commandNotificationConfigure
	list   commandNotificationList
	delete commandNotificationDelete
	test   commandNotificationSendTestMessage
}

func (c *commandNotification) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("notifications", "Notifications.").Alias("notification")

	c.config.setup(svc, cmd)
	c.delete.setup(svc, cmd)
	c.test.setup(svc, cmd)
	c.list.setup(svc, cmd)
}
