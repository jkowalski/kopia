// Package notification provides notification functionality for Kopia.
package notification

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
)

var log = logging.Module("notification")

const profileNameKey = "profile"

// ListProfiles returns a list of notification profiles.
func ListProfiles(ctx context.Context, rep repo.Repository) ([]ProfileConfig, error) {
	profileMetadata, err := rep.FindManifests(ctx,
		map[string]string{
			manifest.TypeLabelKey: notificationConfigManifestType,
		})
	if err != nil {
		return nil, errors.Wrap(err, "unable to list notification profiles")
	}

	var profiles []ProfileConfig

	for _, m := range profileMetadata {
		var pc ProfileConfig
		if _, err := rep.GetManifest(ctx, m.ID, &pc); err != nil {
			return nil, errors.Wrap(err, "unable to get notification profile")
		}

		profiles = append(profiles, pc)
	}

	return profiles, nil
}

// GetProfile returns a notification profile by name.
func GetProfile(ctx context.Context, rep repo.Repository, name string) (ProfileConfig, bool, error) {
	entries, err := rep.FindManifests(ctx, labelsForProfileName(name))
	if err != nil {
		return ProfileConfig{}, false, errors.Wrap(err, "unable to list notification profiles")
	}

	if len(entries) == 0 {
		return ProfileConfig{}, false, nil
	}

	var pc ProfileConfig

	_, err = rep.GetManifest(ctx, manifest.PickLatestID(entries), &pc)

	return pc, true, errors.Wrap(err, "unable to get notification profile")
}

// SaveProfile saves a notification profile.
func SaveProfile(ctx context.Context, rep repo.RepositoryWriter, pc ProfileConfig) error {
	log(ctx).Debugf("saving notification profile %q with method %v", pc.Profile, pc.Method)

	_, err := rep.ReplaceManifests(ctx, labelsForProfileName(pc.Profile), &pc)
	if err != nil {
		return errors.Wrap(err, "unable to save notification profile")
	}

	return nil
}

// DeleteProfile deletes a notification profile.
func DeleteProfile(ctx context.Context, rep repo.RepositoryWriter, name string) error {
	entries, err := rep.FindManifests(ctx, labelsForProfileName(name))
	if err != nil {
		return errors.Wrap(err, "unable to list notification profiles")
	}

	for _, e := range entries {
		if err := rep.DeleteManifest(ctx, e.ID); err != nil {
			return errors.Wrapf(err, "unable to delete notification profile %q", e.ID)
		}
	}

	return nil
}

// SendTestNotification sends a test notification using the specified provider.
func SendTestNotification(ctx context.Context, prov Provider) error {
	log(ctx).Infof("Sending test notification using %v", prov.Summary())

	markdownBody := fmt.Sprintf(`This is a test notification from Kopia.

- Kopia Version: **%s**
- Build Info:    **%s**
- Github Repo:   **%s**

If you received this, your notification configuration is correct.`, repo.BuildVersion, repo.BuildInfo, repo.BuildGitHubRepo)

	//nolint:wrapcheck
	return prov.Send(ctx, Message{
		Subject:      "Test notification from Kopia at " + clock.Now().Format("2006-01-02 15:04:05"),
		MarkdownBody: markdownBody,
	})
}

func labelsForProfileName(name string) map[string]string {
	return map[string]string{
		manifest.TypeLabelKey: notificationConfigManifestType,
		profileNameKey:        name,
	}
}
