package cli

import (
	"bytes"
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandLogsShow struct {
	logSessionIDs []string

	crit logSelectionCriteria
	out  textOutput
}

func (c *commandLogsShow) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("show", "Show contents of the log.").Alias("cat")

	cmd.Arg("session-id", "Log Session ID to show").StringsVar(&c.logSessionIDs)

	cmd.Action(svc.directRepositoryReadAction(c.run))

	c.crit.setup(cmd)
	c.out.setup(svc)
}

func (c *commandLogsShow) run(ctx context.Context, rep repo.DirectRepository) error {
	sessions, err := getLogSessions(ctx, rep.BlobReader())
	if err != nil {
		return err
	}

	sessions = c.crit.filterLogSessions(sessions)

	if len(sessions) == 0 {
		return errors.Errorf("no logs found")
	}

	if len(c.logSessionIDs) > 0 {
		sessions = filterLogSessions(sessions, func(l *logSessionInfo) bool {
			for _, sid := range c.logSessionIDs {
				if l.id == sid {
					return true
				}
			}

			return false
		})
	}

	// by default show latest one
	if !c.crit.any() {
		sessions = sessions[len(sessions)-1:]
		log(ctx).Infof("Showing latest log (%v)", formatTimestamp(sessions[0].startTime))
	}

	for _, s := range sessions {
		for _, bm := range s.segments {
			data, err := rep.BlobReader().GetBlob(ctx, bm.BlobID, 0, -1)
			if err != nil {
				return errors.Wrap(err, "error getting log")
			}

			data, err = rep.Crypter().DecryptBLOB(data, bm.BlobID)
			if err != nil {
				return errors.Wrap(err, "error decrypting log")
			}

			if err := showContentWithFlags(c.out.stdout(), bytes.NewReader(data), true, false); err != nil {
				log(ctx).Errorf("error showing log: %v", err)
			}
		}
	}

	return nil
}
