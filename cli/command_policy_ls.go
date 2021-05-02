package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandPolicyList struct {
	jo jsonOutput
}

func (c *commandPolicyList) setup(parent commandParent) {
	cmd := parent.Command("list", "List policies.").Alias("ls")
	c.jo.setup(cmd)
	cmd.Action(repositoryReaderAction(c.run))
}

func (c *commandPolicyList) run(ctx context.Context, rep repo.Repository) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	policies, err := policy.ListPolicies(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "error listing policies")
	}

	sort.Slice(policies, func(i, j int) bool {
		return policies[i].Target().String() < policies[j].Target().String()
	})

	for _, pol := range policies {
		if c.jo.jsonOutput {
			jl.emit(policy.TargetWithPolicy{ID: pol.ID(), Target: pol.Target(), Policy: pol})
		} else {
			fmt.Println(pol.ID(), pol.Target())
		}
	}

	return nil
}
