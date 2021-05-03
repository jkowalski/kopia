package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/maintenance"
)

type commandContentRewrite struct {
	contentRewriteIDs           []string
	contentRewriteParallelism   int
	contentRewriteShortPacks    bool
	contentRewriteFormatVersion int
	contentRewritePackPrefix    string
	contentRewriteDryRun        bool
	contentRewriteSafety        maintenance.SafetyParameters

	contentRange contentRangeFlags
}

func (c *commandContentRewrite) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("rewrite", "Rewrite content using most recent format")
	cmd.Arg("contentID", "Identifiers of contents to rewrite").StringsVar(&c.contentRewriteIDs)
	cmd.Flag("parallelism", "Number of parallel workers").Default("16").IntVar(&c.contentRewriteParallelism)

	cmd.Flag("short", "Rewrite contents from short packs").BoolVar(&c.contentRewriteShortPacks)
	cmd.Flag("format-version", "Rewrite contents using the provided format version").Default("-1").IntVar(&c.contentRewriteFormatVersion)
	cmd.Flag("pack-prefix", "Only rewrite contents from pack blobs with a given prefix").StringVar(&c.contentRewritePackPrefix)
	cmd.Flag("dry-run", "Do not actually rewrite, only print what would happen").Short('n').BoolVar(&c.contentRewriteDryRun)
	c.contentRange.setup(cmd)
	safetyFlagVar(cmd, &c.contentRewriteSafety)
	cmd.Action(svc.directRepositoryWriteAction(c.runContentRewriteCommand))
}

func (c *commandContentRewrite) runContentRewriteCommand(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	advancedCommand(ctx)

	return maintenance.RewriteContents(ctx, rep, &maintenance.RewriteContentsOptions{
		ContentIDRange: c.contentRange.contentIDRange(),
		ContentIDs:     toContentIDs(c.contentRewriteIDs),
		FormatVersion:  c.contentRewriteFormatVersion,
		PackPrefix:     blob.ID(c.contentRewritePackPrefix),
		Parallel:       c.contentRewriteParallelism,
		ShortPacks:     c.contentRewriteShortPacks,
		DryRun:         c.contentRewriteDryRun,
	}, c.contentRewriteSafety)
}

func toContentIDs(s []string) []content.ID {
	var result []content.ID
	for _, cid := range s {
		result = append(result, content.ID(cid))
	}

	return result
}
