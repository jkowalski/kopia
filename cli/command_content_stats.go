package cli

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type commandContentStats struct {
	raw          bool
	contentRange contentRangeFlags
}

func (c *commandContentStats) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("stats", "Content statistics")
	cmd.Flag("raw", "Raw numbers").Short('r').BoolVar(&c.raw)
	c.contentRange.setup(cmd)
	cmd.Action(svc.directRepositoryReadAction(c.run))
}

func (c *commandContentStats) run(ctx context.Context, rep repo.DirectRepository) error {
	var sizeThreshold uint32 = 10

	countMap := map[uint32]int{}
	totalSizeOfContentsUnder := map[uint32]int64{}

	var sizeThresholds []uint32

	for i := 0; i < 8; i++ {
		sizeThresholds = append(sizeThresholds, sizeThreshold)
		countMap[sizeThreshold] = 0
		sizeThreshold *= 10
	}

	var totalSize, count int64

	if err := rep.ContentReader().IterateContents(
		ctx,
		content.IterateOptions{
			Range: c.contentRange.contentIDRange(),
		},
		func(b content.Info) error {
			totalSize += int64(b.GetPackedLength())
			count++
			for s := range countMap {
				if b.GetPackedLength() < s {
					countMap[s]++
					totalSizeOfContentsUnder[s] += int64(b.GetPackedLength())
				}
			}
			return nil
		}); err != nil {
		return errors.Wrap(err, "error iterating contents")
	}

	sizeToString := units.BytesStringBase10
	if c.raw {
		sizeToString = func(l int64) string { return strconv.FormatInt(l, 10) }
	}

	fmt.Println("Count:", count)
	fmt.Println("Total:", sizeToString(totalSize))

	if count == 0 {
		return nil
	}

	fmt.Println("Average:", sizeToString(totalSize/count))

	fmt.Printf("Histogram:\n\n")

	var lastSize uint32

	for _, size := range sizeThresholds {
		fmt.Printf("%9v between %v and %v (total %v)\n",
			countMap[size]-countMap[lastSize],
			sizeToString(int64(lastSize)),
			sizeToString(int64(size)),
			sizeToString(totalSizeOfContentsUnder[size]-totalSizeOfContentsUnder[lastSize]),
		)

		lastSize = size
	}

	return nil
}
