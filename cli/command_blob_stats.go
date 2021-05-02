package cli

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type commandBlobStats struct {
	raw    bool
	prefix string
}

func (c *commandBlobStats) setup(app appServices, parent commandParent) {
	cmd := parent.Command("stats", "Content statistics")
	cmd.Flag("raw", "Raw numbers").Short('r').BoolVar(&c.raw)
	cmd.Flag("prefix", "Blob name prefix").StringVar(&c.prefix)
	cmd.Action(app.directRepositoryReadAction(c.run))
}

func (c *commandBlobStats) run(ctx context.Context, rep repo.DirectRepository) error {
	var sizeThreshold int64 = 10

	countMap := map[int64]int{}
	totalSizeOfContentsUnder := map[int64]int64{}

	var sizeThresholds []int64

	for i := 0; i < 8; i++ {
		sizeThresholds = append(sizeThresholds, sizeThreshold)
		countMap[sizeThreshold] = 0
		sizeThreshold *= 10
	}

	var totalSize, count int64

	if err := rep.BlobReader().ListBlobs(
		ctx,
		blob.ID(c.prefix),
		func(b blob.Metadata) error {
			totalSize += b.Length
			count++
			if count%10000 == 0 {
				log(ctx).Infof("Got %v blobs...", count)
			}
			for s := range countMap {
				if b.Length < s {
					countMap[s]++
					totalSizeOfContentsUnder[s] += b.Length
				}
			}
			return nil
		}); err != nil {
		return errors.Wrap(err, "error listing blobs")
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

	var lastSize int64

	for _, size := range sizeThresholds {
		fmt.Printf("%9v between %v and %v (total %v)\n",
			countMap[size]-countMap[lastSize],
			sizeToString(lastSize),
			sizeToString(size),
			sizeToString(totalSizeOfContentsUnder[size]-totalSizeOfContentsUnder[lastSize]),
		)

		lastSize = size
	}

	return nil
}
