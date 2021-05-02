package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandManifestList struct {
	manifestListFilter []string
	manifestListSort   []string

	jo jsonOutput
}

func (c *commandManifestList) setup(parent commandParent) {
	cmd := parent.Command("list", "List manifest items").Alias("ls").Default()
	cmd.Flag("filter", "List of key:value pairs").StringsVar(&c.manifestListFilter)
	cmd.Flag("sort", "List of keys to sort by").StringsVar(&c.manifestListSort)
	c.jo.setup(cmd)
	cmd.Action(repositoryReaderAction(c.listManifestItems))
}

func (c *commandManifestList) listManifestItems(ctx context.Context, rep repo.Repository) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	filter := map[string]string{}

	for _, kv := range c.manifestListFilter {
		p := strings.Index(kv, ":")
		if p <= 0 {
			return errors.Errorf("invalid list filter %q, missing ':'", kv)
		}

		filter[kv[0:p]] = kv[p+1:]
	}

	items, err := rep.FindManifests(ctx, filter)
	if err != nil {
		return errors.Wrap(err, "unable to find manifests")
	}

	sort.Slice(items, func(i, j int) bool {
		for _, key := range c.manifestListSort {
			if v1, v2 := items[i].Labels[key], items[j].Labels[key]; v1 != v2 {
				return v1 < v2
			}
		}

		return items[i].ModTime.Before(items[j].ModTime)
	})

	for _, it := range items {
		if c.jo.jsonOutput {
			jl.emit(it)
		} else {
			t := it.Labels["type"]
			fmt.Printf("%v %10v %v type:%v %v\n", it.ID, it.Length, formatTimestamp(it.ModTime.Local()), t, sortedMapValues(it.Labels))
		}
	}

	return nil
}

func sortedMapValues(m map[string]string) string {
	var result []string

	for k, v := range m {
		if k == "type" {
			continue
		}

		result = append(result, fmt.Sprintf("%v:%v", k, v))
	}

	sort.Strings(result)

	return strings.Join(result, " ")
}
