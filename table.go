package main

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"time"

	mmmath "github.com/FGasper/mongo-speedcam/math"
	"github.com/olekukonko/tablewriter"
	"github.com/samber/lo"
)

func displayTable(
	eventCountsByType map[string]int,
	eventSizesByType map[string]int,
	delta time.Duration,
) {
	if delta == 0 {
		panic("nonzero delta is nonsensical!")
	}

	if len(eventCountsByType) == 0 {
		fmt.Printf("No writes seen over %s.\n", delta.Round(10*time.Millisecond))
		return
	}

	allEventsCount := lo.Sum(slices.Collect(maps.Values(eventCountsByType)))
	totalSize := lo.Sum(slices.Collect(maps.Values(eventSizesByType)))

	fmt.Print("\n")

	fmt.Printf(
		"%s ops/sec (%s/sec; avg: %s)\n",
		FmtReal((mmmath.DivideToF64(allEventsCount, delta.Seconds()))),
		FmtBytes(mmmath.DivideToF64(totalSize, delta.Seconds())),
		FmtBytes(mmmath.DivideToF64(totalSize, allEventsCount)),
	)

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{
		"Event",
		"Count",
		"Total Size",
		"Avg Size",
	})

	eventTypes := slices.Sorted(maps.Keys(eventCountsByType))

	for _, eventType := range eventTypes {
		countFraction := mmmath.DivideToF64(eventCountsByType[eventType], allEventsCount)
		sizeFraction := mmmath.DivideToF64(eventSizesByType[eventType], totalSize)

		lo.Must0(table.Append([]string{
			eventType,
			fmt.Sprintf(
				"%s%%",
				FmtReal(100*countFraction),
			),
			fmt.Sprintf(
				"%s%%",
				FmtReal(100*sizeFraction),
			),
			FmtBytes(mmmath.DivideToF64(eventSizesByType[eventType], eventCountsByType[eventType])),
		}))
	}

	lo.Must0(table.Render())

	fmt.Printf("Sample window: %s\n", delta.Round(10*time.Millisecond))
}

func displayNamespaceTable(
	namespaceCounts map[string]int,
	namespaceSizes map[string]int,
	delta time.Duration,
) {
	if len(namespaceCounts) == 0 {
		return
	}

	totalOps := lo.Sum(slices.Collect(maps.Values(namespaceCounts)))
	totalSize := lo.Sum(slices.Collect(maps.Values(namespaceSizes)))

	fmt.Print("\nPer-Namespace Statistics:\n")

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{
		"Namespace",
		"Ops",
		"% of Ops",
		"Size",
		"% of Size",
		"Ops/sec",
	})

	// Sort namespaces by operation count (descending)
	namespaces := slices.Sorted(maps.Keys(namespaceCounts))
	slices.SortFunc(namespaces, func(a, b string) int {
		return namespaceCounts[b] - namespaceCounts[a]
	})

	for _, ns := range namespaces {
		opsCount := namespaceCounts[ns]
		size := namespaceSizes[ns]
		opsFraction := mmmath.DivideToF64(opsCount, totalOps)
		sizeFraction := mmmath.DivideToF64(size, totalSize)
		opsPerSec := mmmath.DivideToF64(opsCount, delta.Seconds())

		lo.Must0(table.Append([]string{
			ns,
			FmtReal(opsCount),
			fmt.Sprintf("%s%%", FmtReal(100*opsFraction)),
			FmtBytes(size),
			fmt.Sprintf("%s%%", FmtReal(100*sizeFraction)),
			FmtReal(opsPerSec),
		}))
	}

	lo.Must0(table.Render())
}
