package main

import (
	"time"

	"github.com/FGasper/mongo-speedcam/history"
)

type eventStats struct {
	sizes, counts       map[string]int
	namespaceCounts     map[string]int // namespace -> count
	namespaceSizes      map[string]int // namespace -> total size
}

func tallyEventsHistory(
	eventsHistory *history.History[eventStats],
) (eventStats, int, time.Duration) {
	eventsInWindow := eventsHistory.Get()

	totalStats := eventStats{}
	initMap(&totalStats.counts)
	initMap(&totalStats.sizes)
	initMap(&totalStats.namespaceCounts)
	initMap(&totalStats.namespaceSizes)

	for _, curLog := range eventsInWindow {
		for evtType, val := range curLog.Datum.counts {
			if _, ok := totalStats.counts[evtType]; !ok {
				totalStats.counts[evtType] = val
			} else {
				totalStats.counts[evtType] += val
			}
		}

		for evtType, val := range curLog.Datum.sizes {
			if _, ok := totalStats.sizes[evtType]; !ok {
				totalStats.sizes[evtType] = val
			} else {
				totalStats.sizes[evtType] += val
			}

		}

		for ns, val := range curLog.Datum.namespaceCounts {
			if _, ok := totalStats.namespaceCounts[ns]; !ok {
				totalStats.namespaceCounts[ns] = val
			} else {
				totalStats.namespaceCounts[ns] += val
			}
		}

		for ns, val := range curLog.Datum.namespaceSizes {
			if _, ok := totalStats.namespaceSizes[ns]; !ok {
				totalStats.namespaceSizes[ns] = val
			} else {
				totalStats.namespaceSizes[ns] += val
			}
		}
	}

	curStatsInterval := time.Since(eventsInWindow[0].At)

	return totalStats, len(eventsInWindow), curStatsInterval
}
