package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/FGasper/mongo-speedcam/agg"
	"github.com/FGasper/mongo-speedcam/cursor"
	"github.com/FGasper/mongo-speedcam/history"
	"github.com/FGasper/mongo-speedcam/resumetoken"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func _runChangeStream(ctx context.Context, connstr string, interval time.Duration) error {
	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("opening session: %w", err)
	}

	sctx := mongo.NewSessionContext(ctx, sess)

	unixTimeStart := uint32(time.Now().Add(-interval).Unix())
	startTS := bson.Timestamp{T: unixTimeStart}

	db := client.Database("admin")

	fmt.Printf("Gathering change events from the past %s …\n", interval)

	startTime := time.Now()

	resp := db.RunCommand(
		sctx,
		bson.D{
			{"aggregate", 1},
			{"cursor", bson.D{}},
			{"pipeline", mongo.Pipeline{
				{{"$changeStream", bson.D{
					{"allChangesForCluster", true},
					{"showSystemEvents", true},
					{"showExpandedEvents", true},
					{"startAtOperationTime", startTS},
				}}},
				{{"$match", bson.D{
					{"clusterTime", bson.D{
						{"$lte", bson.Timestamp{T: uint32(time.Now().Unix())}},
					}},
				}}},
				{{"$addFields", bson.D{
					{"operationType", "$$REMOVE"},
					{"op", bson.D{{"$cond", bson.D{
						{"if", bson.D{{"$in", [2]any{
							"$operationType",
							eventsToTruncate,
						}}}},
						{"then", bson.D{{"$substr",
							[3]any{"$operationType", 0, 1},
						}}},
						{"else", "$operationType"},
					}}}},
					{"size", bson.D{{"$bsonSize", "$$ROOT"}}},
				}}},
				{{"$project", bson.D{
					{"_id", 1},
					{"op", 1},
					{"size", 1},
					{"clusterTime", 1},
				}}},
			}},
		},
	)

	cursor, err := cursor.New(db, resp)
	if err != nil {
		return fmt.Errorf("opening change stream: %w", err)
	}

	eventSizesByType := map[string]int{}
	eventCountsByType := map[string]int{}

	fullEventName := map[string]string{}
	for _, eventName := range eventsToTruncate {
		fullEventName[eventName[:1]] = eventName
	}

	var minUnixSecs, maxUnixSecs uint32

cursorLoop:
	for {
		if cursor.IsFinished() {
			return fmt.Errorf("unexpected end of change stream")
		}

		for _, event := range cursor.GetCurrentBatch() {
			t, _ := event.Lookup("clusterTime").Timestamp()

			if time.Unix(int64(t), 0).After(startTime) {
				break cursorLoop
			}

			if minUnixSecs == 0 {
				minUnixSecs = t
			}

			maxUnixSecs = t

			op := event.Lookup("op").StringValue()

			if fullOp, isShortened := fullEventName[op]; isShortened {
				op = fullOp
			}

			eventCountsByType[op]++
			eventSizesByType[op] += int(event.Lookup("size").AsInt64())
		}

		rt, hasToken := cursor.GetCursorExtra()["postBatchResumeToken"]
		if !hasToken {
			return fmt.Errorf("change stream lacks resume token??")
		}

		tokenTS, err := resumetoken.New(rt.Document()).Timestamp()
		if err != nil {
			return fmt.Errorf("parsing timestamp from change stream resume token")
		}

		if time.Unix(int64(tokenTS.T), 0).After(startTime) {
			break cursorLoop
		}

		if err := cursor.GetNext(sctx); err != nil {
			return fmt.Errorf("iterating change stream: %w", err)
		}
	}

	delta := time.Duration(1+maxUnixSecs-minUnixSecs) * time.Second

	displayTable(eventCountsByType, eventSizesByType, delta)

	return nil
}

func _runChangeStreamLoop(
	ctx context.Context,
	connstr string,
	window, reportInterval time.Duration,
) error {
	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("opening session: %w", err)
	}

	sctx := mongo.NewSessionContext(ctx, sess)

	cs, err := client.Watch(
		sctx,
		mongo.Pipeline{
			// Stage 1: $match - Filter out system databases, collections, and namespace first
			{{"$match", agg.And{
				// Database filter: Allow only user databases
				agg.Expr(agg.Not{agg.Or{
					agg.In("$ns.db", "mongosync_reserved_for_internal_use", "admin", "local", "config"),
					agg.Eq(0, agg.IndexOfCP("$ns.db", "mongosync_reserved_for_verification_", 0, 1)),
					agg.Eq(0, agg.IndexOfCP("$ns.db", "__mdb_internal", 0, 1)),
				}}),
				// Collection filter: Allow only non-system collections
				agg.Expr(agg.Not{agg.Eq(0, agg.IndexOfCP("$ns.coll", "system.", 0, 1))}),
				// Namespace filter: Match everything or explicitly allow rename operations
				agg.Or{
					bson.D{},                            // empty document - matches everything
					bson.D{{"operationType", "rename"}}, // explicitly allow rename operations
				},
			}}},

			// Stage 3: Final projection with existing logic
			{{"$project", bson.D{
				{"updateDescription", "$$REMOVE"},
				{"_id", 1},
				{"clusterTime", 1},
				{"op", agg.Cond{
					If:   agg.In("$operationType", eventsToTruncate...),
					Then: agg.SubstrBytes{"$operationType", 0, 1},
					Else: "$operationType",
				}},
				{"size", agg.BSONSize("$$ROOT")},
				{"ns", 1},
			}}},
		},
		options.ChangeStream().
			SetCustomPipeline(bson.M{
				"showSystemEvents":   true,
				"showExpandedEvents": true,
			}).SetFullDocument("updateLookup"),
	)
	if err != nil {
		return fmt.Errorf("opening change stream: %w", err)
	}
	defer cs.Close(sctx)

	fmt.Printf("Listening for change events. Stats showing every %s …\n", reportInterval)

	eventsHistory := history.New[eventStats](window)

	var changeStreamLag atomic.Pointer[time.Duration]

	go func() {
		for {
			time.Sleep(reportInterval)

			totalStats, _, curStatsInterval := tallyEventsHistory(eventsHistory)

			displayTable(totalStats.counts, totalStats.sizes, curStatsInterval)

			fmt.Printf("Change stream lag: %s\n", lo.FromPtr(changeStreamLag.Load()))
		}
	}()

	fullEventName := map[string]string{}
	for _, eventName := range eventsToTruncate {
		fullEventName[eventName[:1]] = eventName
	}

	var curEventStats eventStats
	initMap(&curEventStats.counts)
	initMap(&curEventStats.sizes)

	for cs.Next(sctx) {
		op := cs.Current.Lookup("op").StringValue()

		if fullOp, isShortened := fullEventName[op]; isShortened {
			op = fullOp
		}

		curEventStats.counts[op]++
		curEventStats.sizes[op] += int(cs.Current.Lookup("size").AsInt64())

		if cs.RemainingBatchLength() == 0 {
			eventsHistory.Add(curEventStats)
			initMap(&curEventStats.counts)
			initMap(&curEventStats.sizes)
		}

		sessTS, err := GetClusterTimeFromSession(sess)
		if err != nil {

		} else {
			eventT, _ := cs.Current.Lookup("clusterTime").Timestamp()

			lagSecs := int64(sessTS.T) - int64(eventT)
			changeStreamLag.Store(lo.ToPtr(time.Duration(lagSecs) * time.Second))
		}
	}
	if cs.Err() != nil {
		return fmt.Errorf("reading change stream: %w", cs.Err())
	}

	return fmt.Errorf("unexpected end of change stream")
}

func _runChangeStreamLoopFilterManually(
	ctx context.Context,
	connstr string,
	window, reportInterval time.Duration,
) error {
	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("opening session: %w", err)
	}

	sctx := mongo.NewSessionContext(ctx, sess)

	// Watch without $match filter - we'll filter on the client side
	cs, err := client.Watch(
		sctx,
		mongo.Pipeline{
			// Only projection, no $match stage
			{{"$project", bson.D{
				{"updateDescription", "$$REMOVE"},
				{"_id", 1},
				{"clusterTime", 1},
				{"operationType", 1},
				{"ns", 1},
			}}},
		},
		options.ChangeStream().
			SetCustomPipeline(bson.M{
				"showSystemEvents":   true,
				"showExpandedEvents": true,
			}).SetFullDocument("updateLookup"),
	)
	if err != nil {
		return fmt.Errorf("opening change stream: %w", err)
	}
	defer cs.Close(sctx)

	fmt.Printf("Listening for change events (filtering manually). Stats showing every %s …\n", reportInterval)

	eventsHistory := history.New[eventStats](window)

	var changeStreamLag atomic.Pointer[time.Duration]

	go func() {
		for {
			time.Sleep(reportInterval)

			totalStats, _, curStatsInterval := tallyEventsHistory(eventsHistory)

			displayTable(totalStats.counts, totalStats.sizes, curStatsInterval)

			fmt.Printf("Change stream lag: %s\n", lo.FromPtr(changeStreamLag.Load()))
		}
	}()

	fullEventName := map[string]string{}
	for _, eventName := range eventsToTruncate {
		fullEventName[eventName[:1]] = eventName
	}

	var curEventStats eventStats
	initMap(&curEventStats.counts)
	initMap(&curEventStats.sizes)

	// Helper function to check if event should be filtered out
	shouldFilterEvent := func(event bson.Raw) bool {
		// Get namespace info
		nsDB := event.Lookup("ns", "db").StringValue()
		nsColl := event.Lookup("ns", "coll").StringValue()
		operationType := event.Lookup("operationType").StringValue()

		// Database filter: Filter out system databases
		if nsDB == "mongosync_reserved_for_internal_use" ||
			nsDB == "admin" ||
			nsDB == "local" ||
			nsDB == "config" {
			return true
		}

		// Filter out databases starting with specific prefixes
		if len(nsDB) >= len("mongosync_reserved_for_verification_") &&
			nsDB[:len("mongosync_reserved_for_verification_")] == "mongosync_reserved_for_verification_" {
			return true
		}
		if len(nsDB) >= len("__mdb_internal") &&
			nsDB[:len("__mdb_internal")] == "__mdb_internal" {
			return true
		}

		// Collection filter: Filter out system collections
		if len(nsColl) >= len("system.") &&
			nsColl[:len("system.")] == "system." {
			return true
		}

		// Allow rename operations explicitly
		if operationType == "rename" {
			return false
		}

		return false
	}

	for cs.Next(sctx) {
		// Apply client-side filtering
		if shouldFilterEvent(cs.Current) {
			continue
		}

		// Get operation type and convert to short form if needed
		operationType := cs.Current.Lookup("operationType").StringValue()
		op := operationType

		// Check if this is an event to truncate
		for _, eventName := range eventsToTruncate {
			if operationType == eventName {
				op = eventName[:1]
				break
			}
		}

		// Calculate size
		size := len(cs.Current)

		curEventStats.counts[op]++
		curEventStats.sizes[op] += size

		if cs.RemainingBatchLength() == 0 {
			eventsHistory.Add(curEventStats)
			initMap(&curEventStats.counts)
			initMap(&curEventStats.sizes)
		}

		sessTS, err := GetClusterTimeFromSession(sess)
		if err != nil {

		} else {
			eventT, _ := cs.Current.Lookup("clusterTime").Timestamp()

			lagSecs := int64(sessTS.T) - int64(eventT)
			changeStreamLag.Store(lo.ToPtr(time.Duration(lagSecs) * time.Second))
		}
	}
	if cs.Err() != nil {
		return fmt.Errorf("reading change stream: %w", cs.Err())
	}

	return fmt.Errorf("unexpected end of change stream")
}
