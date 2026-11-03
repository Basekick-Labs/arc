package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

func TestTopicMappingJSONAcceptsLegacyAndRuleValues(t *testing.T) {
	input := `{"legacy/sensors":"iot","factory/+/+/metrics":{"measurement":"machine_metrics","tags_from_topic":[{"position":1,"tag_name":"line"},{"position":2,"tag_name":"machine_id"}]}}`
	var mapping TopicMapping
	if err := json.Unmarshal([]byte(input), &mapping); err != nil {
		t.Fatalf("unmarshal topic mapping: %v", err)
	}

	if got := mapping["legacy/sensors"].Database; got != "iot" {
		t.Errorf("legacy database = %q, want iot", got)
	}
	wantRule := TopicMappingRule{
		Measurement: "machine_metrics",
		TagsFromTopic: []TopicTagExtraction{
			{Position: 1, TagName: "line"},
			{Position: 2, TagName: "machine_id"},
		},
	}
	if got := mapping["factory/+/+/metrics"]; !reflect.DeepEqual(got, wantRule) {
		t.Errorf("object rule = %#v, want %#v", got, wantRule)
	}
	compiled, err := compileTopicMapping(mapping)
	if err != nil {
		t.Fatalf("compile decoded mapping: %v", err)
	}
	legacy, _, matched := matchTopicMapping(compiled, "legacy/sensors")
	if !matched || legacy.Database != "iot" {
		t.Fatalf("legacy topic mapping = %#v, matched=%v", legacy, matched)
	}

	encoded, err := json.Marshal(mapping)
	if err != nil {
		t.Fatalf("marshal topic mapping: %v", err)
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &values); err != nil {
		t.Fatalf("unmarshal encoded topic mapping: %v", err)
	}
	if len(values["legacy/sensors"]) == 0 || values["legacy/sensors"][0] != '"' {
		t.Errorf("legacy database rule changed JSON shape: %s", values["legacy/sensors"])
	}
	if len(values["factory/+/+/metrics"]) == 0 || values["factory/+/+/metrics"][0] != '{' {
		t.Errorf("object rule lost JSON object shape: %s", values["factory/+/+/metrics"])
	}

	var roundTrip TopicMapping
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatalf("round-trip topic mapping: %v", err)
	}
	if !reflect.DeepEqual(roundTrip, mapping) {
		t.Errorf("round-trip = %#v, want %#v", roundTrip, mapping)
	}
}

func TestTopicMappingJSONRejectsMalformedRules(t *testing.T) {
	for _, input := range []string{
		`{"a":null}`,
		`{"a":1}`,
		`{"a":{"tags_from_topic":[{"tag_name":"line"}]}}`,
	} {
		t.Run(input, func(t *testing.T) {
			var mapping TopicMapping
			if err := json.Unmarshal([]byte(input), &mapping); err == nil {
				t.Fatalf("unmarshal %s succeeded, want an error", input)
			}
		})
	}
}

func TestSubscriptionRequestsDecodeObjectTopicMappings(t *testing.T) {
	createJSON := `{"topic_mapping":{"sensors/legacy":"iot","factory/+/+/metrics":{"measurement":"machine_metrics"}}}`
	var create CreateSubscriptionRequest
	if err := json.Unmarshal([]byte(createJSON), &create); err != nil {
		t.Fatalf("decode create request: %v", err)
	}
	if create.TopicMapping["sensors/legacy"].Database != "iot" || create.TopicMapping["factory/+/+/metrics"].Measurement != "machine_metrics" {
		t.Fatalf("create topic mapping = %#v", create.TopicMapping)
	}

	updateJSON := `{"topic_mapping":{"factory/#":{"tags_from_topic":[{"position":1,"tag_name":"line"}]}}}`
	var update UpdateSubscriptionRequest
	if err := json.Unmarshal([]byte(updateJSON), &update); err != nil {
		t.Fatalf("decode update request: %v", err)
	}
	if update.TopicMapping == nil || (*update.TopicMapping)["factory/#"].TagsFromTopic[0].TagName != "line" {
		t.Fatalf("update topic mapping = %#v", update.TopicMapping)
	}
}

func TestCompileTopicMappingValidatesFiltersAndRuleValues(t *testing.T) {
	tests := []struct {
		name    string
		mapping TopicMapping
		wantErr bool
	}{
		{name: "valid wildcard", mapping: TopicMapping{"factory/+/metrics": {Measurement: "metrics"}}},
		{name: "embedded plus", mapping: TopicMapping{"factory/a+/metrics": {Database: "iot"}}, wantErr: true},
		{name: "nonterminal hash", mapping: TopicMapping{"factory/#/metrics": {Database: "iot"}}, wantErr: true},
		{name: "bad database", mapping: TopicMapping{"factory/+": {Database: "../iot"}}, wantErr: true},
		{name: "bad measurement", mapping: TopicMapping{"factory/+": {Measurement: "1metrics"}}, wantErr: true},
		{name: "negative position", mapping: TopicMapping{"factory/+": {TagsFromTopic: []TopicTagExtraction{{Position: -1, TagName: "line"}}}}, wantErr: true},
		{name: "time tag", mapping: TopicMapping{"factory/+": {TagsFromTopic: []TopicTagExtraction{{Position: 1, TagName: "TIME"}}}}, wantErr: true},
		{name: "duplicate tags", mapping: TopicMapping{"factory/+": {TagsFromTopic: []TopicTagExtraction{{Position: 1, TagName: "line"}, {Position: 2, TagName: "LINE"}}}}, wantErr: true},
		{name: "empty rule", mapping: TopicMapping{"factory/+": {}}, wantErr: true},
		// Without a trailing # a topic has exactly the filter's levels, so a
		// position past them can never hold a value.
		{name: "position past last level", mapping: TopicMapping{"factory/+/metrics": {TagsFromTopic: []TopicTagExtraction{{Position: 3, TagName: "x"}}}}, wantErr: true},
		{name: "position past exact filter", mapping: TopicMapping{"a/b": {TagsFromTopic: []TopicTagExtraction{{Position: 2, TagName: "x"}}}}, wantErr: true},
		{name: "last level position", mapping: TopicMapping{"factory/+/metrics": {TagsFromTopic: []TopicTagExtraction{{Position: 2, TagName: "x"}}}}},
		{name: "position under trailing hash", mapping: TopicMapping{"factory/#": {TagsFromTopic: []TopicTagExtraction{{Position: 9, TagName: "x"}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTopicMapping(tt.mapping, true)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateTopicMapping() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Before #479 a mapping key was an exact topic, so a stored "sensors/temp+x"
// was legal and never matched: a published topic cannot contain a wildcard.
// Such a row keeps starting and keeps ingesting; only the dead rule is left
// out. Its targets are still checked, and a create or a topic_mapping update
// still refuses the filter.
func TestStoredMappingWithAnInvalidFilterKeepsWorking(t *testing.T) {
	mapping := TopicMapping{
		"sensors/temp+x": {Database: "olddb"},
		"plant/#":        {Database: "plantdb"},
	}
	if err := validateTopicMapping(mapping, true); err == nil {
		t.Fatal("strict validation accepted an invalid filter")
	}
	if err := validateTopicMapping(mapping, false); err != nil {
		t.Fatalf("boot validation refused a dead filter: %v", err)
	}
	if err := validateTopicMapping(TopicMapping{"sensors/temp+x": {Database: "../x"}}, false); err == nil {
		t.Fatal("boot validation accepted an unwritable database behind a dead filter")
	}
	dead := deadTopicFilters(mapping)
	if len(dead) != 1 || dead[0].filter != "sensors/temp+x" {
		t.Fatalf("deadTopicFilters = %+v, want only sensors/temp+x", dead)
	}

	s := NewSubscriber(&Subscription{ID: "s", Name: "s", Database: "iot", TopicMapping: mapping}, nil, nil, nil, zerolog.Nop())
	defer s.cancel()
	if s.topicMappingErr != nil {
		t.Fatalf("NewSubscriber: %v", s.topicMappingErr)
	}
	if rule, _, ok := matchTopicMapping(s.topicMappingRules, "plant/a"); !ok || rule.Database != "plantdb" {
		t.Fatalf("plant/a matched %v %+v, want plantdb", ok, rule)
	}
	if _, _, ok := matchTopicMapping(s.topicMappingRules, "sensors/temp+x"); ok {
		t.Fatal("a dead filter was compiled")
	}
}

func TestManagerUpdateOfAStoredInvalidFilter(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	sub := &Subscription{
		ID: "legacy-1", Name: "legacy", Broker: "tcp://localhost:1883", ClientID: "c",
		Topics: []string{"sensors/#"}, QoS: 1, Database: "iot",
		TopicMapping: TopicMapping{"sensors/temp+x": {Database: "olddb"}},
	}
	sub.SetDefaults()
	if err := mgr.repo.Create(ctx, sub); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	name := "renamed"
	if _, err := mgr.Update(ctx, sub.ID, &UpdateSubscriptionRequest{Name: &name}); err != nil {
		t.Fatalf("an update that leaves topic_mapping alone was refused: %v", err)
	}
	same := TopicMapping{"sensors/temp+x": {Database: "olddb"}}
	if _, err := mgr.Update(ctx, sub.ID, &UpdateSubscriptionRequest{TopicMapping: &same}); !errors.Is(err, ErrValidation) {
		t.Fatalf("an update that sets an invalid filter: err = %v, want ErrValidation", err)
	}
}

// Unknown keys are refused, so a typo cannot leave a rule silently partial.
func TestTopicMappingRefusesUnknownKeys(t *testing.T) {
	for _, input := range []string{
		`{"a/+":{"database":"d","tag_from_topic":[{"position":1,"tag_name":"x"}]}}`,
		`{"a/+":{"tags_from_topic":[{"position":1,"tag_name":"x","tagname":"y"}]}}`,
	} {
		var mapping TopicMapping
		if err := json.Unmarshal([]byte(input), &mapping); err == nil {
			t.Errorf("unmarshal %s: accepted an unknown key", input)
		}
	}
}

func TestMapToRecordMeasurementOnlyRule(t *testing.T) {
	subscriber := &Subscriber{logger: zerolog.Nop()}
	decoded, err := subscriber.convertToRecords(map[string]interface{}{
		"m":    "payload_metrics",
		"tags": map[string]interface{}{"site": "plant-a"},
		"v":    1.0,
	}, TopicMappingRule{Measurement: "rule_metrics"}, nil)
	if err != nil {
		t.Fatalf("convert payload: %v", err)
	}
	record := decoded[0].(*models.Record)
	if record.Measurement != "rule_metrics" || record.Tags["site"] != "plant-a" || record.Fields["v"] != 1.0 {
		t.Fatalf("record = %+v, want rule measurement with payload tags and fields", record)
	}
}

// A topic tag wins over a payload tag that differs from it only in case.
// Stored names are case-insensitive and StripRecordNames keeps the first
// spelling in byte order, so a payload "LINE" would otherwise drop the
// configured "line" on every message.
func TestTopicTagWinsOverACaseOnlyPayloadTag(t *testing.T) {
	subscriber := &Subscriber{logger: zerolog.Nop()}
	decoded, err := subscriber.convertToRecords(map[string]interface{}{
		"tags": map[string]interface{}{"LINE": "payload-tag", "site": "a"},
		"v":    1.0,
	}, TopicMappingRule{TagsFromTopic: []TopicTagExtraction{{Position: 1, TagName: "line"}}}, []string{"factory", "line-3"})
	if err != nil {
		t.Fatalf("convert payload: %v", err)
	}
	record := decoded[0].(*models.Record)
	if !reflect.DeepEqual(record.Tags, map[string]string{"line": "line-3", "site": "a"}) {
		t.Fatalf("tags = %#v, want line=line-3 and site=a", record.Tags)
	}
	if drops := ingest.StripRecordNames(decoded, nil); len(drops) != 0 || record.Tags["line"] != "line-3" {
		t.Fatalf("StripRecordNames dropped %+v; tags now %#v", drops, record.Tags)
	}
}

// Topic tags must not create two columns that differ only in case. The
// writer stores a field of exactly a tag's name as "<name>_value"; renaming a
// case-variant field the same way here once let "Temp_value" and the writer's
// "temp_value" both reach the file (#1342). Whatever survives StripRecordNames,
// the names the writer then stores are pairwise distinct after folding.
func TestTopicTagsLeaveNoCaseTwinColumns(t *testing.T) {
	subscriber := &Subscriber{logger: zerolog.Nop()}
	decoded, err := subscriber.convertToRecords(map[string]interface{}{
		"temp": 1.0,
		"Temp": 2.0,
		"v":    3.0,
	}, TopicMappingRule{TagsFromTopic: []TopicTagExtraction{{Position: 1, TagName: "temp"}}}, []string{"factory", "hot"})
	if err != nil {
		t.Fatalf("convert payload: %v", err)
	}
	ingest.StripRecordNames(decoded, nil)
	record := decoded[0].(*models.Record)
	stored := map[string]string{}
	add := func(name string) {
		folded := ingest.FoldColumnName(name)
		if prev, dup := stored[folded]; dup {
			t.Fatalf("columns %q and %q differ only in case: tags %#v fields %#v", prev, name, record.Tags, record.Fields)
		}
		stored[folded] = name
	}
	for name := range record.Tags {
		add(name)
	}
	for name := range record.Fields {
		if _, isTag := record.Tags[name]; isTag {
			name += "_value" // the writer's rename (arrow_writer rowsToColumnar)
		}
		add(name)
	}
}

func TestMatchTopicMappingSupportsMQTTFilters(t *testing.T) {
	rules, err := compileTopicMapping(TopicMapping{
		"factory/+/+/metrics":              {Measurement: "wildcard_metrics"},
		"factory/line-7/+/metrics":         {Measurement: "specific_metrics"},
		"factory/line-7/machine-3/metrics": {Database: "exact_db"},
		"factory/+":                        {Database: "single_level"},
		"devices/#":                        {Database: "devices"},
		"$SYS/#":                           {Database: "system"},
		"#":                                {Database: "all_other_topics"},
	})
	if err != nil {
		t.Fatalf("compile rules: %v", err)
	}

	tests := []struct {
		topic           string
		wantDatabase    string
		wantMeasurement string
		wantMatched     bool
	}{
		{topic: "factory/line-7/machine-3/metrics", wantDatabase: "exact_db", wantMatched: true},
		{topic: "factory/line-7/machine-4/metrics", wantMeasurement: "specific_metrics", wantMatched: true},
		{topic: "factory/line-8/machine-4/metrics", wantMeasurement: "wildcard_metrics", wantMatched: true},
		{topic: "factory/line-8/metrics", wantDatabase: "all_other_topics", wantMatched: true},
		{topic: "devices", wantDatabase: "devices", wantMatched: true},
		{topic: "devices/zone/line", wantDatabase: "devices", wantMatched: true},
		{topic: "$SYS/status", wantDatabase: "system", wantMatched: true},
		{topic: "unmatched", wantDatabase: "all_other_topics", wantMatched: true},
	}
	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got, _, matched := matchTopicMapping(rules, tt.topic)
			if matched != tt.wantMatched || got.Database != tt.wantDatabase || got.Measurement != tt.wantMeasurement {
				t.Fatalf("matchTopicMapping(%q) = (%#v, %v), want database=%q measurement=%q matched=%v", tt.topic, got, matched, tt.wantDatabase, tt.wantMeasurement, tt.wantMatched)
			}
		})
	}

	// A leading wildcard filter must not match a system topic.
	wildcardOnly, err := compileTopicMapping(TopicMapping{"#": {Database: "iot"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, matched := matchTopicMapping(wildcardOnly, "$SYS/status"); matched {
		t.Fatal("# matched a topic beginning with $")
	}
	singleLevel, err := compileTopicMapping(TopicMapping{"factory/+": {Database: "iot"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, matched := matchTopicMapping(singleLevel, "factory/line-7"); !matched {
		t.Fatal("+ did not match one topic segment")
	}
	if _, _, matched := matchTopicMapping(singleLevel, "factory/line-7/machine-3"); matched {
		t.Fatal("+ matched more than one topic segment")
	}
}

func TestMapToRecordAppliesTopicMeasurementAndTags(t *testing.T) {
	subscriber := &Subscriber{logger: zerolog.Nop()}
	topic := "factory/line-7/machine-3/metrics"
	payload := map[string]interface{}{
		"measurement": "../payload-measurement",
		"tags":        map[string]interface{}{"line": "payload-line", "site": "plant-a"},
		"value":       12.5,
	}
	rules, err := compileTopicMapping(TopicMapping{"factory/+/+/metrics": {
		Measurement: "machine_metrics",
		TagsFromTopic: []TopicTagExtraction{
			{Position: 1, TagName: "line"},
			{Position: 2, TagName: "machine_id"},
		},
	}})
	if err != nil {
		t.Fatalf("compile topic mapping: %v", err)
	}
	rule, matchedLevels, matched := matchTopicMapping(rules, topic)
	if !matched {
		t.Fatalf("topic %q did not match mapping", topic)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	decoded, err := subscriber.decodePayload(topic, encoded, rule, matchedLevels)
	if err != nil {
		t.Fatalf("decode mapped payload: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("record count = %d, want 1", len(decoded))
	}
	record, ok := decoded[0].(*models.Record)
	if !ok {
		t.Fatalf("record type = %T, want *models.Record", decoded[0])
	}
	if record.Measurement != "machine_metrics" {
		t.Errorf("measurement = %q, want machine_metrics", record.Measurement)
	}
	if record.Tags["line"] != "line-7" || record.Tags["machine_id"] != "machine-3" || record.Tags["site"] != "plant-a" {
		t.Errorf("tags = %#v, want topic values plus payload site", record.Tags)
	}
}

// Under a trailing # a topic can be shorter than a position: that segment is
// skipped and the row is still written.
func TestMapToRecordSkipsASegmentAHashTopicLacks(t *testing.T) {
	subscriber := &Subscriber{logger: zerolog.Nop()}
	rules, err := compileTopicMapping(TopicMapping{"factory/#": {TagsFromTopic: []TopicTagExtraction{
		{Position: 1, TagName: "line"}, {Position: 4, TagName: "missing"},
	}}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	rule, levels, ok := matchTopicMapping(rules, "factory/line-1")
	if !ok {
		t.Fatal("factory/line-1 did not match factory/#")
	}
	decoded, err := subscriber.convertToRecords(map[string]interface{}{"v": 1.0}, rule, levels)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	record := decoded[0].(*models.Record)
	if record.Tags["line"] != "line-1" {
		t.Errorf("tags = %#v, want line=line-1", record.Tags)
	}
	if _, ok := record.Tags["missing"]; ok {
		t.Errorf("out-of-range extraction added a tag: %#v", record.Tags)
	}
}

func TestMapToRecordKeepsPayloadValuesWhenRuleOmitsFields(t *testing.T) {
	subscriber := &Subscriber{logger: zerolog.Nop()}
	payload := map[string]interface{}{
		"m":    "payload_metrics",
		"tags": map[string]interface{}{"site": "plant-a"},
	}
	decoded, err := subscriber.convertToRecords(payload, TopicMappingRule{Database: "iot"}, []string{"sensors", "line-1"})
	if err != nil {
		t.Fatalf("convert payload: %v", err)
	}
	record := decoded[0].(*models.Record)
	if record.Measurement != "payload_metrics" || record.Tags["site"] != "plant-a" {
		t.Fatalf("payload values changed without a measurement or tag rule: %#v", record)
	}
}

// The boot path itself: startSubscriber must get past target validation for a
// stored row whose only problem is a dead filter. The broker is unreachable,
// so Start fails on connect — the point is that it is reached.
func TestStartSubscriberAcceptsAStoredDeadFilter(t *testing.T) {
	mgr := newTestManager(t)
	sub := &Subscription{
		ID: "legacy-2", Name: "legacy", Broker: "tcp://127.0.0.1:1", ClientID: "c",
		Topics: []string{"sensors/#"}, QoS: 1, Database: "iot",
		ConnectTimeoutSeconds: 1,
		TopicMapping:          TopicMapping{"sensors/temp+x": {Database: "olddb"}},
	}
	sub.SetDefaults()
	err := mgr.startSubscriber(sub)
	if err != nil && strings.Contains(err.Error(), "targets cannot be written") {
		t.Fatalf("startSubscriber refused a dead filter: %v", err)
	}
	if err == nil {
		_ = mgr.StopSubscription(context.Background(), sub.ID)
	}
}
