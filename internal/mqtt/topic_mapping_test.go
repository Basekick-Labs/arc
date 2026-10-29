package mqtt

import (
	"encoding/json"
	"reflect"
	"testing"

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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := compileTopicMapping(tt.mapping)
			if (err != nil) != tt.wantErr {
				t.Fatalf("compileTopicMapping() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
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
			{Position: 9, TagName: "missing"},
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
