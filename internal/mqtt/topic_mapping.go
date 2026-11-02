package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/basekick-labs/arc/internal/ingest"
)

// TopicMapping keeps the original topic-to-database shorthand while allowing
// a rule object to set a database, measurement and tags extracted from a topic.
type TopicMapping map[string]TopicMappingRule

// TopicMappingRule controls how messages matching one MQTT topic filter are
// routed and named.
type TopicMappingRule struct {
	Database      string               `json:"database,omitempty"`
	Measurement   string               `json:"measurement,omitempty"`
	TagsFromTopic []TopicTagExtraction `json:"tags_from_topic,omitempty"`
}

// UnmarshalJSON accepts the legacy string form as a database-only rule.
func (r *TopicMappingRule) UnmarshalJSON(data []byte) error {
	value := bytes.TrimSpace(data)
	if len(value) == 0 {
		return errors.New("topic mapping value cannot be empty")
	}
	if value[0] == '"' {
		var database string
		if err := json.Unmarshal(value, &database); err != nil {
			return err
		}
		*r = TopicMappingRule{Database: database}
		return nil
	}
	if value[0] != '{' {
		return errors.New("topic mapping value must be a database string or rule object")
	}
	// Unknown keys are refused: a misspelt "tag_from_topic" would otherwise
	// leave a database-only rule and every tag silently unwritten.
	type ruleAlias TopicMappingRule
	var decoded ruleAlias
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&decoded); err != nil {
		return fmt.Errorf("topic mapping rule: %w", err)
	}
	*r = TopicMappingRule(decoded)
	return nil
}

// MarshalJSON writes database-only rules in the old string form, so existing
// subscriptions keep their stored and returned JSON shape.
func (r TopicMappingRule) MarshalJSON() ([]byte, error) {
	if r.Database != "" && r.Measurement == "" && len(r.TagsFromTopic) == 0 {
		return json.Marshal(r.Database)
	}
	type ruleAlias TopicMappingRule
	return json.Marshal(ruleAlias(r))
}

// TopicTagExtraction selects a zero-based segment from the full topic name.
type TopicTagExtraction struct {
	Position int    `json:"position"`
	TagName  string `json:"tag_name"`
}

// UnmarshalJSON requires both fields; position zero is valid and cannot by
// itself distinguish an omitted position from an explicit one.
func (e *TopicTagExtraction) UnmarshalJSON(data []byte) error {
	var raw struct {
		Position *int    `json:"position"`
		TagName  *string `json:"tag_name"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("tags_from_topic: %w", err)
	}
	if raw.Position == nil {
		return errors.New("tags_from_topic position is required")
	}
	if raw.TagName == nil {
		return errors.New("tags_from_topic tag_name is required")
	}
	e.Position = *raw.Position
	e.TagName = *raw.TagName
	return nil
}

type compiledTopicRule struct {
	filter        string
	levels        []string
	rule          TopicMappingRule
	literalLevels int
	plusLevels    int
	hashLevels    int
}

type compiledTopicMapping struct {
	exact     map[string]TopicMappingRule
	wildcards []compiledTopicRule
}

// validateTopicMapping checks every rule's targets, and with strictFilters
// every key as an MQTT topic filter too (see validateSubscriptionTargets).
func validateTopicMapping(mapping TopicMapping, strictFilters bool) error {
	filters := make([]string, 0, len(mapping))
	for filter := range mapping {
		filters = append(filters, filter)
	}
	sort.Strings(filters)
	for _, filter := range filters {
		if strictFilters {
			if err := validateTopicFilter(filter); err != nil {
				return err
			}
		}
		if err := validateTopicRule(filter, mapping[filter]); err != nil {
			return err
		}
	}
	return nil
}

type deadTopicFilter struct {
	filter string
	reason string
}

// deadTopicFilters lists the mapping keys that are not valid MQTT topic
// filters. compileTopicMapping leaves them out: no published topic can match
// one (#479).
func deadTopicFilters(mapping TopicMapping) []deadTopicFilter {
	var dead []deadTopicFilter
	for filter := range mapping {
		if err := validateTopicFilter(filter); err != nil {
			dead = append(dead, deadTopicFilter{filter: filter, reason: err.Error()})
		}
	}
	sort.Slice(dead, func(i, j int) bool { return dead[i].filter < dead[j].filter })
	return dead
}

// compileTopicMapping validates and orders rules once when a subscriber starts.
// More literal levels, then more single-level wildcards, take precedence over
// broader filters. The filter text breaks any remaining ties deterministically.
func compileTopicMapping(mapping TopicMapping) (compiledTopicMapping, error) {
	filters := make([]string, 0, len(mapping))
	for filter := range mapping {
		filters = append(filters, filter)
	}
	sort.Strings(filters)
	compiled := compiledTopicMapping{
		exact:     make(map[string]TopicMappingRule, len(mapping)),
		wildcards: make([]compiledTopicRule, 0, len(mapping)),
	}
	for _, filter := range filters {
		rule := mapping[filter]
		if validateTopicFilter(filter) != nil {
			continue // dead: no topic can match it (deadTopicFilters)
		}
		if err := validateTopicRule(filter, rule); err != nil {
			return compiledTopicMapping{}, err
		}
		if !strings.ContainsAny(filter, "+#") {
			compiled.exact[filter] = rule
			continue
		}
		wildcard := compiledTopicRule{
			filter: filter,
			levels: strings.Split(filter, "/"),
			rule:   rule,
		}
		for _, level := range wildcard.levels {
			switch level {
			case "+":
				wildcard.plusLevels++
			case "#":
				wildcard.hashLevels++
			default:
				wildcard.literalLevels++
			}
		}
		compiled.wildcards = append(compiled.wildcards, wildcard)
	}
	sort.Slice(compiled.wildcards, func(i, j int) bool {
		left, right := compiled.wildcards[i], compiled.wildcards[j]
		if left.literalLevels != right.literalLevels {
			return left.literalLevels > right.literalLevels
		}
		if left.plusLevels != right.plusLevels {
			return left.plusLevels > right.plusLevels
		}
		if left.hashLevels != right.hashLevels {
			return left.hashLevels < right.hashLevels
		}
		return left.filter < right.filter
	})
	return compiled, nil
}

func validateTopicFilter(filter string) error {
	if filter == "" {
		return errors.New("topic mapping filter cannot be empty")
	}
	if len(filter) > MaxTopicLength {
		return fmt.Errorf("topic mapping filter exceeds %d characters", MaxTopicLength)
	}
	if strings.IndexByte(filter, 0) >= 0 {
		return errors.New("topic mapping filter cannot contain a null character")
	}
	levels := strings.Split(filter, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || i != len(levels)-1) {
			return fmt.Errorf("topic mapping filter %q has an invalid # wildcard", filter)
		}
		if strings.Contains(level, "+") && level != "+" {
			return fmt.Errorf("topic mapping filter %q has an invalid + wildcard", filter)
		}
	}
	return nil
}

func validateTopicRule(filter string, rule TopicMappingRule) error {
	if rule.Database == "" && rule.Measurement == "" && len(rule.TagsFromTopic) == 0 {
		return fmt.Errorf("topic mapping filter %q must set database, measurement, or tags_from_topic", filter)
	}
	if rule.Database != "" && !isValidStorageSegment(rule.Database) {
		return fmt.Errorf("topic mapping %q targets database %q, which must start with a letter and contain only alphanumeric characters, underscores, or hyphens", filter, rule.Database)
	}
	if rule.Measurement != "" && !isValidMeasurementSegment(rule.Measurement) {
		return fmt.Errorf("topic mapping %q measurement %q must start with a letter and contain only alphanumeric characters, underscores, or hyphens (at most 128 characters)", filter, rule.Measurement)
	}
	// A filter without a trailing # matches only topics with exactly its
	// number of levels, so a position past them can never hold a value: a
	// typo that would silently write no tag. Under a trailing # the topic may
	// be longer, and a missing segment is skipped per message.
	levels := strings.Split(filter, "/")
	maxPosition := -1
	if levels[len(levels)-1] != "#" {
		maxPosition = len(levels) - 1
	}
	seenTagNames := make(map[string]string, len(rule.TagsFromTopic))
	for _, extraction := range rule.TagsFromTopic {
		if extraction.Position < 0 {
			return fmt.Errorf("topic mapping %q has a negative tags_from_topic position", filter)
		}
		if maxPosition >= 0 && extraction.Position > maxPosition {
			return fmt.Errorf("topic mapping %q tags_from_topic position %d is past the filter's last level (%d; positions start at 0)", filter, extraction.Position, maxPosition)
		}
		if extraction.TagName == "" {
			return fmt.Errorf("topic mapping %q has an empty tags_from_topic tag_name", filter)
		}
		if problem := ingest.ColumnNameProblem(extraction.TagName); problem != "" {
			return fmt.Errorf("topic mapping %q tag_name %q cannot be stored: %s", filter, extraction.TagName, problem)
		}
		folded := ingest.FoldColumnName(extraction.TagName)
		if previous, exists := seenTagNames[folded]; exists {
			return fmt.Errorf("topic mapping %q repeats tag names %q and %q", filter, previous, extraction.TagName)
		}
		seenTagNames[folded] = extraction.TagName
	}
	return nil
}

func matchTopicMapping(mapping compiledTopicMapping, topic string) (TopicMappingRule, []string, bool) {
	if topic == "" || strings.IndexByte(topic, 0) >= 0 {
		return TopicMappingRule{}, nil, false
	}
	if rule, ok := mapping.exact[topic]; ok {
		var topicLevels []string
		if len(rule.TagsFromTopic) > 0 {
			topicLevels = strings.Split(topic, "/")
		}
		return rule, topicLevels, true
	}
	if len(mapping.wildcards) == 0 {
		return TopicMappingRule{}, nil, false
	}
	topicLevels := strings.Split(topic, "/")
	for _, rule := range mapping.wildcards {
		if topicFilterMatches(rule.levels, topicLevels) {
			return rule.rule, topicLevels, true
		}
	}
	return TopicMappingRule{}, nil, false
}

func topicFilterMatches(filterLevels, topicLevels []string) bool {
	if len(filterLevels) == 0 || len(topicLevels) == 0 {
		return false
	}
	if strings.HasPrefix(topicLevels[0], "$") && (filterLevels[0] == "+" || filterLevels[0] == "#") {
		return false
	}
	for i, filterLevel := range filterLevels {
		if filterLevel == "#" {
			return i == len(filterLevels)-1
		}
		if i >= len(topicLevels) {
			return false
		}
		if filterLevel != "+" && filterLevel != topicLevels[i] {
			return false
		}
	}
	return len(filterLevels) == len(topicLevels)
}
