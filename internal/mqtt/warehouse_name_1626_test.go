package mqtt

import (
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
)

// A subscription cannot write into the Iceberg warehouse directory, as its database or through
// a topic_mapping rule (#1626).
func TestSubscriptionRefusesWarehouseDatabase_Issue1626(t *testing.T) {
	base := func() *CreateSubscriptionRequest {
		return &CreateSubscriptionRequest{
			Name: "s", Broker: "tcp://localhost:1883", ClientID: "c",
			Topics: []string{"sensors/#"}, Database: "lv_wh",
		}
	}
	if err := ValidateCreateRequest(base()); err != nil {
		t.Fatalf("no warehouse name set: %v", err)
	}
	storage.SetWarehouseRootName("lv_wh")
	t.Cleanup(func() { storage.SetWarehouseRootName("") })

	if err := ValidateCreateRequest(base()); err == nil || !strings.Contains(err.Error(), "Iceberg warehouse") {
		t.Errorf("database lv_wh: %v, want the warehouse refusal", err)
	}
	mapped := base()
	mapped.Database = "iot"
	mapped.TopicMapping = TopicMapping{"sensors/a": {Database: "lv_wh"}}
	if err := ValidateCreateRequest(mapped); err == nil || !strings.Contains(err.Error(), "Iceberg warehouse") {
		t.Errorf("topic_mapping into lv_wh: %v, want the warehouse refusal", err)
	}
	ok := base()
	ok.Database = "lv_wh-other"
	if err := ValidateCreateRequest(ok); err != nil {
		t.Errorf("lv_wh-other: %v", err)
	}
}
