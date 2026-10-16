package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func objectKeyWithLength(firstSegment string, totalLength int) string {
	middleSegment := strings.Repeat("x", 150)
	lastSegmentLength := totalLength - len(firstSegment) - 4*len(middleSegment) - 5
	return strings.Join([]string{
		firstSegment,
		middleSegment,
		middleSegment,
		middleSegment,
		middleSegment,
		strings.Repeat("z", lastSegmentLength),
	}, "/")
}

func prefixedListingFixtures(t *testing.T, prefix string) (string, string) {
	t.Helper()
	if len(prefix) != 201 {
		t.Fatalf("prefix length = %d, want 201", len(prefix))
	}
	valid := objectKeyWithLength("valid", MaxUsableKeyLen-len(prefix))
	over := objectKeyWithLength("over", MaxUsableKeyLen+1-len(prefix))
	if got := len(prefix + valid); got != MaxUsableKeyLen {
		t.Fatalf("at-limit object name length = %d, want %d", got, MaxUsableKeyLen)
	}
	if got := len(prefix + over); got != MaxUsableKeyLen+1 {
		t.Fatalf("over-limit object name length = %d, want %d", got, MaxUsableKeyLen+1)
	}
	return valid, over
}

func assertPrefixedListingContract(t *testing.T, list, listObjects []string, hasValid, hasOver bool, unusable []UnusableObject, valid, over string) {
	t.Helper()
	if len(list) != 1 || list[0] != valid {
		t.Errorf("List = %q, want only the at-limit key %q", list, valid)
	}
	if len(listObjects) != 1 || listObjects[0] != valid {
		t.Errorf("ListObjects = %q, want only the at-limit key %q", listObjects, valid)
	}
	if !hasValid {
		t.Error("HasObjectsUnderPrefix(valid/) = false, want true")
	}
	if hasOver {
		t.Error("HasObjectsUnderPrefix(over/) = true, want false for an unaddressable object")
	}
	if len(unusable) != 1 {
		t.Fatalf("ListUnusable returned %d entries, want the over-limit object", len(unusable))
	}
	if unusable[0].Path != over {
		t.Errorf("ListUnusable path = %q, want relative spelling %q", unusable[0].Path, over)
	}
	if !errors.Is(unusable[0].Err, ErrInvalidPath) {
		t.Errorf("ListUnusable error = %v, want it to wrap ErrInvalidPath", unusable[0].Err)
	}
}

func TestS3ListingsValidateCompletePrefixedObjectNames(t *testing.T) {
	ctx := context.Background()
	prefix := strings.Repeat("p", 200) + "/"
	valid, over := prefixedListingFixtures(t, prefix)
	stub := newS3Stub(t)
	stub.list = []stubListObject{
		{key: prefix + valid, size: 4},
		{key: prefix + over, size: 8},
	}
	b := stubBackendWithPrefix(t, stub, prefix)

	list, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	objects, err := b.ListObjects(ctx, "")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	var listed []string
	for _, object := range objects {
		listed = append(listed, object.Path)
	}
	hasValid, err := b.HasObjectsUnderPrefix(ctx, "valid/")
	if err != nil {
		t.Fatalf("HasObjectsUnderPrefix(valid/): %v", err)
	}
	hasOver, err := b.HasObjectsUnderPrefix(ctx, "over/")
	if err != nil {
		t.Fatalf("HasObjectsUnderPrefix(over/): %v", err)
	}
	unusable, err := b.ListUnusable(ctx, "")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}
	assertPrefixedListingContract(t, list, listed, hasValid, hasOver, unusable, valid, over)
}

func TestAzureListingsValidateCompletePrefixedObjectNames(t *testing.T) {
	ctx := context.Background()
	prefix := strings.Repeat("p", 200) + "/"
	valid, over := prefixedListingFixtures(t, prefix)
	b, _ := newStubbedAzureBackend(t, prefix, []stubBlob{
		{name: prefix + valid, size: 4},
		{name: prefix + over, size: 8},
	})

	list, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	objects, err := b.ListObjects(ctx, "")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	var listed []string
	for _, object := range objects {
		listed = append(listed, object.Path)
	}
	hasValid, err := b.HasObjectsUnderPrefix(ctx, "valid/")
	if err != nil {
		t.Fatalf("HasObjectsUnderPrefix(valid/): %v", err)
	}
	hasOver, err := b.HasObjectsUnderPrefix(ctx, "over/")
	if err != nil {
		t.Fatalf("HasObjectsUnderPrefix(over/): %v", err)
	}
	unusable, err := b.ListUnusable(ctx, "")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}
	assertPrefixedListingContract(t, list, listed, hasValid, hasOver, unusable, valid, over)
}
