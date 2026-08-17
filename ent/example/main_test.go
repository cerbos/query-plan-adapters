package main

import "testing"

func TestLoadCases(t *testing.T) {
	cases, err := loadCases()
	if err != nil {
		t.Fatal(err)
	}
	if len(cases.Cases) != 10 {
		t.Fatalf("got %d demo cases, want 10", len(cases.Cases))
	}
	if cases.Cases[0].ID != "filtered/alice/view" {
		t.Fatalf("first case is %q", cases.Cases[0].ID)
	}
}
