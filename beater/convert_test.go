package beater

import (
	"github.com/coreos/go-systemd/sdjournal"
	"github.com/elastic/beats/libbeat/common"
	"testing"
)

func TestGivenAJournalEntryWithABooleanStringWhenConvertingThenItShouldNotBeCastedToBool(t *testing.T) {
	var journalEntryFields map[string]string = make(map[string]string)
	journalEntryFields["message"] = "true"
	var journalEntry sdjournal.JournalEntry = sdjournal.JournalEntry{Fields: journalEntryFields}

	var mappedJournalEntry common.MapStr = MapStrFromJournalEntry(&journalEntry, false, false, "")

	if mappedJournalEntry["message"] != "true" {
		t.Errorf("Error! 'message' field shoud not have been casted")
	}
}
