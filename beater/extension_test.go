package beater

import (
	"github.com/coreos/go-systemd/sdjournal"
	"github.com/elastic/beats/libbeat/common"
	"testing"
)

func TestGivenAJournalEntryWithoutContainerIdAndSyslogIdentifierWhenSendingItShouldBeIgnored(t *testing.T) {
	var journalEntry common.MapStr = common.MapStr{}
	var rawJournalEntry sdjournal.JournalEntry = sdjournal.JournalEntry{}
	var incomingLogEvents chan common.MapStr = make(chan common.MapStr, 1)
	journalEntry["pid"] = 42
	journalEntry["syslog_identifier"] = nil
	var jb *Journalbeat = &Journalbeat{
		JournalBeatExtension: &JournalBeatExtension{
			metrics:           &JournalBeatMetrics{},
			incomingLogEvents: incomingLogEvents,
		},
	}
	jb.metrics.init(false, "")

	jb.sendEvent(journalEntry, &rawJournalEntry)

	select {
	case <-incomingLogEvents:
		t.Errorf("Error! No events should have been sent to 'incomingLogEvents' channel")
	default:
	}
}

func TestGivenAJournalEntryWithABooleanStringWhenConvertingThenItShouldNotBeCastedToBool(t *testing.T) {
	var journalEntryFields map[string]string = make(map[string]string)
	journalEntryFields["message"] = "true"
	var journalEntry sdjournal.JournalEntry = sdjournal.JournalEntry{Fields: journalEntryFields}

	var mappedJournalEntry common.MapStr = MapStrFromJournalEntry(&journalEntry, false, false, "")

	if mappedJournalEntry["message"] != "true" {
		t.Errorf("Error! 'message' field shoud not have been casted")
	}
}
