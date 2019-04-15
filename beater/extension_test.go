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
