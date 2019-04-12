package beater

import (
	"testing"
	"github.com/elastic/beats/libbeat/common"
	"github.com/coreos/go-systemd/sdjournal"
)

func TestGivenAJournalEntryWithoutContainerIdAndSyslogIdentifierWhenSendingItShouldBeIgnored(t *testing.T) {
	var journalEntry common.MapStr = common.MapStr{}
	var rawJournalEntry sdjournal.JournalEntry = sdjournal.JournalEntry{}
	var incomingLogEvents chan common.MapStr = make(chan common.MapStr, 1)
	journalEntry["pid"] = 42;
	journalEntry["syslog_identifier"] = nil;
	var jb *Journalbeat = &Journalbeat{
		JournalBeatExtension: &JournalBeatExtension{
			metrics: &JournalBeatMetrics{},
			incomingLogEvents: incomingLogEvents,
		},
	}
	jb.metrics.init(false, "")

	jb.sendEvent(journalEntry, &rawJournalEntry)

	select {
	case <-incomingLogEvents:
		t.Errorf("Error! no events should have been sent to 'incomingLogEvents' channel")
	default:
	}
}
