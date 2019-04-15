package beater

import (
	"github.com/coreos/go-systemd/sdjournal"
	"github.com/elastic/beats/libbeat/common"
	"testing"
)

func TestGivenAJournalEntryWithoutContainerIdAndNoSyslogIdentifierWhenTryingToSendItShouldBeIgnored(t *testing.T) {
	var rawJournalEntry sdjournal.JournalEntry = sdjournal.JournalEntry{}
	var journalEntry common.MapStr = common.MapStr{}
	journalEntry["pid"] = "42"
	var incomingLogEvents chan common.MapStr = make(chan common.MapStr, 1)
	var jb *Journalbeat = createJournalBeatWithIncomingLogEventsChannel(incomingLogEvents)

	jb.sendEvent(journalEntry, &rawJournalEntry)

	select {
	case <-incomingLogEvents:
		t.Errorf("Error! No events should have been sent to 'incomingLogEvents' channel")
	default:
	}
}

// creates a Journalbeat object with the given channel in order to check interactions with it
func createJournalBeatWithIncomingLogEventsChannel(incomingLogEvents chan common.MapStr) *Journalbeat {
	var jb *Journalbeat = &Journalbeat{
		JournalBeatExtension: &JournalBeatExtension{
			metrics:           &JournalBeatMetrics{},
			incomingLogEvents: incomingLogEvents,
		},
	}
	jb.metrics.init(false, "")
	return jb
}
