// Copyright 2017 Marcus Heese
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beater

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"time"

	"github.com/coreos/go-systemd/sdjournal"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/medallia/journalbeat/config"
	"github.com/medallia/journalbeat/journal"
)

type LogBuffer struct {
	time     time.Time
	logEvent common.MapStr
	logType  string
}

const (
	//These are the fields for the container logs.
	containerTagField       string = "CONTAINER_TAG"
	containerIdField        string = "CONTAINER_ID"
	containerTimestampField string = "_SOURCE_REALTIME_TIMESTAMP"

	//These are the fields for the host process logs.
	tagField       string = "SYSLOG_IDENTIFIER"
	processField   string = "_PID"
	timestampField string = "@timestamp"

	//Common fields for both container and host process logs.
	hostNameField string = "_HOST_NAME"
	messageField  string = "MESSAGE"

	channelSize int = 1000
)

//intentional decision to move everyone to this format.
var validRegexMultilineContinuation =
	regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3} (?P<LogLine>.*)`)

// Journalbeat is the main Journalbeat struct
type Journalbeat struct {
	done   chan struct{}
	config config.Config
	client publisher.Client

	journal *sdjournal.Journal

	cursorChan chan string

	journalTypeOutstandingLogBuffer map[string]*LogBuffer
	incomingLogMessages             chan common.MapStr
}

func (jb *Journalbeat) initJournal() error {
	var err error

	seekToHelper := func(position string, err error) error {
		if err == nil {
			logp.Info("Seek to %s successful", position)
		} else {
			logp.Warn("Could not seek to %s: %v", position, err)
		}
		return err
	}

	// connect to the Systemd Journal
	if jb.journal, err = sdjournal.NewJournal(); err != nil {
		return err
	}

	// add specific units to monitor if any
	for _, unit := range jb.config.Units {
		if err = jb.journal.AddMatch(sdjournal.SD_JOURNAL_FIELD_SYSTEMD_UNIT + "=" + unit); err != nil {
			return fmt.Errorf("Filtering unit %s failed: %v", unit, err)
		}
	}

	// seek position
	position := jb.config.SeekPosition
	// try seekToCursor first, if that is requested
	if position == config.SeekPositionCursor {
		if cursor, err := ioutil.ReadFile(jb.config.CursorStateFile); err != nil {
			logp.Warn("Could not seek to cursor: reading cursor state file failed: %v", err)
		} else {
			// try to seek to cursor and if successful return
			if err = seekToHelper(config.SeekPositionCursor, jb.journal.SeekCursor(string(cursor))); err == nil {
				return nil
			}
		}

		if jb.config.CursorSeekFallback == config.SeekPositionDefault {
			return err
		}

		position = jb.config.CursorSeekFallback
	}

	switch position {
	case config.SeekPositionHead:
		err = seekToHelper(config.SeekPositionHead, jb.journal.SeekHead())
	case config.SeekPositionTail:
		err = seekToHelper(config.SeekPositionTail, jb.journal.SeekTail())
	}

	if err != nil {
		return fmt.Errorf("Seeking to a good position in journal failed: %v", err)
	}

	return nil
}

// WriteCursorLoop runs the loop which flushes the current cursor position to a file
func (jb *Journalbeat) writeCursorLoop() {
	var cursor string
	saveCursorState := func(cursor string) {
		if cursor != "" {
			if err := ioutil.WriteFile(jb.config.CursorStateFile, []byte(cursor), 0644); err != nil {
				logp.Err("Could not write to cursor state file: %v", err)
			}
		}
	}

	// save cursor for the last time when stop signal caught
	// Saving the cursor through defer guarantees that the jb.cursorChan has been fully consumed
	// and we are writing the cursor of the last message published.
	defer func() { saveCursorState(cursor) }()

	tick := time.Tick(jb.config.CursorFlushPeriod)

	for cursor = range jb.cursorChan {
		select {
		case <-tick:
			saveCursorState(cursor)
		default:
		}
	}
}

// New creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	var err error
	if err = cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	jb := &Journalbeat{
		done:                            make(chan struct{}),
		config:                          config,
		cursorChan:                      make(chan string),
		incomingLogMessages:             make(chan common.MapStr, channelSize),
		journalTypeOutstandingLogBuffer: make(map[string]*LogBuffer),
	}

	if err = jb.initJournal(); err != nil {
		logp.Err("Failed to connect to the Systemd Journal: %v", err)
		return nil, err
	}

	return jb, nil
}

func (jb *Journalbeat) flushStaleLogMessages() {
	for logType, logBuffer := range jb.journalTypeOutstandingLogBuffer {
		if time.Now().Sub(logBuffer.time).Seconds() >= jb.config.FlushLogInterval.Seconds() {
			//this message has been sitting in our buffer for more than 30 seconds time to flush it.
			jb.client.PublishEvent(logBuffer.logEvent, publisher.Guaranteed)
			delete(jb.journalTypeOutstandingLogBuffer, logType)
			jb.cursorChan <- logBuffer.logEvent["cursor"].(string)
		}
	}
}

func (jb *Journalbeat) flushOrBufferLogs(event common.MapStr) {
	//check if it starts with space or tab
	newLogMessage := event["message"].(string)
	logType := event["logBufferingType"].(string)
	//intentional decision to move everyone to this format.
	submatches := validRegexMultilineContinuation.FindStringSubmatch(newLogMessage)
	if len(submatches) == 2 {
		newLogMessage = submatches[1]
		//this is a continuation of previous line
		if oldLog, found := jb.journalTypeOutstandingLogBuffer[logType]; found {
			jb.journalTypeOutstandingLogBuffer[logType].logEvent["message"] =
				oldLog.logEvent["message"].(string) + "\n" + newLogMessage
		} else {
			jb.journalTypeOutstandingLogBuffer[logType] = &LogBuffer{
				time:     time.Now(),
				logType:  event["logBufferingType"].(string),
				logEvent: event,
			}
		}
		jb.journalTypeOutstandingLogBuffer[logType].time = time.Now()
	} else {
		oldLogBuffer, found := jb.journalTypeOutstandingLogBuffer[logType]
		jb.journalTypeOutstandingLogBuffer[logType] = &LogBuffer{
			time:     time.Now(),
			logType:  event["logBufferingType"].(string),
			logEvent: event,
		}
		if found {
			//flush the older logs to async.
			jb.client.PublishEvent(oldLogBuffer.logEvent, publisher.Guaranteed)
		}
	}
}

//TODO optimize this later but for now walkthru all the different types. Use priority queue/multiple threads if needed.
func (jb *Journalbeat) logProcessor() {
	logp.Info("Started the thread which consumes log messages and publishes it")
	tickChan := time.NewTicker(jb.config.FlushLogInterval)
	for {
		select {
		case <-tickChan.C:
			//here we need to walk thru all the map entries and flush out the ones
			//which have been sitting there for some time.
			jb.flushStaleLogMessages()

		case channelEvent := <-jb.incomingLogMessages:
			jb.flushOrBufferLogs(channelEvent)
		}
	}
}

// Run is the main event loop: read from journald and pass it to Publish
func (jb *Journalbeat) Run(b *beat.Beat) error {
	logp.Info("Journalbeat is running!")
	defer func() {
		close(jb.cursorChan)
		jb.client.Close()
		jb.journal.Close()
	}()

	if jb.config.WriteCursorState {
		go jb.writeCursorLoop()
	}

	go jb.logProcessor()

	jb.client = b.Publisher.Connect()

	commonFields := []string{hostNameField, messageField}

	for rawEvent := range journal.Follow(jb.journal, jb.done) {
		event := common.MapStr{}
		if _, ok := rawEvent.Fields[containerIdField]; ok {
			selectedFields := append(commonFields, []string{containerTimestampField, containerTagField, containerIdField}...)
			event = MapStrFromJournalEntry(
				rawEvent,
				jb.config.CleanFieldNames,
				jb.config.ConvertToNumbers,
				jb.config.MoveMetadataLocation,
				selectedFields)
			event["type"] = "container"
			event["@timestamp"] = rawEvent.Fields[containerTimestampField]
			event["logBufferingType"] = rawEvent.Fields[containerIdField]
		} else {
			selectedFields := append(commonFields, []string{tagField, processField, timestampField}...)
			event = MapStrFromJournalEntry(
				rawEvent,
				jb.config.CleanFieldNames,
				jb.config.ConvertToNumbers,
				jb.config.MoveMetadataLocation,
				selectedFields)
			event["type"] = rawEvent.Fields[tagField]
			event["logBufferingType"] = rawEvent.Fields[processField]
		}

		event["input_type"] = jb.config.DefaultType
		event["cursor"] = rawEvent.Cursor

		jb.incomingLogMessages <- event
	}
	return nil
}

// Stop stops Journalbeat execution
func (jb *Journalbeat) Stop() {
	logp.Info("Stopping Journalbeat")
	close(jb.done)
}
