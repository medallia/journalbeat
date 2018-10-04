package beater

import (
	"fmt"
	"github.com/coreos/go-systemd/sdjournal"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/publisher"
	"hash/fnv"
	"os"
	"strconv"
	"time"
)

const (
	// These are the fields for the container logs.
	containerTagField string = "container_tag"
	containerIdField  string = "container_id"

	// These are the fields for the host native process logs.
	tagField       string = "syslog_identifier"
	processIdField string = "pid"

	// Common fields for both container and host process logs.
	hostNameField  string = "host_name"
	messageField   string = "message"
	timestampField string = "source_realtime_timestamp"
	priorityField  string = "priority"
	inputTypeField string = "input_type"

	// Added fields
	utcTimestampField     string = "utcTimestamp"
	cursorField           string = "cursor"
	logBufferingTypeField string = "logBufferingType"

	channelSize  int   = 1000
	microseconds int64 = 1000000
)

type LogBuffer struct {
	time     time.Time
	logEvent common.MapStr
	logType  string
}

type JournalBeatExtension struct {
	metrics *JournalBeatMetrics

	// corresponds to the number of downstream logstash aggregators available at startup.
	publishers        []publisher.Client
	logBuffersByType  map[string]*LogBuffer
	incomingLogEvents chan common.MapStr
	processorDone     chan struct{}
}

func hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

// "circular shift" a config list
func shiftList(cfg *common.Config, target *common.Config, key string, shift int) error {
	count, err := cfg.CountField(key)
	if err != nil {
		return err
	}
	offset := 0
	for n := shift; n < count; n++ {
		item, err := cfg.String(key, n)
		if err != nil {
			return err
		}
		target.SetString(key, offset, item)
		offset++
	}
	for n := 0; n < shift; n++ {
		item, err := cfg.String(key, n)
		if err != nil {
			return err
		}
		target.SetString(key, offset, item)
		offset++
	}
	return nil
}

func (jb *Journalbeat) flushStaleEvents() {
	now := time.Now()
	var lastLogBuffer *LogBuffer
	for logType, logBuffer := range jb.logBuffersByType {
		if now.Sub(logBuffer.time).Seconds() >= jb.config.FlushLogInterval.Seconds() {
			// this message has been sitting in our buffer for more than XX seconds, time to flush it.
			jb.publishEvent(logBuffer)
			delete(jb.logBuffersByType, logType)
			lastLogBuffer = logBuffer
		}
	}
	if lastLogBuffer != nil {
		jb.cursorChan <- lastLogBuffer.logEvent[cursorField].(string)
	}
}

func (jb *Journalbeat) flushOrBufferEvent(event common.MapStr) {
	// check if it starts with space or tab
	newLogMessage := event[messageField].(string)
	logType := event[logBufferingTypeField].(string)
	logBuffer, found := jb.logBuffersByType[logType]

	// we consider this is a continuation of previous line
	isContinuation := newLogMessage != "" && (newLogMessage[0] == ' ' || newLogMessage[0] == '\t')

	if isContinuation && found {
		logBuffer.logEvent[messageField] = logBuffer.logEvent[messageField].(string) + "\n" + newLogMessage
		logBuffer.time = time.Now()
	} else {
		jb.logBuffersByType[logType] = toLogBuffer(event)
		if found {
			// flush the older logs to async.
			jb.publishEvent(logBuffer)
			jb.metrics.logMessageDelay.Update(time.Now().Unix() - (event[utcTimestampField].(int64) / microseconds))
		}
	}
}

func (jbe *JournalBeatExtension) getPublisher(logBuffer *LogBuffer) publisher.Client {
	var partitioningString string
	if tag, ok := logBuffer.logEvent[containerTagField]; ok {
		// same container - same instance
		// Assuming equal config - if container moves, it should still
		// end up at same logstash instance
		partitioningString = tag.(string)
	} else if buftype, ok := logBuffer.logEvent[logBufferingTypeField]; ok {
		// journalbeat does re-assembly based on logBufferingType
		partitioningString = buftype.(string)
	} else if eventtype, ok := logBuffer.logEvent["type"]; ok {
		partitioningString = eventtype.(string)
	}

	numPartitions := len(jbe.publishers)
	partition := hash(partitioningString) % numPartitions
	return jbe.publishers[partition]
}

func (jbe *JournalBeatExtension) publishEvent(logBuffer *LogBuffer) {
	jbe.getPublisher(logBuffer).PublishEvent(logBuffer.logEvent, publisher.Guaranteed)
	jbe.metrics.logMessagesPublished.Inc(1)
}

func toLogBuffer(event common.MapStr) *LogBuffer {
	return &LogBuffer{
		time:     time.Now(),
		logType:  event[logBufferingTypeField].(string),
		logEvent: event,
	}
}

func (jb *Journalbeat) runLogProcessor() {
	logp.Info("Started the thread which consumes log messages and publishes them")
	tickChan := time.NewTicker(jb.config.FlushLogInterval)
	for {
		// TODO optimize this later but for now walk through all the different types. Use priority queue/multiple threads if needed.
		select {
		case <-jb.done:
			close(jb.processorDone)
			return
		case <-tickChan.C:
			// here we need to walk through all the map entries and flush out the ones
			// which have been sitting there for some time.
			jb.flushStaleEvents()

		case channelEvent := <-jb.incomingLogEvents:
			jb.flushOrBufferEvent(channelEvent)
		}
	}
}

type StringSet map[string]bool

func (s1 StringSet) union(s2 StringSet) StringSet {
	result := StringSet{}
	for k := range s1 {
		result[k] = true
	}
	for k := range s2 {
		result[k] = true
	}
	return result
}

var commonFields = StringSet{
	hostNameField:  true,
	messageField:   true,
	priorityField:  true,
	inputTypeField: true,
}

var containerFields = commonFields.union(StringSet{
	containerTagField: true,
	containerIdField:  true,
})

var nativeFields = commonFields.union(StringSet{
	tagField:       true,
	processIdField: true,
})

func (jbe *JournalBeatExtension) sendEvent(event common.MapStr, rawEvent *sdjournal.JournalEntry) {
	var messageSize int64 = 0
	if message, exists := event[messageField]; exists {
		messageSize = int64(len(fmt.Sprintf("%v", message)))
	}
	jbe.metrics.journalMessageSize.Update(messageSize)

	var newEvent common.MapStr
	if containerId, exists := event[containerIdField]; exists {
		newEvent = cloneFields(event, containerFields)
		newEvent["type"] = "container"
		newEvent[logBufferingTypeField] = containerId
		jbe.metrics.journalEntriesContainer.Inc(1)
	} else if processId, exists := event[processIdField]; exists {
		newEvent = cloneFields(event, nativeFields)
		newEvent["type"] = event[tagField]
		newEvent[logBufferingTypeField] = processId
		jbe.metrics.journalEntriesNative.Inc(1)
	} else {
		logp.Info("Unknown type message %s", event)
		jbe.metrics.journalEntriesUnknown.Inc(1)
		return
	}

	newEvent[cursorField] = rawEvent.Cursor

	if tmStr, ok := event[timestampField]; ok {
		if ts, err := strconv.ParseInt(tmStr.(string), 10, 64); err == nil {
			newEvent[utcTimestampField] = ts
		}
	}
	if newEvent[utcTimestampField] == nil {
		newEvent[utcTimestampField] = int64(rawEvent.RealtimeTimestamp)
	}

	jbe.incomingLogEvents <- newEvent
}

func cloneFields(event common.MapStr, fields StringSet) common.MapStr {
	result := common.MapStr{}
	for k, v := range event {
		if fields[k] {
			result[k] = v
		}
	}
	return result
}

func (jbe *JournalBeatExtension) init(b *beat.Beat) error {
	if b.Config.Output["logstash"] == nil {
		// TODO we might want to make this work with other outputs too
		logp.Err("Invalid configuration, logstash output not defined")
		os.Exit(101)
	}

	hostsCount, err := b.Config.Output["logstash"].CountField("hosts")
	if err != nil {
		logp.Err("Invalid configuration for sending contents to logstash")
		os.Exit(101)
	}

	for i := 0; i < hostsCount; i++ {
		newProcessors, err := processors.New(b.Config.Processors)
		if err != nil {
			return fmt.Errorf("error initializing processors: %v", err)
		}

		// override the hosts to pick one of the entries from the original hosts configuration.
		newOutput, err := common.NewConfigFrom(b.Config.Output["logstash"])
		if err != nil {
			return fmt.Errorf("failed to clone output config: %v", err)
		}
		err = shiftList(b.Config.Output["logstash"], newOutput, "hosts", i)
		if err != nil {
			return fmt.Errorf("failed to shift list %v", err)
		}

		newPublisher, err := publisher.New(b.Name, b.Version, map[string]*common.Config{"logstash": newOutput}, b.Config.Shipper, newProcessors)
		if err != nil {
			return fmt.Errorf("error initializing publisher: %v", err)
		}

		jbe.publishers = append(jbe.publishers, newPublisher.Connect())
	}
	return nil
}

func (jbe *JournalBeatExtension) close() {
	for _, client := range jbe.publishers {
		client.Close()
	}
	jbe.metrics.close()
}
