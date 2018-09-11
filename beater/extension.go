
package beater

import (
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
	"hash/fnv"
	"time"
)

const (
	metricPrefix string = "logging.journalbeat"
	//These are the fields for the container logs.
	containerTagField string = "CONTAINER_TAG"
	containerIdField  string = "CONTAINER_ID"

	//These are the fields for the host process logs.
	tagField     string = "SYSLOG_IDENTIFIER"
	processField string = "_PID"

	//Common fields for both container and host process logs.
	hostNameField  string = "_HOST_NAME"
	messageField   string = "MESSAGE"
	timestampField string = "_SOURCE_REALTIME_TIMESTAMP"
	priorityField  string = "PRIORITY"

	channelSize  int   = 1000
	microseconds int64 = 1000000
)

type LogBuffer struct {
	time     time.Time
	logEvent common.MapStr
	logType  string
}

func hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func getPartition(lb *LogBuffer, numPartitions int) int {
	partition := 0
	if tag, ok := lb.logEvent["container_tag"]; ok {
		// same container - same instance
		// Assuming equal config - if container moves, it should still
		// end up at same logstash instance
		partition = hash(tag.(string)) % numPartitions
	} else if buftype, ok := lb.logEvent["logBufferingType"]; ok {
		// journalbeat does re-assembly based on logBufferingType
		partition = hash(buftype.(string)) % numPartitions
	} else if eventtype, ok := lb.logEvent["type"]; ok {
		partition = hash(eventtype.(string)) % numPartitions
	}
	return partition
}

// "circular shift" a config list
func shiftlist(cfg *common.Config, target *common.Config, key string, shift int) error {
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

func (jb *Journalbeat) flushStaleLogMessages() {
	for logType, logBuffer := range jb.journalTypeOutstandingLogBuffer {
		if time.Now().Sub(logBuffer.time).Seconds() >= jb.config.FlushLogInterval.Seconds() {
			//this message has been sitting in our buffer for more than 30 seconds time to flush it.
			partition := getPartition(logBuffer, jb.numLogstashAvailable)
			jb.logstashClients[partition].PublishEvent(logBuffer.logEvent, publisher.Guaranteed)
			delete(jb.journalTypeOutstandingLogBuffer, logType)
			jb.cursorChan <- logBuffer.logEvent["cursor"].(string)
		}
	}
}

func (jb *Journalbeat) flushOrBufferLogs(event common.MapStr) {
	//check if it starts with space or tab
	newLogMessage := event["message"].(string)
	logType := event["logBufferingType"].(string)

	if newLogMessage != "" && (newLogMessage[0] == ' ' || newLogMessage[0] == '\t') {
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
			partition := getPartition(oldLogBuffer, jb.numLogstashAvailable)
			jb.logstashClients[partition].PublishEvent(oldLogBuffer.logEvent, publisher.Guaranteed)
			//update stats if enabled
			if jb.config.MetricsEnabled {
				jb.logMessagesPublished.Inc(1)
				jb.logMessageDelay.Update(time.Now().Unix() - (event["utcTimestamp"].(int64) / microseconds))
			}
		}
	}
}

// TODO optimize this later but for now walkthru all the different types. Use priority queue/multiple threads if needed.
func (jb *Journalbeat) logProcessor() {
	logp.Info("Started the thread which consumes log messages and publishes them")
	tickChan := time.NewTicker(jb.config.FlushLogInterval)
	for {
		select {
		case <-tickChan.C:
			// here we need to walk through all the map entries and flush out the ones
			// which have been sitting there for some time.
			jb.flushStaleLogMessages()

		case channelEvent := <-jb.incomingLogMessages:
			jb.flushOrBufferLogs(channelEvent)
		}
	}
}