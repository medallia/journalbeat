package beater

import (
	"fmt"
	"github.com/coreos/go-systemd/sdjournal"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/f0ster/go-metrics-influxdb"
	"github.com/rcrowley/go-metrics"
	"github.com/wavefronthq/go-metrics-wavefront"
	"hash/fnv"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	metricPrefix string = "logging.journalbeat."

	// These are the fields for the container logs.
	containerTagField string = "container_tag"
	containerIdField  string = "container_id"

	// These are the fields for the host native process logs.
	tagField     string = "syslog_identifier"
	processField string = "pid"

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
	// corresponds to the number of downstream logstash aggregators available at startup.
	numLogstashAvailable            int
	logstashClients                 []publisher.Client
	journalTypeOutstandingLogBuffer map[string]*LogBuffer
	incomingLogEvents               chan common.MapStr

	logMessagesPublished metrics.Counter
	logMessageDelay      metrics.Gauge
}

func hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func getPartition(logBuffer *LogBuffer, numPartitions int) int {
	var partition int
	if tag, ok := logBuffer.logEvent[containerTagField]; ok {
		// same container - same instance
		// Assuming equal config - if container moves, it should still
		// end up at same logstash instance
		partition = hash(tag.(string)) % numPartitions
	} else if buftype, ok := logBuffer.logEvent[logBufferingTypeField]; ok {
		// journalbeat does re-assembly based on logBufferingType
		partition = hash(buftype.(string)) % numPartitions
	} else if eventtype, ok := logBuffer.logEvent["type"]; ok {
		partition = hash(eventtype.(string)) % numPartitions
	}
	return partition
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

func (jb *Journalbeat) flushStaleLogMessages() {
	for logType, logBuffer := range jb.journalTypeOutstandingLogBuffer {
		if time.Now().Sub(logBuffer.time).Seconds() >= jb.config.FlushLogInterval.Seconds() {
			// this message has been sitting in our buffer for more than XX seconds, time to flush it.
			jb.publishEvent(logBuffer)
			delete(jb.journalTypeOutstandingLogBuffer, logType)
			jb.cursorChan <- logBuffer.logEvent[cursorField].(string)
		}
	}
}

func (jb *Journalbeat) flushOrBufferLogs(event common.MapStr) {
	// check if it starts with space or tab
	newLogMessage := event[messageField].(string)
	logType := event[logBufferingTypeField].(string)

	if newLogMessage != "" && (newLogMessage[0] == ' ' || newLogMessage[0] == '\t') {
		// we consider this is a continuation of previous line
		if oldLog, found := jb.journalTypeOutstandingLogBuffer[logType]; found {
			jb.journalTypeOutstandingLogBuffer[logType].logEvent[messageField] =
				oldLog.logEvent[messageField].(string) + "\n" + newLogMessage
		} else {
			jb.journalTypeOutstandingLogBuffer[logType] = toLogBuffer(event)
		}
		jb.journalTypeOutstandingLogBuffer[logType].time = time.Now()
	} else {
		oldLogBuffer, found := jb.journalTypeOutstandingLogBuffer[logType]
		jb.journalTypeOutstandingLogBuffer[logType] = toLogBuffer(event)
		if found {
			// flush the older logs to async.
			jb.publishEvent(oldLogBuffer)
			// update stats if enabled
			if jb.config.MetricsEnabled {
				jb.logMessagesPublished.Inc(1)
				jb.logMessageDelay.Update(time.Now().Unix() - (event[utcTimestampField].(int64) / microseconds))
			}
		}
	}
}

func (jbe *JournalBeatExtension) publishEvent(logBuffer *LogBuffer) {
	partition := getPartition(logBuffer, jbe.numLogstashAvailable)
	jbe.logstashClients[partition].PublishEvent(logBuffer.logEvent, publisher.Guaranteed)
}

func toLogBuffer(event common.MapStr) *LogBuffer {
	return &LogBuffer{
		time:     time.Now(),
		logType:  event[logBufferingTypeField].(string),
		logEvent: event,
	}
}

// TODO optimize this later but for now walk through all the different types. Use priority queue/multiple threads if needed.
func (jb *Journalbeat) logProcessor() {
	logp.Info("Started the thread which consumes log messages and publishes them")
	tickChan := time.NewTicker(jb.config.FlushLogInterval)
	for {
		select {
		case <-tickChan.C:
			// here we need to walk through all the map entries and flush out the ones
			// which have been sitting there for some time.
			jb.flushStaleLogMessages()

		case channelEvent := <-jb.incomingLogEvents:
			jb.flushOrBufferLogs(channelEvent)
		}
	}
}

func (jb *Journalbeat) startMetricsReporters() {
	if jb.config.MetricsEnabled {
		registry := metrics.NewPrefixedRegistry(metricPrefix)
		jb.logMessageDelay = metrics.NewRegisteredGauge("MessageConsumptionDelay", registry)
		jb.logMessagesPublished = metrics.NewRegisteredCounter("MessagesPublished", registry)

		if jb.config.WavefrontCollector != "" {
			logp.Info("Wavefront metrics are enabled. Sending to " + jb.config.WavefrontCollector)
			addr, err := net.ResolveTCPAddr("tcp", jb.config.WavefrontCollector)
			if jb.config.WavefrontCollector != "" && err == nil {
				logp.Info("Metrics address parsed")

				// make sure the configuration is sane.

				hostname, err := os.Hostname()
				if err == nil {
					jb.config.MetricTags["source"] = hostname
				}

				wfConfig := wavefront.WavefrontConfig{
					Addr:          addr,
					Registry:      registry,
					FlushInterval: jb.config.MetricsInterval,
					DurationUnit:  time.Nanosecond,
					HostTags:      jb.config.MetricTags,
					Percentiles:   []float64{0.5, 0.75, 0.95, 0.99, 0.999},
				}

				// validate if we can emit metrics to wavefront.
				if err = wavefront.WavefrontOnce(wfConfig); err != nil {
					logp.Err("Metrics collection for log processing on this host failed at boot time: %v", err)
				}

				go wavefront.WavefrontWithConfig(wfConfig)
			} else {
				logp.Err("Cannot parse the IP address of wavefront address " + jb.config.WavefrontCollector)
			}
		}

		if jb.config.InfluxDBURL != "" {
			logp.Info("InfluxDB metrics are enabled. Sending to " + jb.config.InfluxDBURL)
			go influxdb.InfluxDB(
				registry,                  // metrics registry
				jb.config.MetricsInterval, // interval
				jb.config.InfluxDBURL,     // the InfluxDB url
				jb.config.InfluxDatabase,  // your InfluxDB database
				"",                        // your InfluxDB user
				"",                        // your InfluxDB password
			)
		}
	}
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
	tagField:     true,
	processField: true,
})

func (jbe *JournalBeatExtension) sendEvent(event common.MapStr, rawEvent *sdjournal.JournalEntry) {
	var newEvent common.MapStr
	if containerId, exists := event[containerIdField]; exists {
		newEvent = cloneFields(event, containerFields)
		newEvent["type"] = "container"
		newEvent[logBufferingTypeField] = containerId
	} else {
		newEvent = cloneFields(event, nativeFields)
		newEvent["type"] = event[tagField]
		newEvent[logBufferingTypeField] = event[processField]
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

func cloneFields(event common.MapStr, fields StringSet) common.MapStr {
	result := common.MapStr{}
	for k, v := range event {
		if fields[k] {
			result[k] = v
		}
	}
	return result
}

func (jbe *JournalBeatExtension) processConfig(b *beat.Beat) error {
	var err error

	if b.Config.Output["logstash"] == nil {
		// TODO we might want to make this work with other outputs too
		logp.Err("Invalid configuration, logstash output not defined")
		os.Exit(101)
	}

	if jbe.numLogstashAvailable, err = b.Config.Output["logstash"].CountField("hosts"); err != nil {
		logp.Err("Invalid configuration for sending contents to logstash")
		os.Exit(101)
	}

	for i := 0; i < jbe.numLogstashAvailable; i++ {
		newProcessors, err := processors.New(b.Config.Processors)
		if err != nil {
			return fmt.Errorf("error initializing processors: %v", err)
		}

		// override the hosts to pick one of the entries from the original hosts configuration.
		newOutput, err := common.NewConfigFrom(b.Config.Output["logstash"])
		if err != nil {
			return fmt.Errorf("Failed to clone output config: %v", err)
		}
		err = shiftList(b.Config.Output["logstash"], newOutput, "hosts", i)
		if err != nil {
			return fmt.Errorf("Failed to shift list %v", err)
		}

		newPublisher, err := publisher.New(b.Name, b.Version, map[string]*common.Config{"logstash": newOutput}, b.Config.Shipper, newProcessors)
		if err != nil {
			return fmt.Errorf("error initializing publisher: %v", err)
		}

		jbe.logstashClients = append(jbe.logstashClients, newPublisher.Connect())
	}
	return nil
}

func (jbe *JournalBeatExtension) close() {
	for i := 0; i < jbe.numLogstashAvailable; i++ {
		jbe.logstashClients[i].Close()
	}
	logp.Info("journalbeat extension stopped")
}
