go-metrics-influxdb
===================

This is a reporter for the [go-metrics](https://github.com/rcrowley/go-metrics) library which will post the metrics to [InfluxDB](https://influxdb.com/).

Note
----

This is only compatible with InfluxDB 0.9+.

Usage
-----

```go
import "github.com/vrischmann/go-metrics-influxdb"

go influxdb.InfluxDB(
    metrics.DefaultRegistry, // metrics registry
    time.Second * 10,        // interval
    "http://localhost:8086", // the InfluxDB url
    "mydb",                  // your InfluxDB database
    "myuser",                // your InfluxDB user
    "mypassword",            // your InfluxDB password
)

// recently added - support for tags per metric

type FieldMetadata struct {
    Name  string             `json:"n"` 
    Tags  map[string]string  `json:"t"`
}


fieldMetadata := influxdb.FieldMetadata{Name: "request", Tags: map[string]string{"status-code": strconv.Itoa(rw.StatusCode()), "method": req.Method, "path": uriPath}}
// tag metadata is encoded into the existing 'name' field for posting to influx, as json
meter := metrics.NewMeter()
//registry.GetOrRegister(fieldMetadata.String(), meter)

```

License
-------

go-metrics-influxdb is licensed under the MIT license. See the LICENSE file for details.
