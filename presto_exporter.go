package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

type Presto struct {
	Cluster
	Memory
	Query
}

type Cluster struct {
	RunningQueries int `json:"runningQueries"`
	BlockedQueries int `json:"blockedQueries"`
	QueuedQueries int `json:"queuedQueries"`
	ActiveWorkers int `json:"activeWorkers"`
	RunningDrivers int `json:"runningDrivers"`
	RunningTasks int `json:"runningTasks"`
	ReservedMemory float64 `json:"reservedMemory"`
	TotalInputRows int `json:"totalInputRows"`
	TotalInputBytes int `json:"totalInputBytes"`
	TotalCPUTimeSecs int `json:"totalCpuTimeSecs"`
	AdjustedQueueSize int `json:"adjustedQueueSize"`
}

type Memory struct {
	Reserved struct {
		MaxBytes int64 `json:"maxBytes"`
		ReservedBytes int `json:"reservedBytes"`
		ReservedRevocableBytes int `json:"reservedRevocableBytes"`
		QueryMemoryReservations struct {
		} `json:"queryMemoryReservations"`
		QueryMemoryAllocations struct {
		} `json:"queryMemoryAllocations"`
		QueryMemoryRevocableReservations struct {
		} `json:"queryMemoryRevocableReservations"`
		FreeBytes int64 `json:"freeBytes"`
	} `json:"reserved"`
	General struct {
		MaxBytes int `json:"maxBytes"`
		ReservedBytes int `json:"reservedBytes"`
		ReservedRevocableBytes int `json:"reservedRevocableBytes"`
		QueryMemoryReservations struct {
		} `json:"queryMemoryReservations"`
		QueryMemoryAllocations struct {
		} `json:"queryMemoryAllocations"`
		QueryMemoryRevocableReservations struct {
		} `json:"queryMemoryRevocableReservations"`
		FreeBytes int `json:"freeBytes"`
	} `json:"general"`
}


type Query []struct {
	QueryID string `json:"queryId"`
	Session struct {
		QueryID string `json:"queryId"`
		TransactionID string `json:"transactionId"`
		ClientTransactionSupport bool `json:"clientTransactionSupport"`
		User string `json:"user"`
		Source string `json:"source"`
		Catalog string `json:"catalog"`
		TimeZoneKey int `json:"timeZoneKey"`
		Locale string `json:"locale"`
		RemoteUserAddress string `json:"remoteUserAddress"`
		UserAgent string `json:"userAgent"`
		ClientTags []interface{} `json:"clientTags"`
		ResourceEstimates struct {
		} `json:"resourceEstimates"`
		StartTime int64 `json:"startTime"`
		SystemProperties struct {
		} `json:"systemProperties"`
		CatalogProperties struct {
		} `json:"catalogProperties"`
		UnprocessedCatalogProperties struct {
		} `json:"unprocessedCatalogProperties"`
		Roles struct {
		} `json:"roles"`
		PreparedStatements struct {
		} `json:"preparedStatements"`
		SessionFunctions struct {
		} `json:"sessionFunctions"`
	} `json:"session"`
	ResourceGroupID []string `json:"resourceGroupId"`
	State string `json:"state"`
	MemoryPool string `json:"memoryPool"`
	Scheduled bool `json:"scheduled"`
	Self string `json:"self"`
	Query string `json:"query"`
	QueryStats struct {
		CreateTime string `json:"createTime"`
		EndTime string `json:"endTime"`
		WaitingForPrerequisitesTime string `json:"waitingForPrerequisitesTime"`
		QueuedTime string `json:"queuedTime"`
		ElapsedTime string `json:"elapsedTime"`
		ExecutionTime string `json:"executionTime"`
		RunningTasks int `json:"runningTasks"`
		TotalDrivers int `json:"totalDrivers"`
		QueuedDrivers int `json:"queuedDrivers"`
		RunningDrivers int `json:"runningDrivers"`
		CompletedDrivers int `json:"completedDrivers"`
		RawInputDataSize string `json:"rawInputDataSize"`
		RawInputPositions int `json:"rawInputPositions"`
		CumulativeUserMemory float64 `json:"cumulativeUserMemory"`
		CumulativeTotalMemory float64 `json:"cumulativeTotalMemory"`
		UserMemoryReservation string `json:"userMemoryReservation"`
		TotalMemoryReservation string `json:"totalMemoryReservation"`
		PeakUserMemoryReservation string `json:"peakUserMemoryReservation"`
		PeakTotalMemoryReservation string `json:"peakTotalMemoryReservation"`
		PeakTaskTotalMemoryReservation string `json:"peakTaskTotalMemoryReservation"`
		TotalCPUTime string `json:"totalCpuTime"`
		TotalScheduledTime string `json:"totalScheduledTime"`
		FullyBlocked bool `json:"fullyBlocked"`
		BlockedReasons []interface{} `json:"blockedReasons"`
		TotalAllocation string `json:"totalAllocation"`
		PeakNodeTotalMemorReservation string `json:"peakNodeTotalMemorReservation"`
	} `json:"queryStats"`
	ErrorType string `json:"errorType,omitempty"`
	ErrorCode struct {
		Code int `json:"code"`
		Name string `json:"name"`
		Type string `json:"type"`
		Retriable bool `json:"retriable"`
	} `json:"errorCode,omitempty"`
	FailureInfo struct {
		Type string `json:"type"`
		Message string `json:"message"`
		Suppressed []interface{} `json:"suppressed"`
		Stack []string `json:"stack"`
		ErrorLocation struct {
			LineNumber int `json:"lineNumber"`
			ColumnNumber int `json:"columnNumber"`
		} `json:"errorLocation"`
		ErrorCode struct {
			Code int `json:"code"`
			Name string `json:"name"`
			Type string `json:"type"`
			Retriable bool `json:"retriable"`
		} `json:"errorCode"`
	} `json:"failureInfo,omitempty"`
	QueryType string `json:"queryType"`
	Warnings []interface{} `json:"warnings"`
}


type Exporter struct {
	URI string
	clusterMetrics ,memoryMetrics,queryMetrics map[string]*prometheus.Desc
}

func newClusterMetrics(metricName string,docString string,labels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName("presto","cluster",metricName),docString,labels,nil)
}

func newMemMetrics(subName string,metricName string,docString string,labels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName("presto_memory",subName,metricName),docString,labels,nil)
}

func newQueryMetrics(subName string,metricName string,docString string,labels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName("presto_query",subName,metricName),docString,labels,nil)
}

func NewExporter(uri string) *Exporter  {
	return &Exporter{
		URI:            uri,
		clusterMetrics: map[string]*prometheus.Desc{
			"runningQueries": newClusterMetrics("running_queries","",[]string{}),
			"blockedQueries": newClusterMetrics("blocked_queries","",[]string{}),
			"queuedQueries": newClusterMetrics("queued_queries","",[]string{}),
			"activeWorkers": newClusterMetrics("active_workers","",[]string{}),
			"runningDrivers": newClusterMetrics("running_drivers","",[]string{}),
			"reservedMemory": newClusterMetrics("reserved_memory","",[]string{}),
			"totalInputRows": newClusterMetrics("totalInput_rows","",[]string{}),
			"totalInputBytes": newClusterMetrics("totalInput_bytes","",[]string{}),
			"adjustedQueueSize": newClusterMetrics("adjusted_queue_size","",[]string{}),
		},
		memoryMetrics: map[string]*prometheus.Desc{
			"rMaxBytes": newMemMetrics("reserved","max_bytes","",[]string{}),
			"rReservedBytes": newMemMetrics("reserved","reserved_bytes","",[]string{}),
			"rReservedRevocableBytes": newMemMetrics("reserved","reserved_revocable_bytes","",[]string{}),
			//"RQueryMemoryReservations": newMemMetrics("reserved","query_memory_reservations","",[]string{}),
			//"RQueryMemoryAllocations": newMemMetrics("reserved","query_memory_allocations","",[]string{}),
			//"RQueryMemoryRevocableReservations": newMemMetrics("reserved","query_memory_revocable_reservations","",[]string{}),
			"rFreeBytes": newMemMetrics("reserved","free_bytes","",[]string{}),
			"gMaxBytes": newMemMetrics("general","max_bytes","",[]string{}),
			"gReservedBytes": newMemMetrics("general","reserved_bytes","",[]string{}),
			"gReservedRevocableBytes": newMemMetrics("general","reserved_revocable_bytes","",[]string{}),
			"gFreeBytes": newMemMetrics("general","free_bytes","",[]string{}),
		},
		queryMetrics: map[string]*prometheus.Desc{
			"code": newQueryMetrics("error","code","",
				[]string{"name","type","query_type","message","query","self","user","source","query_id","catalog","remoteUserAddress",
					"transactionId","createTime","endTime","total_cpu_time","totalDrivers","waitingForPrerequisitesTime","queuedTime",
				"elapsedTime","executionTime"}),
		},
	}
}

var (
	listenAddress       = flag.String("web.listen_address", ":9016", "Address on which to expose metrics and web interface.")
	metricsPath         = flag.String("web.telemetry_path", "/metrics", "Path under which to expose metrics.")
	prestoURI      = flag.String("presto.presto_uri", "http://10.70.70.92:8080", "presto URI.")
	prestoScrapeTimeout = flag.Int("hadoop.scrape_timeout", 2, "The number of seconds to wait for an HTTP response from the presto")
	insecure            = flag.Bool("insecure", false, "Ignore server certificate if using https")
)
 

func fetchHTTP(uri string, timeout time.Duration) func() (io.ReadCloser, error) {
	http.DefaultClient.Timeout = timeout
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: *insecure}

	return func() (io.ReadCloser, error) {
		resp, err := http.DefaultClient.Get(uri)
		if err != nil {
			return nil, err
		}
		if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP status %d", resp.StatusCode)
		}
		return resp.Body, nil
	}
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range e.clusterMetrics {
		ch <- m
	}
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	var cluster  Presto
	var memory Presto
	var query Presto
	clusterData, err := fetchData(cluster, e.URI,"/v1/cluster")
	if err != nil {
		log.Println("fetch clusterData error:",err)
	}
	ch <- prometheus.MustNewConstMetric(e.clusterMetrics["runningQueries"],prometheus.GaugeValue,float64(clusterData.RunningQueries))
	ch <- prometheus.MustNewConstMetric(e.clusterMetrics["blockedQueries"],prometheus.GaugeValue,float64(clusterData.BlockedQueries))
	ch <- prometheus.MustNewConstMetric(e.clusterMetrics["queuedQueries"],prometheus.GaugeValue,float64(clusterData.QueuedQueries))
	ch <- prometheus.MustNewConstMetric(e.clusterMetrics["activeWorkers"],prometheus.GaugeValue,float64(clusterData.ActiveWorkers))
	ch <- prometheus.MustNewConstMetric(e.clusterMetrics["runningDrivers"],prometheus.GaugeValue,float64(clusterData.RunningDrivers))
	ch <- prometheus.MustNewConstMetric(e.clusterMetrics["reservedMemory"],prometheus.GaugeValue,clusterData.ReservedMemory)
	ch <- prometheus.MustNewConstMetric(e.clusterMetrics["totalInputRows"],prometheus.GaugeValue,float64(clusterData.TotalInputRows))
	ch <- prometheus.MustNewConstMetric(e.clusterMetrics["totalInputBytes"],prometheus.GaugeValue,float64(clusterData.TotalInputBytes))
	ch <- prometheus.MustNewConstMetric(e.clusterMetrics["adjustedQueueSize"],prometheus.GaugeValue,float64(clusterData.AdjustedQueueSize))
	memData, err := fetchData(memory, e.URI,"/v1/cluster/memory")
	if err != nil {
		log.Println("fetch memeData error:",err)
	}
	ch <- prometheus.MustNewConstMetric(e.memoryMetrics["rMaxBytes"],prometheus.GaugeValue,float64(memData.Memory.Reserved.MaxBytes))
	ch <- prometheus.MustNewConstMetric(e.memoryMetrics["rReservedBytes"],prometheus.GaugeValue,float64(memData.Memory.Reserved.ReservedBytes))
	ch <- prometheus.MustNewConstMetric(e.memoryMetrics["rReservedRevocableBytes"],prometheus.GaugeValue,float64(memData.Memory.Reserved.ReservedRevocableBytes))
	ch <- prometheus.MustNewConstMetric(e.memoryMetrics["rFreeBytes"],prometheus.GaugeValue,float64(memData.Memory.Reserved.FreeBytes))
	ch <- prometheus.MustNewConstMetric(e.memoryMetrics["gMaxBytes"],prometheus.GaugeValue,float64(memData.Memory.Reserved.MaxBytes))
	ch <- prometheus.MustNewConstMetric(e.memoryMetrics["gReservedBytes"],prometheus.GaugeValue,float64(memData.Memory.Reserved.ReservedBytes))
	ch <- prometheus.MustNewConstMetric(e.memoryMetrics["gReservedRevocableBytes"],prometheus.GaugeValue,float64(memData.Memory.Reserved.ReservedRevocableBytes))
	ch <- prometheus.MustNewConstMetric(e.memoryMetrics["gFreeBytes"],prometheus.GaugeValue,float64(memData.Memory.Reserved.FreeBytes))
	queryData, err := fetchQData(query.Query, e.URI,"/v1/query")
	if err != nil {
		log.Println("fetch queryData error:",err)
	}
	for _,q:= range queryData{
		ch <- prometheus.MustNewConstMetric(e.queryMetrics["code"],prometheus.GaugeValue,float64(q.ErrorCode.Code),
			q.ErrorCode.Name,q.ErrorCode.Type,q.QueryType,q.FailureInfo.Message,q.Query,q.Self,q.Session.User,q.Session.Source,
			q.QueryID,q.Session.Catalog,q.Session.RemoteUserAddress,q.Session.TransactionID,q.QueryStats.CreateTime,q.QueryStats.EndTime,
			q.QueryStats.TotalCPUTime,strconv.Itoa(q.QueryStats.TotalDrivers),q.QueryStats.WaitingForPrerequisitesTime,q.QueryStats.QueuedTime,
			q.QueryStats.ElapsedTime,q.QueryStats.ExecutionTime)

	}

}

func fetchData(s Presto,u string,path string)  (Presto,error)  {
	body, err := fetchHTTP(u+path, time.Duration(*prestoScrapeTimeout)*time.Second)()
	if err != nil {
		return Presto{}, err
	}
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return Presto{}, err
	}
	err = json.Unmarshal(data, &s)
	if err != nil {
		return Presto{}, err
	}
	body.Close()
	return s,nil
}

func fetchQData(s Query,u string,path string)  (Query,error)  {
	body, err := fetchHTTP(u+path, time.Duration(*prestoScrapeTimeout)*time.Second)()
	if err != nil {
		return Query{}, err
	}
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return Query{}, err
	}
	err = json.Unmarshal(data, &s)
	if err != nil {
		return Query{}, err
	}
	body.Close()
	return s,nil
}

func main() {
	flag.Parse()
	log.Printf("Build context %s", version.BuildContext())

	exporter := NewExporter(*prestoURI)
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Presto Exporter</title></head>
			<body>
			<h1>Presto Exporter</h1>
			<p><a href="` + *metricsPath + `">Metrics</a></p>
			</body>
			</html>`))
	})

	log.Fatal(http.ListenAndServe(*listenAddress, nil))

}