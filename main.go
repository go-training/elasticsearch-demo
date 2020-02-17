package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/tidwall/gjson"
)

var (
	_       = fmt.Print
	count   int
	batch   int
	bucket  string
	address string
)

func init() {
	flag.IntVar(&count, "count", 10000, "Number of documents to generate")
	flag.IntVar(&batch, "batch", 0, "Number of documents to send in one batch")
	flag.StringVar(&bucket, "bucket", "test", "Index")
	flag.StringVar(&address, "address", "http://127.0.0.1:9200", "server address")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())
}

// WindowsLog
type WindowsLog struct {
	Hostname     string
	EventCode    string
	Kernel       string
	ComputerName string
	RecordID     string

	AuthenticationPackageName string
	IpAddress                 string
	LmPackageName             string
	LogonProcessName          string
	LogonType                 string
	ProcessId                 string
	SubjectUserSid            string
	TargetDomainName          string
	TargetUserName            string
	TargetUserSid             string
	WorkstationName           string

	FailureReason     string
	CommandLine       string
	NewProcessName    string
	SubjectUserName   string
	SubjectDomainName string

	ObjectName     string
	ObjectServer   string
	ObjectType     string
	ProcessName    string
	ShareLocalPath string
}

func main() {
	log.SetFlags(0)
	cfg := elasticsearch.Config{
		Addresses: []string{
			address,
		},
	}

	var (
		r map[string]interface{}
		// wg sync.WaitGroup
	)

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// 1. Get cluster info
	//
	res, err := es.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()
	// Check response status
	if res.IsError() {
		log.Fatalf("Error: %s", res.String())
	}
	// Deserialize the response into a map.
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print client and server version numbers.
	log.Printf("Client: %s", elasticsearch.Version)
	log.Printf("Server: %s", r["version"].(map[string]interface{})["number"])
	log.Println(strings.Repeat("~", 37))

	// 2. Index documents concurrently
	//
	// for i, title := range []string{"Test One", "Test Two"} {
	// 	wg.Add(1)

	// 	go func(i int, title string) {
	// 		defer wg.Done()

	// 		// Build the request body.
	// 		var b strings.Builder
	// 		b.WriteString(`{"title" : "`)
	// 		b.WriteString(title)
	// 		b.WriteString(`"}`)

	// 		// Set up the request object.
	// 		req := esapi.IndexRequest{
	// 			Index:      bucket,
	// 			DocumentID: strconv.Itoa(i + 1),
	// 			Body:       strings.NewReader(b.String()),
	// 			Refresh:    "true",
	// 		}

	// 		// Perform the request with the client.
	// 		res, err := req.Do(context.Background(), es)
	// 		if err != nil {
	// 			log.Fatalf("Error getting response: %s", err)
	// 		}
	// 		defer res.Body.Close()

	// 		if res.IsError() {
	// 			log.Printf("[%s] Error indexing document ID=%d", res.Status(), i+1)
	// 		} else {
	// 			// Deserialize the response into a map.
	// 			var r map[string]interface{}
	// 			if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
	// 				log.Printf("Error parsing the response body: %s", err)
	// 			} else {
	// 				// Print the response status and indexed document version.
	// 				log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
	// 			}
	// 		}
	// 	}(i, title)
	// }
	// wg.Wait()

	log.Println(strings.Repeat("-", 37))

	// 3. Search for the indexed documents
	//
	// Build the request body.
	var buf bytes.Buffer
	hosts := []string{
		// "MSTARVS4",
		// "MTKSCMP61",
		// "YMY-SVR1",
		// "mstarbkcv01",
		// "mstarbkcv04",
		// "mstarbkcv05",
		// "mstarbkcv06",
		// "mstarvstp01",
		"pedigree-svr1",
		// "proj-sps",

		// "YMY-AP1",
		// "MTKRTDT01",
		// "mslab793506990",
		// "mstarbkcv02",
		// "mstarbkcv03",
		// "mstardp01",
		"mstarhv01",
		// "mstarsus",
		// "mtkppc01",
	}

	for _, host := range hosts {
		start := batch
		num := 0
		// Print the ID and document source for each hit.
		events := []*WindowsLog{}
		for {
			log.Println("size:", count)
			log.Println("from:", start)
			query := map[string]interface{}{
				"size": count,
				"from": start,
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{
							{
								"bool": map[string]interface{}{
									"minimum_should_match": 1,
									"should": []map[string]interface{}{
										map[string]interface{}{
											"match_phrase": map[string]interface{}{
												"event.code": "4688",
											},
										},
										map[string]interface{}{
											"match_phrase": map[string]interface{}{
												"event.code": "4624",
											},
										},
										map[string]interface{}{
											"match_phrase": map[string]interface{}{
												"event.code": "4625",
											},
										},
										map[string]interface{}{
											"match_phrase": map[string]interface{}{
												"event.code": "5140",
											},
										},
										map[string]interface{}{
											"match_phrase": map[string]interface{}{
												"event.code": "4663",
											},
										},
									},
								},
							},
							{
								"bool": map[string]interface{}{
									"minimum_should_match": 1,
									"should": []map[string]interface{}{
										map[string]interface{}{
											"match_phrase": map[string]interface{}{
												"host.name": host,
											},
										},
									},
								},
							},
						},
					},
				},
			}
			if err := json.NewEncoder(&buf).Encode(query); err != nil {
				log.Fatalf("Error encoding query: %s", err)
			}

			// Perform the search request.
			res, err = es.Search(
				es.Search.WithContext(context.Background()),
				es.Search.WithIndex("auditbeat-2019.09.*", "auditbeat-2019.10.*"),
				es.Search.WithBody(&buf),
				es.Search.WithTrackTotalHits(true),
				es.Search.WithPretty(),
			)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer res.Body.Close()

			if res.IsError() {
				var e map[string]interface{}
				if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
					log.Fatalf("Error parsing the response body: %s", err)
				} else {
					// Print the response status and error information.
					log.Fatalf("[%s] %s: %s",
						res.Status(),
						e["error"].(map[string]interface{})["type"],
						e["error"].(map[string]interface{})["reason"],
					)
				}
			}

			if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
				log.Fatalf("Error parsing the response body: %s", err)
			}
			// Print the response status, number of results, and request duration.
			log.Printf(
				"[%s] %d hits; took: %dms; host: %s",
				res.Status(),
				int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
				int(r["took"].(float64)),
				host,
			)

			num = int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))

			log.Printf(
				"start: %d, num: %d; host: %s",
				start,
				num,
				host,
			)
			for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
				output, _ := json.Marshal(hit.(map[string]interface{})["_source"])
				// log.Printf(" * ID=%s, %v, %v", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"].(map[string]interface{})["event"].(map[string]interface{})["code"].(float64), gjson.Get(string(output), "host.name").String())

				event := &WindowsLog{
					EventCode:    gjson.Get(string(output), "event.code").String(),
					Hostname:     gjson.Get(string(output), "host.name").String(),
					Kernel:       gjson.Get(string(output), "host.os.kernel").String(),
					ComputerName: gjson.Get(string(output), "winlog.computer_name").String(),
					RecordID:     gjson.Get(string(output), "winlog.record_id").String(),

					AuthenticationPackageName: gjson.Get(string(output), "winlog.event_data.AuthenticationPackageName").String(),
					IpAddress:                 gjson.Get(string(output), "winlog.event_data.IpAddress").String(),
					LmPackageName:             gjson.Get(string(output), "winlog.event_data.LmPackageName").String(),
					LogonProcessName:          gjson.Get(string(output), "winlog.event_data.LogonProcessName").String(),
					LogonType:                 gjson.Get(string(output), "winlog.event_data.LogonType").String(),
					ProcessId:                 gjson.Get(string(output), "winlog.event_data.ProcessId").String(),
					SubjectUserSid:            gjson.Get(string(output), "winlog.event_data.SubjectUserSid").String(),
					TargetDomainName:          gjson.Get(string(output), "winlog.event_data.TargetDomainName").String(),
					TargetUserName:            gjson.Get(string(output), "winlog.event_data.TargetUserName").String(),
					TargetUserSid:             gjson.Get(string(output), "winlog.event_data.TargetUserSid").String(),
					WorkstationName:           gjson.Get(string(output), "winlog.event_data.WorkstationName").String(),

					FailureReason:     gjson.Get(string(output), "winlog.event_data.FailureReason").String(),
					CommandLine:       gjson.Get(string(output), "winlog.event_data.CommandLine").String(),
					NewProcessName:    gjson.Get(string(output), "winlog.event_data.NewProcessName").String(),
					SubjectUserName:   gjson.Get(string(output), "winlog.event_data.SubjectUserName").String(),
					SubjectDomainName: gjson.Get(string(output), "winlog.event_data.SubjectDomainName").String(),

					ObjectName:     gjson.Get(string(output), "winlog.event_data.ObjectName").String(),
					ObjectServer:   gjson.Get(string(output), "winlog.event_data.ObjectServer").String(),
					ObjectType:     gjson.Get(string(output), "winlog.event_data.ObjectType").String(),
					ProcessName:    gjson.Get(string(output), "winlog.event_data.ProcessName").String(),
					ShareLocalPath: gjson.Get(string(output), "winlog.event_data.ShareLocalPath").String(),
				}
				events = append(events, event)
			}

			log.Println(strings.Repeat("=", 37))

			output, _ := json.Marshal(events)

			// To start, here's how to dump a string (or just
			// bytes) into a file.
			if err := ioutil.WriteFile("log/"+host+"_output_"+strconv.Itoa(start)+".json", output, 0644); err != nil {
				log.Fatalf("can't write the file: %s", err)
			}

			if start < num {
				start += count
			}

			if start > num {
				break
			}
		}
	}
}
