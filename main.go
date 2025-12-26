package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gorilla/mux"
)

type Config struct {
	Endpoint     string              `json:"endpoint"`
	AccessKey    string              `json:"access_key"`
	AccessSecret string              `json:"access_secret"`
	Project      string              `json:"project"`
	Logstore     string              `json:"logstore"`
	PathPrefix   map[string][]string `json:"path_prefix"`
	HTTPListen   string              `json:"http_listen"`
}

type HostStat struct {
	Host          string
	Request       int64
	Request20X    int64
	RequestLength int64
	RequestTime   float64
	Paths         map[string]*PathStat
}

type PathStat struct {
	Path          string
	Request       int64
	Request20X    int64
	RequestLength int64
	RequestTime   float64
}

var (
	cfg       Config
	client    sls.ClientInterface
	hostStats = make(map[string]*HostStat)
	mu        sync.RWMutex
)

func main() {
	loadConfig()

	client = sls.CreateNormalInterface(
		cfg.Endpoint,
		cfg.AccessKey,
		cfg.AccessSecret,
		cfg.Project,
	)

	startPuller()
	startHTTP()
}

func loadConfig() {
	b, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatal(err)
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Fatal(err)
	}
}

func startPuller() {
	shards, err := client.ListShards(cfg.Project, cfg.Logstore)
	if err != nil {
		log.Fatal(err)
	}

	for _, s := range shards {
		go pullShard(s.ShardID)
	}
}

func pullShard(shardID int) {
	cursor, err := client.GetCursor(
		cfg.Project,
		cfg.Logstore,
		shardID,
		"end", // 关键：每次启动从最新开始
	)
	if err != nil {
		log.Println("get cursor error:", err)
		return
	}

	for {
		logGroupList, nextCursor, err := client.PullLogs(
			cfg.Project,
			cfg.Logstore,
			shardID,
			cursor,
			"",
			100,
		)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		processLogGroups(logGroupList)
		cursor = nextCursor

		if logGroupList == nil || len(logGroupList.LogGroups) == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func processLogGroups(lgList *sls.LogGroupList) {
	if lgList == nil {
		return
	}

	mu.Lock()
	defer mu.Unlock()

	for _, lg := range lgList.LogGroups {
		for _, l := range lg.Logs {
			fields := make(map[string]string)
			for _, c := range l.Contents {
				if c.Key != nil && c.Value != nil {
					fields[*c.Key] = *c.Value
				}
			}

			host := fields["host"]
			if host == "" {
				continue
			}

			status, _ := strconv.Atoi(fields["status"])
			reqLen, _ := strconv.ParseInt(fields["request_length"], 10, 64)
			reqTime, _ := strconv.ParseFloat(fields["request_time"], 64)
			uri := fields["request_uri"]

			h := getHost(host)

			h.Request++
			h.RequestLength += reqLen
			h.RequestTime += reqTime
			if status >= 200 && status < 300 {
				h.Request20X++
			}

			for _, p := range cfg.PathPrefix[host] {
				if strings.HasPrefix(uri, p) {
					ps := getPath(h, p)
					ps.Request++
					ps.RequestLength += reqLen
					ps.RequestTime += reqTime
					if status >= 200 && status < 300 {
						ps.Request20X++
					}
				}
			}
		}
	}
}

func getHost(host string) *HostStat {
	if h, ok := hostStats[host]; ok {
		return h
	}
	h := &HostStat{
		Host:  host,
		Paths: make(map[string]*PathStat),
	}
	hostStats[host] = h
	return h
}

func getPath(h *HostStat, p string) *PathStat {
	if ps, ok := h.Paths[p]; ok {
		return ps
	}
	ps := &PathStat{Path: p}
	h.Paths[p] = ps
	return ps
}

func startHTTP() {
	r := mux.NewRouter()
	r.HandleFunc("/hosts", getHosts)
	r.HandleFunc("/hosts/{host}/{path}", getHostPath)

	log.Println("HTTP listen on", cfg.HTTPListen)
	log.Fatal(http.ListenAndServe(cfg.HTTPListen, r))
}

func getHosts(w http.ResponseWriter, _ *http.Request) {
	mu.RLock()
	defer mu.RUnlock()

	var res []map[string]interface{}
	for _, h := range hostStats {
		avg := 0.0
		if h.Request > 0 {
			avg = h.RequestTime / float64(h.Request)
		}
		res = append(res, map[string]interface{}{
			"host":             h.Host,
			"request":          h.Request,
			"request_20X":      h.Request20X,
			"request_length":   h.RequestLength,
			"request_time":     h.RequestTime,
			"avg_request_time": avg,
		})
	}
	json.NewEncoder(w).Encode(res)
}

func getHostPath(w http.ResponseWriter, r *http.Request) {
	v := mux.Vars(r)
	host := v["host"]
	path := "/" + v["path"]

	mu.RLock()
	defer mu.RUnlock()

	h, ok := hostStats[host]
	if !ok {
		http.NotFound(w, r)
		return
	}
	ps, ok := h.Paths[path]
	if !ok {
		http.NotFound(w, r)
		return
	}

	avg := 0.0
	if ps.Request > 0 {
		avg = ps.RequestTime / float64(ps.Request)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"host":             host,
		"path":             path,
		"request":          ps.Request,
		"request_20X":      ps.Request20X,
		"request_length":   ps.RequestLength,
		"request_time":     ps.RequestTime,
		"avg_request_time": avg,
	})
}
