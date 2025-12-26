package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	Request4XX    int64
	Request5XX    int64
	RequestLength int64
	RequestTime   float64
	Paths         map[string]*PathStat
	mu            sync.RWMutex
}

type PathStat struct {
	Path          string
	Request       int64
	Request20X    int64
	Request4XX    int64
	Request5XX    int64
	RequestLength int64
	RequestTime   float64
}

type StatResult struct {
	Host           string              `json:"host,omitempty"`
	Path           string              `json:"path,omitempty"`
	Request        int64               `json:"request"`
	Request20X     int64               `json:"request_20X"`
	Request4XX     int64               `json:"request_4XX"`
	Request5XX     int64               `json:"request_5XX"`
	RequestLength  int64               `json:"request_length"`
	RequestTime    float64             `json:"request_time"`
	AvgRequestTime float64             `json:"avg_request_time"`
	AvgRequestSize float64             `json:"avg_request_size"`
	SuccessRate    float64             `json:"success_rate"`
	Paths          map[string]PathStat `json:"paths,omitempty"`
}

var (
	cfg            Config
	client         sls.ClientInterface
	hostStats      = make(map[string]*HostStat)
	mu             sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	totalProcessed int64
	startTime      time.Time
)

func main() {
	log.Println("========================================")
	log.Println("Starting log statistics service...")
	log.Println("========================================")

	loadConfig()
	startTime = time.Now()

	// 打印配置信息（隐藏敏感信息）
	log.Printf("Configuration loaded:")
	log.Printf("  Endpoint: %s", cfg.Endpoint)
	log.Printf("  AccessKey: %s***%s", cfg.AccessKey[:min(4, len(cfg.AccessKey))], cfg.AccessKey[max(0, len(cfg.AccessKey)-4):])
	log.Printf("  Project: %s", cfg.Project)
	log.Printf("  Logstore: %s", cfg.Logstore)
	log.Printf("  HTTP Listen: %s", cfg.HTTPListen)
	log.Printf("  Path Prefixes: %d hosts configured", len(cfg.PathPrefix))
	for host, paths := range cfg.PathPrefix {
		log.Printf("    - %s: %v", host, paths)
	}

	log.Println("Creating SLS client...")
	client = sls.CreateNormalInterface(
		cfg.Endpoint,
		cfg.AccessKey,
		cfg.AccessSecret,
		cfg.Project,
	)
	log.Println("SLS client created successfully")

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	startPuller()
	startHTTP()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func loadConfig() {
	log.Println("Loading configuration from config.json...")
	b, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	log.Printf("Config file size: %d bytes", len(b))

	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Fatal("Failed to parse config:", err)
	}

	// 验证必填字段并打印详细信息
	if cfg.Endpoint == "" {
		log.Fatal("Config error: endpoint is empty")
	}
	log.Printf("  endpoint length: %d chars", len(cfg.Endpoint))

	if cfg.AccessKey == "" {
		log.Fatal("Config error: access_key is empty")
	}
	log.Printf("  access_key length: %d chars", len(cfg.AccessKey))
	log.Printf("  access_key has spaces: %v", strings.Contains(cfg.AccessKey, " "))
	log.Printf("  access_key has newlines: %v", strings.Contains(cfg.AccessKey, "\n"))

	if cfg.AccessSecret == "" {
		log.Fatal("Config error: access_secret is empty")
	}
	log.Printf("  access_secret length: %d chars", len(cfg.AccessSecret))
	log.Printf("  access_secret has spaces: %v", strings.Contains(cfg.AccessSecret, " "))
	log.Printf("  access_secret has newlines: %v", strings.Contains(cfg.AccessSecret, "\n"))

	if cfg.Project == "" {
		log.Fatal("Config error: project is empty")
	}
	log.Printf("  project length: %d chars", len(cfg.Project))

	if cfg.Logstore == "" {
		log.Fatal("Config error: logstore is empty")
	}
	log.Printf("  logstore length: %d chars", len(cfg.Logstore))

	if cfg.HTTPListen == "" {
		cfg.HTTPListen = ":8080" // 默认端口
		log.Println("Using default HTTP listen address: :8080")
	}

	// 自动清理空格和换行符
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.AccessKey = strings.TrimSpace(cfg.AccessKey)
	cfg.AccessSecret = strings.TrimSpace(cfg.AccessSecret)
	cfg.Project = strings.TrimSpace(cfg.Project)
	cfg.Logstore = strings.TrimSpace(cfg.Logstore)

	log.Println("Configuration validated and trimmed successfully")
}

func startPuller() {
	log.Println("Listing shards from SLS...")
	log.Printf("  Project: %s", cfg.Project)
	log.Printf("  Logstore: %s", cfg.Logstore)

	shards, err := client.ListShards(cfg.Project, cfg.Logstore)
	if err != nil {
		log.Printf("Failed to list shards: %+v", err)
		log.Fatal("Please check your credentials and project/logstore configuration")
	}

	log.Printf("Found %d shards, starting pullers...", len(shards))
	for i, s := range shards {
		log.Printf("  Shard %d: ID=%d, Status=%s", i, s.ShardID, s.Status)
		go pullShard(s.ShardID)
	}
	log.Println("All shard pullers started")
}

func pullShard(shardID int) {
	log.Printf("[Shard %d] Initializing...", shardID)

	// 关键：每次启动从最新位置开始，避免重复处理历史数据
	log.Printf("[Shard %d] Getting cursor from 'end' position...", shardID)
	cursor, err := client.GetCursor(
		cfg.Project,
		cfg.Logstore,
		shardID,
		"end", // 从最新位置开始
	)
	if err != nil {
		log.Printf("[Shard %d] Failed to get cursor: %+v", shardID, err)
		return
	}

	log.Printf("[Shard %d] Started successfully, cursor: %s", shardID, cursor)

	backoff := time.Second
	maxBackoff := 30 * time.Second
	consecutiveErrors := 0
	lastLogTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Shard %d] Stopped by context", shardID)
			return
		default:
		}

		logGroupList, nextCursor, err := client.PullLogs(
			cfg.Project,
			cfg.Logstore,
			shardID,
			cursor,
			"",
			1000, // 每次拉取更多日志，提高效率
		)
		if err != nil {
			consecutiveErrors++
			log.Printf("[Shard %d] Pull error (consecutive: %d): %+v", shardID, consecutiveErrors, err)

			// 错误过多时延长等待时间
			if consecutiveErrors > 10 {
				backoff = maxBackoff
			}

			time.Sleep(backoff)
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}

		// 成功后重置
		if consecutiveErrors > 0 {
			log.Printf("[Shard %d] Recovered from errors", shardID)
		}
		consecutiveErrors = 0
		backoff = time.Second

		if logGroupList != nil && len(logGroupList.LogGroups) > 0 {
			count := processLogGroups(logGroupList)
			if count > 0 {
				// 每分钟最多打印一次日志，避免刷屏
				if time.Since(lastLogTime) > time.Minute {
					log.Printf("[Shard %d] Processed %d logs, total: %d",
						shardID, count, atomic.LoadInt64(&totalProcessed))
					lastLogTime = time.Now()
				}
			}
		}

		cursor = nextCursor

		// 没有新数据时短暂休眠
		if logGroupList == nil || len(logGroupList.LogGroups) == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func processLogGroups(lgList *sls.LogGroupList) int {
	if lgList == nil {
		return 0
	}

	processed := 0
	skipped := 0

	for _, lg := range lgList.LogGroups {
		for _, l := range lg.Logs {
			// 解析日志字段
			fields := make(map[string]string)
			for _, c := range l.Contents {
				if c.Key != nil && c.Value != nil {
					fields[*c.Key] = *c.Value
				}
			}

			host := fields["host"]
			if host == "" {
				skipped++
				continue
			}

			status, _ := strconv.Atoi(fields["status"])
			reqLen, _ := strconv.ParseInt(fields["request_length"], 10, 64)
			reqTime, _ := strconv.ParseFloat(fields["request_time"], 64)
			uri := fields["request_uri"]

			// 第一次处理日志时打印样例
			if atomic.LoadInt64(&totalProcessed) == 0 && processed == 0 {
				log.Println("Sample log entry:")
				log.Printf("  host: %s", host)
				log.Printf("  status: %d", status)
				log.Printf("  request_uri: %s", uri)
				log.Printf("  request_length: %d", reqLen)
				log.Printf("  request_time: %.3f", reqTime)
			}

			// 更新主机统计
			h := getOrCreateHost(host)
			h.mu.Lock()

			h.Request++
			h.RequestLength += reqLen
			h.RequestTime += reqTime

			// 按状态码分类
			switch {
			case status >= 200 && status < 300:
				h.Request20X++
			case status >= 400 && status < 500:
				h.Request4XX++
			case status >= 500 && status < 600:
				h.Request5XX++
			}

			// 更新路径统计
			if prefixes, ok := cfg.PathPrefix[host]; ok {
				matched := false
				for _, prefix := range prefixes {
					if strings.HasPrefix(uri, prefix) {
						ps := getOrCreatePath(h, prefix)
						ps.Request++
						ps.RequestLength += reqLen
						ps.RequestTime += reqTime

						switch {
						case status >= 200 && status < 300:
							ps.Request20X++
						case status >= 400 && status < 500:
							ps.Request4XX++
						case status >= 500 && status < 600:
							ps.Request5XX++
						}
						matched = true
						break // 只匹配第一个前缀
					}
				}

				// 第一次匹配/不匹配时打印调试信息
				if processed < 5 {
					if matched {
						log.Printf("  [%s] URI '%s' matched path prefix", host, uri)
					} else {
						log.Printf("  [%s] URI '%s' did not match any configured prefix: %v", host, uri, prefixes)
					}
				}
			}

			h.mu.Unlock()
			processed++
		}
	}

	if skipped > 0 && atomic.LoadInt64(&totalProcessed) < 100 {
		log.Printf("Skipped %d logs without host field", skipped)
	}

	atomic.AddInt64(&totalProcessed, int64(processed))
	return processed
}

func getOrCreateHost(host string) *HostStat {
	mu.RLock()
	h, ok := hostStats[host]
	mu.RUnlock()

	if ok {
		return h
	}

	mu.Lock()
	defer mu.Unlock()

	// 双重检查
	if h, ok := hostStats[host]; ok {
		return h
	}

	h = &HostStat{
		Host:  host,
		Paths: make(map[string]*PathStat),
	}
	hostStats[host] = h
	log.Printf("New host tracked: %s", host)
	return h
}

func getOrCreatePath(h *HostStat, prefix string) *PathStat {
	if ps, ok := h.Paths[prefix]; ok {
		return ps
	}
	ps := &PathStat{Path: prefix}
	h.Paths[prefix] = ps
	return ps
}

func calculateStatResult(host string, request, request20X, request4XX, request5XX, requestLength int64, requestTime float64, paths map[string]*PathStat) StatResult {
	result := StatResult{
		Host:          host,
		Request:       request,
		Request20X:    request20X,
		Request4XX:    request4XX,
		Request5XX:    request5XX,
		RequestLength: requestLength,
		RequestTime:   requestTime,
	}

	if request > 0 {
		result.AvgRequestTime = requestTime / float64(request)
		result.AvgRequestSize = float64(requestLength) / float64(request)
		result.SuccessRate = float64(request20X) / float64(request) * 100
	}

	if paths != nil {
		result.Paths = make(map[string]PathStat)
		for k, v := range paths {
			result.Paths[k] = *v
		}
	}

	return result
}

func startHTTP() {
	r := mux.NewRouter()
	r.HandleFunc("/hosts", getHosts).Methods("GET")
	r.HandleFunc("/hosts/{host}", getHostDetail).Methods("GET")
	r.HandleFunc("/hosts/{host}/paths/{path:.*}", getHostPath).Methods("GET")
	r.HandleFunc("/stats", getGlobalStats).Methods("GET")
	r.HandleFunc("/health", healthCheck).Methods("GET")

	// 优雅关闭
	srv := &http.Server{
		Addr:         cfg.HTTPListen,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("HTTP server listening on %s", cfg.HTTPListen)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("HTTP server error:", err)
	}
}

func healthCheck(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          "ok",
		"uptime_seconds":  time.Since(startTime).Seconds(),
		"total_processed": atomic.LoadInt64(&totalProcessed),
	})
}

func getGlobalStats(w http.ResponseWriter, _ *http.Request) {
	mu.RLock()
	hostCount := len(hostStats)
	mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_hosts":     hostCount,
		"total_processed": atomic.LoadInt64(&totalProcessed),
		"uptime_seconds":  time.Since(startTime).Seconds(),
		"qps":             float64(atomic.LoadInt64(&totalProcessed)) / time.Since(startTime).Seconds(),
	})
}

func getHosts(w http.ResponseWriter, _ *http.Request) {
	mu.RLock()
	hosts := make([]*HostStat, 0, len(hostStats))
	for _, h := range hostStats {
		hosts = append(hosts, h)
	}
	mu.RUnlock()

	results := make([]StatResult, 0, len(hosts))
	for _, h := range hosts {
		h.mu.RLock()
		result := calculateStatResult(
			h.Host, h.Request, h.Request20X, h.Request4XX, h.Request5XX,
			h.RequestLength, h.RequestTime, nil,
		)
		h.mu.RUnlock()
		results = append(results, result)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func getHostDetail(w http.ResponseWriter, r *http.Request) {
	v := mux.Vars(r)
	host := v["host"]

	mu.RLock()
	h, ok := hostStats[host]
	mu.RUnlock()

	if !ok {
		http.Error(w, "Host not found", http.StatusNotFound)
		return
	}

	h.mu.RLock()
	result := calculateStatResult(
		h.Host, h.Request, h.Request20X, h.Request4XX, h.Request5XX,
		h.RequestLength, h.RequestTime, h.Paths,
	)
	h.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func getHostPath(w http.ResponseWriter, r *http.Request) {
	v := mux.Vars(r)
	host := v["host"]
	path := "/" + v["path"]

	mu.RLock()
	h, ok := hostStats[host]
	mu.RUnlock()

	if !ok {
		http.Error(w, "Host not found", http.StatusNotFound)
		return
	}

	h.mu.RLock()
	ps, ok := h.Paths[path]
	if !ok {
		h.mu.RUnlock()
		http.Error(w, "Path not found", http.StatusNotFound)
		return
	}

	result := StatResult{
		Host:          host,
		Path:          ps.Path,
		Request:       ps.Request,
		Request20X:    ps.Request20X,
		Request4XX:    ps.Request4XX,
		Request5XX:    ps.Request5XX,
		RequestLength: ps.RequestLength,
		RequestTime:   ps.RequestTime,
	}

	if ps.Request > 0 {
		result.AvgRequestTime = ps.RequestTime / float64(ps.Request)
		result.AvgRequestSize = float64(ps.RequestLength) / float64(ps.Request)
		result.SuccessRate = float64(ps.Request20X) / float64(ps.Request) * 100
	}

	h.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
