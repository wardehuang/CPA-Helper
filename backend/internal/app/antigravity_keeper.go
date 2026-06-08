package app

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	antigravityKeeperDefaultProjectID         = "bamboo-precept-lgxtn"
	antigravityKeeperTierURL                  = "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist"
	antigravityKeeperLogFilePrefix            = "antigravity-keeper-"
	antigravityKeeperRawResponseLogFilePrefix = "antigravity-keeper-raw-response-"
	antigravityKeeperLogComponent             = "antigravity_keeper"
	antigravityKeeperLogRetainedFiles         = 3
	antigravityKeeperMaxInMemoryLogs          = 300
	antigravityKeeperQuotaWindowUsageCacheTTL = 30 * time.Second
	antigravityKeeperFiveHourWindowSeconds    = 5 * 60 * 60
	antigravityKeeperWeekWindowSeconds        = 7 * 24 * 60 * 60
	antigravityKeeperMonthWindowSeconds       = 30 * 24 * 60 * 60
)

var antigravityKeeperQuotaURLs = []string{
	"https://daily-cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
	"https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels",
	"https://cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
}

var antigravityKeeperPrimaryUsageModelPrefixes = []string{
	"claude-",
	"gpt-oss-",
}

var antigravityKeeperSecondaryUsageModelPrefixes = []string{
	"gemini-",
}

type antigravityKeeperRunner struct {
	app            *App
	mu             sync.Mutex
	daemonStop     chan struct{}
	daemonDone     chan struct{}
	running        bool
	runningModes   map[string]struct{}
	inFlightAuths  map[string]string
	state          string
	detail         string
	mode           *string
	lastStartedAt  *time.Time
	lastFinishedAt *time.Time
	stats          antigravityKeeperStats
	logs           []string
}

type antigravityKeeperStats struct {
	Total            int `json:"total"`
	Healthy          int `json:"healthy"`
	StatusDisabled   int `json:"status_disabled"`
	StatusEnabled    int `json:"status_enabled"`
	PriorityDegraded int `json:"priority_degraded"`
	PriorityRestored int `json:"priority_restored"`
	Skipped          int `json:"skipped"`
	NetworkError     int `json:"network_error"`
}

type antigravityKeeperStatusResponse struct {
	Running        bool                   `json:"running"`
	RunningModes   []string               `json:"running_modes"`
	DaemonRunning  bool                   `json:"daemon_running"`
	State          string                 `json:"state"`
	Detail         string                 `json:"detail"`
	Mode           *string                `json:"mode"`
	LastStartedAt  *string                `json:"last_started_at"`
	LastFinishedAt *string                `json:"last_finished_at"`
	Stats          antigravityKeeperStats `json:"stats"`
	Logs           []string               `json:"logs"`
}

type antigravityKeeperPriorityRule struct {
	AccountType string `json:"account_type"`
	Priority    int    `json:"priority"`
}

type antigravityKeeperSettingsUpdateRequest struct {
	ScheduleCron                      *string                         `json:"schedule_cron"`
	QuotaThreshold                    *int                            `json:"quota_threshold"`
	UsageTimeoutSeconds               *int                            `json:"usage_timeout_seconds"`
	CPATimeoutSeconds                 *int                            `json:"cpa_timeout_seconds"`
	MaxRetries                        *int                            `json:"max_retries"`
	WorkerThreads                     *int                            `json:"worker_threads"`
	ConditionalRefreshIntervalSeconds *int                            `json:"conditional_refresh_interval_seconds"`
	AccountRefreshCacheMinutes        *int                            `json:"account_refresh_cache_minutes"`
	DryRun                            *bool                           `json:"dry_run"`
	EnableCredentialWebsockets        *bool                           `json:"enable_credential_websockets"`
	AutoStartDaemon                   *bool                           `json:"auto_start_daemon"`
	PriorityRules                     []antigravityKeeperPriorityRule `json:"priority_rules"`
}

type antigravityKeeperCronPreviewRequest struct {
	ScheduleCron string `json:"schedule_cron"`
}

type antigravityKeeperBulkDeleteRequest struct {
	AuthNames []string `json:"auth_names"`
}

type antigravityKeeperRefreshAccountsRequest struct {
	AuthNames []string `json:"auth_names"`
}

type antigravityKeeperPriorityUpdateRequest struct {
	Priority int `json:"priority"`
}

type antigravityKeeperAccount struct {
	Name                   string     `json:"name"`
	Email                  *string    `json:"email"`
	AccountType            *string    `json:"account_type"`
	Disabled               bool       `json:"disabled"`
	Priority               *int       `json:"priority"`
	PrimaryUsedPercent     *int       `json:"primary_used_percent"`
	SecondaryUsedPercent   *int       `json:"secondary_used_percent"`
	PrimaryResetAt         *time.Time `json:"primary_reset_at"`
	SecondaryResetAt       *time.Time `json:"secondary_reset_at"`
	PrimaryWindowSeconds   *int       `json:"primary_window_seconds"`
	SecondaryWindowSeconds *int       `json:"secondary_window_seconds"`
	QuotaThreshold         *int       `json:"quota_threshold"`
	LastStatusCode         *int       `json:"last_status_code"`
	LastError              *string    `json:"last_error"`
	LatestAction           *string    `json:"latest_action"`
	LastCheckedAt          *time.Time `json:"last_checked_at"`
	LastHealthyAt          *time.Time `json:"last_healthy_at"`
}

type antigravityKeeperAccountResponse struct {
	Name                   string                                     `json:"name"`
	Email                  *string                                    `json:"email"`
	AccountType            *string                                    `json:"account_type"`
	Disabled               bool                                       `json:"disabled"`
	Priority               *int                                       `json:"priority"`
	PrimaryUsedPercent     *int                                       `json:"primary_used_percent"`
	SecondaryUsedPercent   *int                                       `json:"secondary_used_percent"`
	PrimaryResetAt         *string                                    `json:"primary_reset_at"`
	SecondaryResetAt       *string                                    `json:"secondary_reset_at"`
	PrimaryWindowSeconds   *int                                       `json:"primary_window_seconds"`
	SecondaryWindowSeconds *int                                       `json:"secondary_window_seconds"`
	PrimaryWindowUsage     *antigravityKeeperQuotaWindowUsageResponse `json:"primary_window_usage"`
	SecondaryWindowUsage   *antigravityKeeperQuotaWindowUsageResponse `json:"secondary_window_usage"`
	QuotaThreshold         *int                                       `json:"quota_threshold"`
	LastStatusCode         *int                                       `json:"last_status_code"`
	LastError              *string                                    `json:"last_error"`
	LatestAction           *string                                    `json:"latest_action"`
	LastCheckedAt          *string                                    `json:"last_checked_at"`
	LastHealthyAt          *string                                    `json:"last_healthy_at"`
}

type antigravityKeeperQuotaWindowUsageResponse struct {
	WindowStart      string  `json:"window_start"`
	WindowEnd        string  `json:"window_end"`
	ResetAt          string  `json:"reset_at"`
	WindowSeconds    int     `json:"window_seconds"`
	Records          int     `json:"records"`
	SuccessRecords   int     `json:"success_records"`
	FailedRecords    int     `json:"failed_records"`
	InputTokens      int     `json:"input_tokens"`
	OutputTokens     int     `json:"output_tokens"`
	CachedTokens     int     `json:"cached_tokens"`
	ReasoningTokens  int     `json:"reasoning_tokens"`
	TotalTokens      int     `json:"total_tokens"`
	EstimatedCostUSD float64 `json:"estimated_cost_usd"`
	UnpricedRecords  int     `json:"unpriced_records"`
	Stale            bool    `json:"stale"`
	WindowSource     string  `json:"window_source"`
}

type antigravityKeeperQuotaWindowUsage struct {
	WindowStart      time.Time
	WindowEnd        time.Time
	ResetAt          time.Time
	WindowSeconds    int
	Records          int
	SuccessRecords   int
	FailedRecords    int
	InputTokens      int
	OutputTokens     int
	CachedTokens     int
	ReasoningTokens  int
	TotalTokens      int
	EstimatedCostUSD float64
	UnpricedRecords  int
	Stale            bool
	WindowSource     string
}

type antigravityKeeperQuotaWindowUsagePair struct {
	Primary   *antigravityKeeperQuotaWindowUsage
	Secondary *antigravityKeeperQuotaWindowUsage
}

type antigravityKeeperWindowUsageCache struct {
	mu        sync.Mutex
	key       string
	expiresAt time.Time
	usages    map[string]antigravityKeeperQuotaWindowUsagePair
}

type antigravityKeeperAuthState struct {
	antigravityKeeperAccount
	RestorePriority *int
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type antigravityKeeperUsageInfo struct {
	PlanType               string
	PrimaryUsedPercent     *int
	SecondaryUsedPercent   *int
	PrimaryResetAt         *time.Time
	SecondaryResetAt       *time.Time
	PrimaryWindowSeconds   *int
	SecondaryWindowSeconds *int
}

type antigravityKeeperHTTPResult struct {
	StatusCode *int
	JSONData   map[string]any
	Brief      string
	Error      string
}

type antigravityKeeperAccountResult struct {
	Name                   string
	Result                 string
	Email                  *string
	AccountType            *string
	Priority               *int
	RestorePriority        *int
	ClearRestorePriority   bool
	Disabled               *bool
	PrimaryUsedPercent     *int
	SecondaryUsedPercent   *int
	PrimaryResetAt         *time.Time
	SecondaryResetAt       *time.Time
	PrimaryWindowSeconds   *int
	SecondaryWindowSeconds *int
	QuotaThreshold         *int
	LastStatusCode         *int
	LastError              *string
	LatestAction           *string
	CheckedAt              time.Time
}

func NewAntigravityKeeperRunner(app *App) *antigravityKeeperRunner {
	return &antigravityKeeperRunner{
		app:    app,
		state:  "idle",
		detail: "尚未运行",
		logs:   []string{},
	}
}

func (r *antigravityKeeperRunner) LoadPersistedState(ctx context.Context) {
	logs, logErr := r.app.loadAntigravityKeeperLogLines(antigravityKeeperMaxInMemoryLogs)
	if logErr != nil {
		log.Printf("restore antigravity keeper logs failed: %v", logErr)
	}
	run, err := r.app.latestAntigravityKeeperRun(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = logs
	if err != nil || run == nil {
		return
	}
	r.state = run.State
	r.detail = run.Detail
	r.mode = run.Mode
	r.lastStartedAt = run.StartedAt
	r.lastFinishedAt = run.FinishedAt
	r.stats = run.Stats
}

func (r *antigravityKeeperRunner) StartAutoIfConfigured() {
	cfg, err := r.app.loadConfig(context.Background())
	if err != nil {
		r.log("读取 Antigravity Keeper 自动启动配置失败：" + err.Error())
		return
	}
	if cfg.AntigravityKeeper.AutoStartDaemon && strings.TrimSpace(cfg.Collector.ManagementKey) != "" {
		if err := r.StartDaemon(); err != nil {
			r.log("启动 Antigravity Keeper 自动巡检失败：" + err.Error())
		}
	}
}

func (r *antigravityKeeperRunner) StartOnce() error {
	if !r.markRunning("once") {
		return conflictError("Antigravity Keeper 正在运行")
	}
	go r.run("once")
	return nil
}

func (r *antigravityKeeperRunner) StartAccounts(authNames []string) error {
	names, err := normalizeAntigravityKeeperAuthNames(authNames)
	if err != nil {
		return err
	}
	if !r.markRunning("accounts") {
		return conflictError("Antigravity Keeper 正在运行")
	}
	go r.runAccounts("accounts", names)
	return nil
}

func (r *antigravityKeeperRunner) StartDaemon() error {
	cfg, err := r.app.loadConfig(context.Background())
	if err != nil {
		return err
	}
	if strings.TrimSpace(cfg.Collector.ManagementKey) == "" {
		return validationError("管理密钥未设置，无法运行 Antigravity Keeper")
	}
	if _, _, err := nextRunTimes(cfg.AntigravityKeeper.ScheduleCron, 1, time.Now()); err != nil {
		return err
	}

	r.mu.Lock()
	if r.daemonRunningLocked() {
		r.mu.Unlock()
		return nil
	}
	r.daemonStop = make(chan struct{})
	r.daemonDone = make(chan struct{})
	stop := r.daemonStop
	done := r.daemonDone
	r.mu.Unlock()

	go r.daemonLoop(stop, done)
	r.log("Antigravity Keeper 已开始按计划自动巡检")
	return nil
}

func (r *antigravityKeeperRunner) Stop() {
	r.mu.Lock()
	stop := r.daemonStop
	done := r.daemonDone
	if stop == nil || done == nil {
		r.mu.Unlock()
		return
	}
	select {
	case <-done:
		r.daemonStop = nil
		r.daemonDone = nil
		r.mu.Unlock()
		return
	default:
	}
	select {
	case <-stop:
	default:
		close(stop)
	}
	r.mu.Unlock()
	<-done
	r.mu.Lock()
	if r.daemonStop == stop {
		r.daemonStop = nil
	}
	if r.daemonDone == done {
		r.daemonDone = nil
	}
	r.mu.Unlock()
	r.log("Antigravity Keeper 已停止自动巡检")
}

func (r *antigravityKeeperRunner) ClearLogs() {
	r.mu.Lock()
	r.logs = []string{}
	r.mu.Unlock()
	if err := r.app.clearAntigravityKeeperLogFiles(); err != nil {
		log.Printf("clear antigravity keeper log files failed: %v", err)
	}
}

func (r *antigravityKeeperRunner) Status() antigravityKeeperStatusResponse {
	r.mu.Lock()
	logs := append([]string{}, r.logs...)
	runningModes := r.runningModeListLocked()
	response := antigravityKeeperStatusResponse{
		Running:        len(runningModes) > 0,
		RunningModes:   runningModes,
		DaemonRunning:  r.daemonRunningLocked(),
		State:          r.state,
		Detail:         r.detail,
		Mode:           antigravityCloneStringPtr(r.mode),
		LastStartedAt:  apiDateTimePtr(r.lastStartedAt),
		LastFinishedAt: apiDateTimePtr(r.lastFinishedAt),
		Stats:          r.stats,
		Logs:           logs,
	}
	r.mu.Unlock()
	if r.app != nil {
		response.Stats = antigravityKeeperStats{}
		if run, err := r.app.latestAntigravityKeeperRunByMode(context.Background(), "daemon"); err == nil && run != nil {
			response.Stats = run.Stats
		}
	}
	return response
}

func (r *antigravityKeeperRunner) daemonRunningLocked() bool {
	if r.daemonDone == nil {
		return false
	}
	select {
	case <-r.daemonDone:
		return false
	default:
		return true
	}
}

func (r *antigravityKeeperRunner) daemonLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for {
		cfg, err := r.app.loadConfig(context.Background())
		if err != nil {
			r.log("读取 Antigravity Keeper 配置失败：" + err.Error())
			if antigravityWaitForStop(stop, time.Minute) {
				return
			}
			continue
		}
		times, _, err := nextRunTimes(cfg.AntigravityKeeper.ScheduleCron, 1, time.Now().In(appTimeLocation))
		if err != nil {
			r.log("Antigravity Keeper 定时表达式无效：" + err.Error())
			if antigravityWaitForStop(stop, time.Minute) {
				return
			}
			continue
		}
		delay := antigravityPositiveDuration(time.Until(times[0]))
		r.log("下一轮计划：" + times[0].In(appTimeLocation).Format("2006-01-02 15:04:05"))
		cronTimer := time.NewTimer(delay)
		conditionalInterval := antigravityKeeperConditionalRefreshInterval(cfg)
		var conditionalTicker *time.Ticker
		var conditionalC <-chan time.Time
		if conditionalInterval > 0 {
			conditionalTicker = time.NewTicker(conditionalInterval)
			conditionalC = conditionalTicker.C
		}

		restartCycle := false
		for !restartCycle {
			select {
			case <-stop:
				cronTimer.Stop()
				if conditionalTicker != nil {
					conditionalTicker.Stop()
				}
				return
			case <-cronTimer.C:
				if conditionalTicker != nil {
					conditionalTicker.Stop()
				}
				if r.markRunning("daemon") {
					go r.run("daemon")
				}
				restartCycle = true
			case <-conditionalC:
				nextCfg, err := r.app.loadConfig(context.Background())
				if err != nil {
					r.log("读取 Antigravity Keeper 条件刷新配置失败：" + err.Error())
					continue
				}
				nextInterval := antigravityKeeperConditionalRefreshInterval(nextCfg)
				if nextInterval != conditionalInterval {
					cronTimer.Stop()
					if conditionalTicker != nil {
						conditionalTicker.Stop()
					}
					restartCycle = true
					continue
				}
				names, err := r.app.conditionalAntigravityKeeperRefreshCandidates(context.Background(), nextCfg)
				if err != nil {
					r.log("Antigravity Keeper 条件刷新候选查询失败：" + err.Error())
					continue
				}
				if len(names) == 0 {
					continue
				}
				if r.markRunning("conditional") {
					go r.runAccounts("conditional", names)
				}
			}
		}
	}
}

func antigravityPositiveDuration(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	return delay
}

func antigravityKeeperConditionalRefreshInterval(cfg AppConfig) time.Duration {
	seconds := cfg.AntigravityKeeper.ConditionalRefreshIntervalSeconds
	if !validKeeperConditionalRefreshInterval(seconds) || seconds == 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func (r *antigravityKeeperRunner) ensureRunningModesLocked() {
	if r.runningModes == nil {
		r.runningModes = map[string]struct{}{}
	}
}

func (r *antigravityKeeperRunner) runningModeListLocked() []string {
	r.ensureRunningModesLocked()
	modes := make([]string, 0, len(r.runningModes))
	for mode := range r.runningModes {
		modes = append(modes, mode)
	}
	sort.Slice(modes, func(i, j int) bool {
		leftOrder := antigravityKeeperModeOrder(modes[i])
		rightOrder := antigravityKeeperModeOrder(modes[j])
		if leftOrder != rightOrder {
			return leftOrder < rightOrder
		}
		return modes[i] < modes[j]
	})
	return modes
}

func (r *antigravityKeeperRunner) ensureInFlightAuthsLocked() {
	if r.inFlightAuths == nil {
		r.inFlightAuths = map[string]string{}
	}
}

func (r *antigravityKeeperRunner) tryLockAuthName(mode string, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureInFlightAuthsLocked()
	if _, exists := r.inFlightAuths[name]; exists {
		return false
	}
	r.inFlightAuths[name] = mode
	return true
}

func (r *antigravityKeeperRunner) unlockAuthName(name string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inFlightAuths == nil {
		return
	}
	delete(r.inFlightAuths, name)
}

func antigravityKeeperModeOrder(mode string) int {
	switch mode {
	case "daemon":
		return 0
	case "once":
		return 1
	case "conditional":
		return 2
	case "accounts":
		return 3
	default:
		return 99
	}
}

func antigravityKeeperModesConflict(existingMode string, nextMode string) bool {
	if existingMode == nextMode {
		return true
	}
	return (existingMode == "once" && nextMode == "daemon") ||
		(existingMode == "daemon" && nextMode == "once")
}

func antigravityKeeperStatusModePtr(modes []string) *string {
	if len(modes) == 0 {
		return nil
	}
	mode := modes[0]
	return &mode
}

func antigravityKeeperRunningDetail(modes []string) string {
	if len(modes) > 1 {
		return "正在运行多个 Antigravity Keeper 任务"
	}
	if len(modes) == 0 {
		return "尚未运行"
	}
	switch modes[0] {
	case "accounts":
		return "正在刷新 Antigravity 账号"
	case "conditional":
		return "正在按条件刷新 Antigravity 账号"
	default:
		return "正在巡检 Antigravity 账号"
	}
}

func (r *antigravityKeeperRunner) markRunning(mode string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureRunningModesLocked()
	for runningMode := range r.runningModes {
		if antigravityKeeperModesConflict(runningMode, mode) {
			return false
		}
	}
	r.runningModes[mode] = struct{}{}
	now := time.Now().In(appTimeLocation)
	r.running = true
	r.state = "running"
	runningModes := r.runningModeListLocked()
	r.detail = antigravityKeeperRunningDetail(runningModes)
	r.mode = antigravityKeeperStatusModePtr(runningModes)
	r.lastStartedAt = &now
	r.lastFinishedAt = nil
	r.stats = antigravityKeeperStats{}
	return true
}

func (r *antigravityKeeperRunner) run(mode string) {
	r.runAccounts(mode, nil)
}

func (r *antigravityKeeperRunner) runAccounts(mode string, authNames []string) {
	options := antigravityKeeperRunOptionsForMode(mode, authNames)
	options.TryLockAuthName = r.tryLockAuthName
	options.UnlockAuthName = r.unlockAuthName
	stats, detail, err := r.app.executeAntigravityKeeperRunWithOptions(context.Background(), options, r.log)
	finishedAt := time.Now().In(appTimeLocation)
	logMessage := detail
	if err != nil {
		logMessage = "巡检失败：" + err.Error()
	}
	r.mu.Lock()
	r.ensureRunningModesLocked()
	delete(r.runningModes, mode)
	runningModes := r.runningModeListLocked()
	r.running = len(runningModes) > 0
	r.lastFinishedAt = &finishedAt
	r.stats = stats
	if r.running {
		r.state = "running"
		r.detail = antigravityKeeperRunningDetail(runningModes)
		r.mode = antigravityKeeperStatusModePtr(runningModes)
	} else if err != nil {
		r.state = "failed"
		r.detail = err.Error()
	} else {
		completedMode := mode
		r.mode = &completedMode
		r.state = "completed"
		r.detail = detail
	}
	r.mu.Unlock()
	if strings.TrimSpace(logMessage) != "" {
		r.log(logMessage)
	}
}

func (r *antigravityKeeperRunner) log(message string) {
	timestamp := time.Now().In(appTimeLocation)
	line := formatAntigravityKeeperLogLine(timestamp, message)
	r.mu.Lock()
	r.logs = appendAntigravityKeeperLog(r.logs, line)
	r.mu.Unlock()
	if err := r.app.appendAntigravityKeeperLogFile(timestamp, line); err != nil {
		log.Printf("write antigravity keeper log failed: %v", err)
	}
}

func appendAntigravityKeeperLog(logs []string, line string) []string {
	logs = append(logs, line)
	if len(logs) > antigravityKeeperMaxInMemoryLogs {
		logs = logs[len(logs)-antigravityKeeperMaxInMemoryLogs:]
	}
	return logs
}

func formatAntigravityKeeperLogLine(timestamp time.Time, message string) string {
	var output strings.Builder
	handler := slog.NewTextHandler(&output, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 && attr.Key == slog.TimeKey {
				return slog.String(slog.TimeKey, timestamp.In(appTimeLocation).Format("2006-01-02T15:04:05.000Z07:00"))
			}
			return attr
		},
	})
	record := slog.NewRecord(timestamp.In(appTimeLocation), slog.LevelInfo, message, 0)
	record.AddAttrs(slog.String("component", antigravityKeeperLogComponent))
	_ = handler.Handle(context.Background(), record)
	return strings.TrimSuffix(output.String(), "\n")
}

type antigravityKeeperLogFile struct {
	path string
	date time.Time
}

func (a *App) antigravityKeeperLogDir() string {
	return filepath.Join(a.dataDir, "logs")
}

func (a *App) appendAntigravityKeeperLogFile(timestamp time.Time, line string) error {
	dir := a.antigravityKeeperLogDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, antigravityKeeperLogFilePrefix+timestamp.In(appTimeLocation).Format("2006-01-02")+".log")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(line + "\n")
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return a.pruneAntigravityKeeperLogFiles()
}

func (a *App) appendAntigravityKeeperRawResponseLogFile(timestamp time.Time, authIndex string, endpoint string, statusCode int, body any) error {
	dir := a.antigravityKeeperLogDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	entry := map[string]any{
		"time":        timestamp.In(appTimeLocation).Format(time.RFC3339Nano),
		"auth_index":  authIndex,
		"endpoint":    endpoint,
		"status_code": statusCode,
		"body_raw":    antigravityKeeperRawResponseText(body),
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, antigravityKeeperRawResponseLogFilePrefix+timestamp.In(appTimeLocation).Format("2006-01-02")+".jsonl")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(payload, '\n'))
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return a.pruneAntigravityKeeperRawResponseLogFiles()
}

func antigravityKeeperRawResponseText(body any) string {
	if text, ok := body.(string); ok {
		return text
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Sprint(body)
	}
	return string(payload)
}

func (a *App) loadAntigravityKeeperLogLines(limit int) ([]string, error) {
	files, err := a.antigravityKeeperLogFiles()
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].date.Before(files[j].date)
	})
	if len(files) > antigravityKeeperLogRetainedFiles {
		files = files[len(files)-antigravityKeeperLogRetainedFiles:]
	}
	lines := []string{}
	for _, file := range files {
		handle, err := os.Open(file.path)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(handle)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			lines = appendAntigravityKeeperLog(lines, line)
		}
		scanErr := scanner.Err()
		closeErr := handle.Close()
		if scanErr != nil {
			return nil, scanErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	if limit > 0 && len(lines) > limit {
		return lines[len(lines)-limit:], nil
	}
	return lines, nil
}

func (a *App) pruneAntigravityKeeperLogFiles() error {
	files, err := a.antigravityKeeperLogFiles()
	if err != nil {
		return err
	}
	return pruneAntigravityKeeperLogFiles(files)
}

func (a *App) pruneAntigravityKeeperRawResponseLogFiles() error {
	files, err := a.antigravityKeeperRawResponseLogFiles()
	if err != nil {
		return err
	}
	return pruneAntigravityKeeperLogFiles(files)
}

func pruneAntigravityKeeperLogFiles(files []antigravityKeeperLogFile) error {
	sort.Slice(files, func(i, j int) bool {
		return files[i].date.After(files[j].date)
	})
	for index, file := range files {
		if index < antigravityKeeperLogRetainedFiles {
			continue
		}
		if err := os.Remove(file.path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (a *App) clearAntigravityKeeperLogFiles() error {
	files, err := a.antigravityKeeperLogFiles()
	if err != nil {
		return err
	}
	rawFiles, err := a.antigravityKeeperRawResponseLogFiles()
	if err != nil {
		return err
	}
	files = append(files, rawFiles...)
	for _, file := range files {
		if err := os.Remove(file.path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (a *App) antigravityKeeperLogFiles() ([]antigravityKeeperLogFile, error) {
	return a.antigravityKeeperLogFilesWithPattern(antigravityKeeperLogFilePrefix, ".log")
}

func (a *App) antigravityKeeperRawResponseLogFiles() ([]antigravityKeeperLogFile, error) {
	return a.antigravityKeeperLogFilesWithPattern(antigravityKeeperRawResponseLogFilePrefix, ".jsonl")
}

func (a *App) antigravityKeeperLogFilesWithPattern(prefix string, suffix string) ([]antigravityKeeperLogFile, error) {
	dir := a.antigravityKeeperLogDir()
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	files := []antigravityKeeperLogFile{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		dateText := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		date, err := time.ParseInLocation("2006-01-02", dateText, appTimeLocation)
		if err != nil {
			continue
		}
		files = append(files, antigravityKeeperLogFile{
			path: filepath.Join(dir, name),
			date: date,
		})
	}
	return files, nil
}

func (a *App) handleAntigravityKeeper(w http.ResponseWriter, r *http.Request) error {
	if _, err := a.adminUser(r.Context(), r); err != nil {
		return err
	}
	parts := splitPath(r.URL.Path, "/api/antigravity-keeper/")
	if len(parts) == 0 {
		return notFoundError("Not Found")
	}
	switch {
	case len(parts) == 1 && parts[0] == "settings":
		if r.Method == http.MethodGet {
			cfg, err := a.loadConfig(r.Context())
			if err != nil {
				return err
			}
			writeJSON(w, http.StatusOK, antigravityKeeperSettingsResponse(cfg))
			return nil
		}
		if r.Method == http.MethodPut {
			return a.updateAntigravityKeeperSettings(w, r)
		}
		return methodNotAllowed()
	case len(parts) == 2 && parts[0] == "schedule" && parts[1] == "preview":
		if err := requireMethod(r, http.MethodPost); err != nil {
			return err
		}
		var payload antigravityKeeperCronPreviewRequest
		if err := decodeJSON(r, &payload); err != nil {
			return err
		}
		times, normalized, err := nextRunTimes(payload.ScheduleCron, 5, time.Now())
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, map[string]any{"schedule_cron": normalized, "next_run_times": apiDateTimes(times)})
		return nil
	case len(parts) == 1 && parts[0] == "status":
		if err := requireMethod(r, http.MethodGet); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, a.antigravityKeeper.Status())
		return nil
	case len(parts) == 1 && parts[0] == "accounts":
		if err := requireMethod(r, http.MethodGet); err != nil {
			return err
		}
		accounts, err := a.listAntigravityKeeperAccounts(r.Context())
		if err != nil {
			return err
		}
		windowUsages, err := a.antigravityKeeperQuotaWindowUsages(r.Context(), accounts)
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": antigravityKeeperAccountResponses(accounts, windowUsages)})
		return nil
	case len(parts) == 1 && parts[0] == "run-once":
		if err := requireMethod(r, http.MethodPost); err != nil {
			return err
		}
		if err := a.antigravityKeeper.StartOnce(); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
		return nil
	case len(parts) == 1 && parts[0] == "start":
		if err := requireMethod(r, http.MethodPost); err != nil {
			return err
		}
		if err := a.antigravityKeeper.StartDaemon(); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
		return nil
	case len(parts) == 1 && parts[0] == "stop":
		if err := requireMethod(r, http.MethodPost); err != nil {
			return err
		}
		a.antigravityKeeper.Stop()
		writeJSON(w, http.StatusOK, map[string]string{"status": "stopping"})
		return nil
	case len(parts) == 2 && parts[0] == "logs" && parts[1] == "clear":
		if err := requireMethod(r, http.MethodPost); err != nil {
			return err
		}
		a.antigravityKeeper.ClearLogs()
		writeJSON(w, http.StatusOK, map[string]string{"status": "cleared"})
		return nil
	case len(parts) == 2 && parts[0] == "accounts" && parts[1] == "bulk-delete":
		if err := requireMethod(r, http.MethodPost); err != nil {
			return err
		}
		return a.bulkDeleteAntigravityKeeperAccounts(w, r)
	case len(parts) == 2 && parts[0] == "accounts" && parts[1] == "refresh":
		if err := requireMethod(r, http.MethodPost); err != nil {
			return err
		}
		var payload antigravityKeeperRefreshAccountsRequest
		if err := decodeJSON(r, &payload); err != nil {
			return err
		}
		if err := a.antigravityKeeper.StartAccounts(payload.AuthNames); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
		return nil
	case len(parts) == 3 && parts[0] == "accounts" && (parts[2] == "enable" || parts[2] == "disable"):
		if err := requireMethod(r, http.MethodPost); err != nil {
			return err
		}
		authName, err := url.PathUnescape(parts[1])
		if err != nil {
			return validationError("账号名称无效")
		}
		disabled := parts[2] == "disable"
		if err := a.setAntigravityKeeperAccountDisabled(r.Context(), authName, disabled); err != nil {
			return err
		}
		if disabled {
			writeJSON(w, http.StatusOK, map[string]string{"status": "disabled"})
		} else {
			writeJSON(w, http.StatusOK, map[string]string{"status": "enabled"})
		}
		return nil
	case len(parts) == 2 && parts[0] == "accounts" && r.Method == http.MethodDelete:
		authName, err := url.PathUnescape(parts[1])
		if err != nil {
			return validationError("账号名称无效")
		}
		if err := a.deleteAntigravityKeeperAccount(r.Context(), authName); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
		return nil
	case len(parts) == 3 && parts[0] == "accounts" && parts[2] == "priority":
		if err := requireMethod(r, http.MethodPatch); err != nil {
			return err
		}
		authName, err := url.PathUnescape(parts[1])
		if err != nil {
			return validationError("账号名称无效")
		}
		var payload antigravityKeeperPriorityUpdateRequest
		if err := decodeJSON(r, &payload); err != nil {
			return err
		}
		if err := a.updateAntigravityKeeperAccountPriority(r.Context(), authName, payload.Priority); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
		return nil
	default:
		return notFoundError("Not Found")
	}
}

func antigravityKeeperSettingsResponse(cfg AppConfig) map[string]any {
	times, normalized, err := nextRunTimes(cfg.AntigravityKeeper.ScheduleCron, 5, time.Now())
	if err != nil {
		normalized = cfg.AntigravityKeeper.ScheduleCron
		times = []time.Time{}
	}
	return map[string]any{
		"cliaproxy_url":                        cfg.Collector.CLIProxyURL,
		"management_key_set":                   strings.TrimSpace(cfg.Collector.ManagementKey) != "",
		"schedule_cron":                        normalized,
		"next_run_times":                       apiDateTimes(times),
		"quota_threshold":                      cfg.AntigravityKeeper.QuotaThreshold,
		"usage_timeout_seconds":                cfg.AntigravityKeeper.UsageTimeoutSeconds,
		"cpa_timeout_seconds":                  cfg.AntigravityKeeper.CPATimeoutSeconds,
		"max_retries":                          cfg.AntigravityKeeper.MaxRetries,
		"worker_threads":                       cfg.AntigravityKeeper.WorkerThreads,
		"conditional_refresh_interval_seconds": cfg.AntigravityKeeper.ConditionalRefreshIntervalSeconds,
		"account_refresh_cache_minutes":        cfg.AntigravityKeeper.AccountRefreshCacheMinutes,
		"dry_run":                              cfg.AntigravityKeeper.DryRun,
		"enable_credential_websockets":         cfg.AntigravityKeeper.EnableCredentialWebsockets,
		"auto_start_daemon":                    cfg.AntigravityKeeper.AutoStartDaemon,
		"priority_rules":                       sortedPriorityRules(cfg.AntigravityKeeperPriorityRule),
	}
}

func antigravityKeeperAccountResponses(accounts []antigravityKeeperAccount, windowUsages map[string]antigravityKeeperQuotaWindowUsagePair) []antigravityKeeperAccountResponse {
	responses := make([]antigravityKeeperAccountResponse, 0, len(accounts))
	for _, account := range accounts {
		usage := windowUsages[account.Name]
		responses = append(responses, antigravityKeeperAccountResponse{
			Name:                   account.Name,
			Email:                  account.Email,
			AccountType:            account.AccountType,
			Disabled:               account.Disabled,
			Priority:               antigravityKeeperDisplayPriority(account.Priority),
			PrimaryUsedPercent:     account.PrimaryUsedPercent,
			SecondaryUsedPercent:   account.SecondaryUsedPercent,
			PrimaryResetAt:         apiDateTimePtr(account.PrimaryResetAt),
			SecondaryResetAt:       apiDateTimePtr(account.SecondaryResetAt),
			PrimaryWindowSeconds:   account.PrimaryWindowSeconds,
			SecondaryWindowSeconds: account.SecondaryWindowSeconds,
			PrimaryWindowUsage:     antigravityKeeperQuotaWindowUsageResponseFrom(usage.Primary),
			SecondaryWindowUsage:   antigravityKeeperQuotaWindowUsageResponseFrom(usage.Secondary),
			QuotaThreshold:         account.QuotaThreshold,
			LastStatusCode:         account.LastStatusCode,
			LastError:              account.LastError,
			LatestAction:           account.LatestAction,
			LastCheckedAt:          apiDateTimePtr(account.LastCheckedAt),
			LastHealthyAt:          apiDateTimePtr(account.LastHealthyAt),
		})
	}
	return responses
}

func antigravityKeeperQuotaWindowUsageResponseFrom(usage *antigravityKeeperQuotaWindowUsage) *antigravityKeeperQuotaWindowUsageResponse {
	if usage == nil {
		return nil
	}
	return &antigravityKeeperQuotaWindowUsageResponse{
		WindowStart:      apiDateTime(usage.WindowStart),
		WindowEnd:        apiDateTime(usage.WindowEnd),
		ResetAt:          apiDateTime(usage.ResetAt),
		WindowSeconds:    usage.WindowSeconds,
		Records:          usage.Records,
		SuccessRecords:   usage.SuccessRecords,
		FailedRecords:    usage.FailedRecords,
		InputTokens:      usage.InputTokens,
		OutputTokens:     usage.OutputTokens,
		CachedTokens:     usage.CachedTokens,
		ReasoningTokens:  usage.ReasoningTokens,
		TotalTokens:      usage.TotalTokens,
		EstimatedCostUSD: usage.EstimatedCostUSD,
		UnpricedRecords:  usage.UnpricedRecords,
		Stale:            usage.Stale,
		WindowSource:     usage.WindowSource,
	}
}

func (a *App) antigravityKeeperQuotaWindowUsages(ctx context.Context, accounts []antigravityKeeperAccount) (map[string]antigravityKeeperQuotaWindowUsagePair, error) {
	if len(accounts) == 0 {
		return map[string]antigravityKeeperQuotaWindowUsagePair{}, nil
	}
	key, err := a.antigravityKeeperQuotaWindowUsageCacheKey(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now().In(appTimeLocation)
	if cached, ok := a.cachedAntigravityKeeperQuotaWindowUsages(key, now); ok {
		return cached, nil
	}
	usages, err := a.computeAntigravityKeeperQuotaWindowUsages(ctx, accounts, now)
	if err != nil {
		return nil, err
	}
	a.storeAntigravityKeeperQuotaWindowUsages(key, now.Add(antigravityKeeperQuotaWindowUsageCacheTTL), usages)
	return usages, nil
}

func (a *App) antigravityKeeperQuotaWindowUsageCacheKey(ctx context.Context) (string, error) {
	var stateUpdated, usageID sql.NullString
	if err := a.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(CAST(updated_at AS TEXT)), '') FROM antigravity_keeper_auth_states`).Scan(&stateUpdated); err != nil {
		return "", err
	}
	if err := a.db.QueryRowContext(ctx, `SELECT COALESCE(CAST(MAX(id) AS TEXT), '') FROM usage_records`).Scan(&usageID); err != nil {
		return "", err
	}
	return stateUpdated.String + "|" + usageID.String, nil
}

func (a *App) cachedAntigravityKeeperQuotaWindowUsages(key string, now time.Time) (map[string]antigravityKeeperQuotaWindowUsagePair, bool) {
	a.antigravityKeeperUsageCache.mu.Lock()
	defer a.antigravityKeeperUsageCache.mu.Unlock()
	if key == "" || key != a.antigravityKeeperUsageCache.key || !now.Before(a.antigravityKeeperUsageCache.expiresAt) {
		return nil, false
	}
	return copyAntigravityKeeperQuotaWindowUsagePairs(a.antigravityKeeperUsageCache.usages), true
}

func (a *App) storeAntigravityKeeperQuotaWindowUsages(key string, expiresAt time.Time, usages map[string]antigravityKeeperQuotaWindowUsagePair) {
	a.antigravityKeeperUsageCache.mu.Lock()
	defer a.antigravityKeeperUsageCache.mu.Unlock()
	a.antigravityKeeperUsageCache.key = key
	a.antigravityKeeperUsageCache.expiresAt = expiresAt
	a.antigravityKeeperUsageCache.usages = copyAntigravityKeeperQuotaWindowUsagePairs(usages)
}

func copyAntigravityKeeperQuotaWindowUsagePairs(source map[string]antigravityKeeperQuotaWindowUsagePair) map[string]antigravityKeeperQuotaWindowUsagePair {
	if source == nil {
		return map[string]antigravityKeeperQuotaWindowUsagePair{}
	}
	copied := make(map[string]antigravityKeeperQuotaWindowUsagePair, len(source))
	for name, pair := range source {
		copied[name] = antigravityKeeperQuotaWindowUsagePair{
			Primary:   copyAntigravityKeeperQuotaWindowUsage(pair.Primary),
			Secondary: copyAntigravityKeeperQuotaWindowUsage(pair.Secondary),
		}
	}
	return copied
}

func copyAntigravityKeeperQuotaWindowUsage(source *antigravityKeeperQuotaWindowUsage) *antigravityKeeperQuotaWindowUsage {
	if source == nil {
		return nil
	}
	copied := *source
	return &copied
}

func (a *App) computeAntigravityKeeperQuotaWindowUsages(ctx context.Context, accounts []antigravityKeeperAccount, now time.Time) (map[string]antigravityKeeperQuotaWindowUsagePair, error) {
	usages := map[string]antigravityKeeperQuotaWindowUsagePair{}
	sourceAccounts := map[string]string{}
	aliases := map[string][]string{}
	var minStart, maxEnd time.Time
	maxWindowSeconds := 0

	for _, account := range accounts {
		addAntigravityKeeperAuthAlias(aliases, account.Name, account.Name)
		addAntigravityKeeperSourceAccountAlias(sourceAccounts, account.Name, account.Name)
		if account.Email != nil {
			addAntigravityKeeperAuthAlias(aliases, *account.Email, account.Name)
			addAntigravityKeeperSourceAccountAlias(sourceAccounts, *account.Email, account.Name)
		}
		if account.Disabled {
			continue
		}

		pair := antigravityKeeperQuotaWindowPairForAccount(account, now)
		if pair.Primary == nil && pair.Secondary == nil {
			continue
		}
		usages[account.Name] = pair
		minStart, maxEnd = antigravityKeeperQuotaWindowBounds(minStart, maxEnd, pair.Primary)
		minStart, maxEnd = antigravityKeeperQuotaWindowBounds(minStart, maxEnd, pair.Secondary)
		for _, usage := range []*antigravityKeeperQuotaWindowUsage{pair.Primary, pair.Secondary} {
			if usage != nil && usage.WindowSeconds > maxWindowSeconds {
				maxWindowSeconds = usage.WindowSeconds
			}
		}
	}
	if minStart.IsZero() || maxEnd.IsZero() {
		return usages, nil
	}

	queryStart := minStart
	if maxWindowSeconds > 0 {
		maxLookbackStart := now.Add(-time.Duration(maxWindowSeconds) * time.Second)
		if queryStart.Before(maxLookbackStart) {
			queryStart = maxLookbackStart
		}
	}
	records, err := a.antigravityKeeperUsageRecordsInRange(ctx, queryStart, maxEnd)
	if err != nil {
		return nil, err
	}
	prices, err := a.priceMap(ctx)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		recordPrimary, recordSecondary := antigravityKeeperUsageRecordQuotaWindows(record)
		if !recordPrimary && !recordSecondary {
			continue
		}
		accountName, ok := antigravityKeeperAccountNameForUsageRecord(record, sourceAccounts, aliases)
		if !ok {
			continue
		}
		pair, ok := usages[accountName]
		if !ok {
			continue
		}
		if recordPrimary && antigravityKeeperRecordInQuotaWindow(record, pair.Primary) {
			addRecordToAntigravityKeeperQuotaWindowUsage(pair.Primary, record, prices)
		}
		if recordSecondary && antigravityKeeperRecordInQuotaWindow(record, pair.Secondary) {
			addRecordToAntigravityKeeperQuotaWindowUsage(pair.Secondary, record, prices)
		}
	}
	return usages, nil
}

func antigravityKeeperQuotaWindowPairForAccount(account antigravityKeeperAccount, now time.Time) antigravityKeeperQuotaWindowUsagePair {
	return antigravityKeeperQuotaWindowUsagePair{
		Primary:   antigravityKeeperQuotaWindowForAccount(account, true, now),
		Secondary: antigravityKeeperQuotaWindowForAccount(account, false, now),
	}
}

func antigravityKeeperQuotaWindowForAccount(account antigravityKeeperAccount, primary bool, now time.Time) *antigravityKeeperQuotaWindowUsage {
	resetAt := account.PrimaryResetAt
	windowSeconds := account.PrimaryWindowSeconds
	if !primary {
		resetAt = account.SecondaryResetAt
		windowSeconds = account.SecondaryWindowSeconds
	}
	if resetAt == nil {
		return nil
	}
	seconds, source, ok := antigravityKeeperQuotaWindowSeconds(account.AccountType, windowSeconds, primary)
	if !ok {
		return nil
	}
	windowEnd := resetAt.In(appTimeLocation)
	windowStart := windowEnd.Add(-time.Duration(seconds) * time.Second)
	return &antigravityKeeperQuotaWindowUsage{
		WindowStart:   windowStart,
		WindowEnd:     windowEnd,
		ResetAt:       windowEnd,
		WindowSeconds: seconds,
		Stale:         !now.Before(windowEnd),
		WindowSource:  source,
	}
}

func antigravityKeeperQuotaWindowSeconds(accountType *string, saved *int, primary bool) (int, string, bool) {
	if saved != nil && *saved > 0 {
		return *saved, "antigravity", true
	}
	if antigravityKeeperFreeQuotaWindowAccount(accountType) {
		return antigravityKeeperWeekWindowSeconds, "inferred", true
	}
	if antigravityKeeperPaidQuotaWindowAccount(accountType) {
		if primary {
			return antigravityKeeperFiveHourWindowSeconds, "inferred", true
		}
		return antigravityKeeperWeekWindowSeconds, "inferred", true
	}
	return 0, "", false
}

func antigravityKeeperFreeQuotaWindowAccount(accountType *string) bool {
	return strings.ToLower(strings.TrimSpace(valueOr(accountType, ""))) == "free"
}

func antigravityKeeperPaidQuotaWindowAccount(accountType *string) bool {
	normalized := strings.ToLower(strings.TrimSpace(valueOr(accountType, "")))
	return normalized == "plus" || normalized == "team" || strings.HasPrefix(normalized, "pro")
}

func antigravityKeeperQuotaWindowBounds(minStart, maxEnd time.Time, usage *antigravityKeeperQuotaWindowUsage) (time.Time, time.Time) {
	if usage == nil {
		return minStart, maxEnd
	}
	if minStart.IsZero() || usage.WindowStart.Before(minStart) {
		minStart = usage.WindowStart
	}
	if maxEnd.IsZero() || usage.WindowEnd.After(maxEnd) {
		maxEnd = usage.WindowEnd
	}
	return minStart, maxEnd
}

func (a *App) antigravityKeeperUsageRecordsInRange(ctx context.Context, start, end time.Time) ([]UsageRecord, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT id, CAST(timestamp AS TEXT), usage_username, api_key_description, provider, model, reasoning_effort, endpoint, source,
		source_account, request_id, auth, auth_index, latency_ms, ttft_ms, failed, input_tokens, output_tokens, cached_tokens,
		cache_read_tokens, cache_creation_tokens, reasoning_tokens, total_tokens, dedupe_key, raw_json
		FROM usage_records
		WHERE timestamp >= ? AND timestamp < ?
		ORDER BY timestamp`, dbTime(start), dbTime(end))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanUsageRecords(rows)
}

func addAntigravityKeeperSourceAccountAlias(aliases map[string]string, value string, name string) {
	valuePtr := &value
	sourceAccount := sourceAccountFromUsageSource(valuePtr)
	if sourceAccount == nil {
		return
	}
	normalizedName := strings.TrimSpace(name)
	if normalizedName == "" {
		return
	}
	existing, ok := aliases[*sourceAccount]
	if ok && existing != normalizedName {
		aliases[*sourceAccount] = ""
		return
	}
	aliases[*sourceAccount] = normalizedName
}

func antigravityKeeperAccountNameForUsageRecord(record UsageRecord, sourceAccounts map[string]string, aliases map[string][]string) (string, bool) {
	if sourceAccount := antigravityKeeperUsageRecordSourceAccount(record); sourceAccount != "" {
		if name, ok := sourceAccounts[sourceAccount]; ok && name != "" {
			return name, true
		}
		return "", false
	}

	identifiers := []string{}
	seen := map[string]bool{}
	addIdentifier := func(value *string) {
		if value == nil {
			return
		}
		normalized := strings.TrimSpace(*value)
		key := antigravityKeeperAuthAliasKey(normalized)
		if key == "" || seen[key] {
			return
		}
		seen[key] = true
		identifiers = append(identifiers, normalized)
	}
	addIdentifier(record.AuthIndex)
	addIdentifier(record.Source)
	for _, field := range []string{"auth_index", "authIndex", "index", "auth_name", "authName", "account_id", "accountId", "email", "account_email", "accountEmail", "user_email", "userEmail"} {
		addIdentifier(rawJSONStringField(record.RawJSON, field))
	}
	for _, identifier := range identifiers {
		if name, ok := antigravityKeeperSingleAuthNameForUsageIdentifier(identifier, aliases); ok {
			return name, true
		}
	}
	return "", false
}

func antigravityKeeperUsageRecordSourceAccount(record UsageRecord) string {
	if record.SourceAccount != nil {
		return strings.ToLower(strings.TrimSpace(*record.SourceAccount))
	}
	if sourceAccount := sourceAccountFromUsageSource(record.Source); sourceAccount != nil {
		return *sourceAccount
	}
	return ""
}

func antigravityKeeperUsageRecordQuotaWindows(record UsageRecord) (bool, bool) {
	if !antigravityKeeperUsageRecordIsAntigravity(record) {
		return false, false
	}
	model := strings.ToLower(strings.TrimSpace(valueOr(record.Model, "")))
	if model == "" {
		return false, false
	}
	return antigravityKeeperModelMatchesPrefixes(model, antigravityKeeperPrimaryUsageModelPrefixes),
		antigravityKeeperModelMatchesPrefixes(model, antigravityKeeperSecondaryUsageModelPrefixes)
}

func antigravityKeeperUsageRecordIsAntigravity(record UsageRecord) bool {
	provider := strings.ToLower(strings.TrimSpace(valueOr(record.Provider, "")))
	if provider == "antigravity" {
		return true
	}
	if rawProvider := rawJSONStringField(record.RawJSON, "provider"); rawProvider != nil {
		return strings.EqualFold(strings.TrimSpace(*rawProvider), "antigravity")
	}
	return false
}

func antigravityKeeperModelMatchesPrefixes(model string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(model, prefix) {
			return true
		}
	}
	return false
}

func antigravityKeeperSingleAuthNameForUsageIdentifier(identifier string, aliases map[string][]string) (string, bool) {
	names := antigravityKeeperAuthNamesForUsageIdentifier(identifier, aliases)
	if len(names) != 1 {
		return "", false
	}
	normalized := strings.TrimSpace(names[0])
	if normalized == "" {
		return "", false
	}
	return normalized, true
}

func antigravityKeeperRecordInQuotaWindow(record UsageRecord, usage *antigravityKeeperQuotaWindowUsage) bool {
	if usage == nil {
		return false
	}
	return !record.Timestamp.Before(usage.WindowStart) && record.Timestamp.Before(usage.WindowEnd)
}

func addRecordToAntigravityKeeperQuotaWindowUsage(usage *antigravityKeeperQuotaWindowUsage, record UsageRecord, prices map[[2]string]ModelPrice) {
	if usage == nil {
		return
	}
	usage.Records++
	if record.Failed {
		usage.FailedRecords++
	} else {
		usage.SuccessRecords++
	}
	usage.InputTokens += usageAggregateInputTokens(record)
	usage.OutputTokens += record.OutputTokens
	usage.CachedTokens += record.CachedTokens
	usage.ReasoningTokens += record.ReasoningTokens
	usage.TotalTokens += usageAggregateTotalTokens(record)
	amount, unpriced := recordCost(record, prices)
	if unpriced {
		usage.UnpricedRecords++
		return
	}
	usage.EstimatedCostUSD = mathRound(usage.EstimatedCostUSD+amount, 8)
}

func (a *App) updateAntigravityKeeperSettings(w http.ResponseWriter, r *http.Request) error {
	var payload antigravityKeeperSettingsUpdateRequest
	if err := decodeJSON(r, &payload); err != nil {
		return err
	}
	cfg, err := a.loadConfig(r.Context())
	if err != nil {
		return err
	}
	if payload.ScheduleCron != nil {
		_, normalized, err := nextRunTimes(*payload.ScheduleCron, 5, time.Now())
		if err != nil {
			return err
		}
		cfg.AntigravityKeeper.ScheduleCron = normalized
	}
	if payload.QuotaThreshold != nil {
		if *payload.QuotaThreshold < 0 || *payload.QuotaThreshold > 100 {
			return validationError("quota_threshold 超出范围")
		}
		cfg.AntigravityKeeper.QuotaThreshold = *payload.QuotaThreshold
	}
	if payload.UsageTimeoutSeconds != nil {
		if *payload.UsageTimeoutSeconds < 1 {
			return validationError("usage_timeout_seconds 不能小于 1")
		}
		cfg.AntigravityKeeper.UsageTimeoutSeconds = *payload.UsageTimeoutSeconds
	}
	if payload.CPATimeoutSeconds != nil {
		if *payload.CPATimeoutSeconds < 1 {
			return validationError("cpa_timeout_seconds 不能小于 1")
		}
		cfg.AntigravityKeeper.CPATimeoutSeconds = *payload.CPATimeoutSeconds
	}
	if payload.MaxRetries != nil {
		if *payload.MaxRetries < 0 || *payload.MaxRetries > 5 {
			return validationError("max_retries 超出范围")
		}
		cfg.AntigravityKeeper.MaxRetries = *payload.MaxRetries
	}
	if payload.WorkerThreads != nil {
		if *payload.WorkerThreads < 1 || *payload.WorkerThreads > 64 {
			return validationError("worker_threads 超出范围")
		}
		cfg.AntigravityKeeper.WorkerThreads = *payload.WorkerThreads
	}
	if payload.ConditionalRefreshIntervalSeconds != nil {
		if !validKeeperConditionalRefreshInterval(*payload.ConditionalRefreshIntervalSeconds) {
			return validationError("conditional_refresh_interval_seconds 超出范围")
		}
		cfg.AntigravityKeeper.ConditionalRefreshIntervalSeconds = *payload.ConditionalRefreshIntervalSeconds
	}
	if payload.AccountRefreshCacheMinutes != nil {
		if *payload.AccountRefreshCacheMinutes < 1 {
			return validationError("account_refresh_cache_minutes 不能小于 1")
		}
		cfg.AntigravityKeeper.AccountRefreshCacheMinutes = *payload.AccountRefreshCacheMinutes
	}
	if payload.DryRun != nil {
		cfg.AntigravityKeeper.DryRun = *payload.DryRun
	}
	if payload.EnableCredentialWebsockets != nil {
		cfg.AntigravityKeeper.EnableCredentialWebsockets = *payload.EnableCredentialWebsockets
	}
	if payload.AutoStartDaemon != nil {
		cfg.AntigravityKeeper.AutoStartDaemon = *payload.AutoStartDaemon
	}
	if payload.PriorityRules != nil {
		rules := map[string]int{}
		for _, item := range payload.PriorityRules {
			key := strings.ToLower(strings.TrimSpace(item.AccountType))
			if key == "" {
				return validationError("账号类型不能为空")
			}
			if item.Priority < 0 || item.Priority > 20 {
				return validationError("priority 超出范围")
			}
			rules[key] = item.Priority
		}
		cfg.AntigravityKeeperPriorityRule = normalizePriorityRules(rules)
	}
	if err := a.saveConfig(r.Context(), cfg); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, antigravityKeeperSettingsResponse(cfg))
	return nil
}

func (a *App) executeAntigravityKeeperRun(ctx context.Context, mode string, logFn func(string)) (antigravityKeeperStats, string, error) {
	return a.executeAntigravityKeeperRunForAccounts(ctx, mode, nil, logFn)
}

type antigravityKeeperRunOptions struct {
	Mode            string
	AuthNames       []string
	ManualRefresh   bool
	UseRefreshCache bool
	PersistRun      bool
	TryLockAuthName func(string, string) bool
	UnlockAuthName  func(string)
}

func (a *App) executeAntigravityKeeperRunForAccounts(ctx context.Context, mode string, authNames []string, logFn func(string)) (antigravityKeeperStats, string, error) {
	return a.executeAntigravityKeeperRunWithOptions(ctx, antigravityKeeperRunOptionsForMode(mode, authNames), logFn)
}

func antigravityKeeperRunOptionsForMode(mode string, authNames []string) antigravityKeeperRunOptions {
	return antigravityKeeperRunOptions{
		Mode:            mode,
		AuthNames:       authNames,
		ManualRefresh:   mode == "accounts",
		UseRefreshCache: mode == "daemon" || mode == "conditional",
		PersistRun:      antigravityKeeperModePersistsRun(mode),
	}
}

func antigravityKeeperModePersistsRun(mode string) bool {
	return mode != "accounts" && mode != "conditional"
}

func (a *App) executeAntigravityKeeperRunWithOptions(ctx context.Context, options antigravityKeeperRunOptions, logFn func(string)) (antigravityKeeperStats, string, error) {
	cfg, err := a.loadConfig(ctx)
	if err != nil {
		return antigravityKeeperStats{}, "", err
	}
	if strings.TrimSpace(cfg.Collector.ManagementKey) == "" {
		return antigravityKeeperStats{}, "", validationError("管理密钥未设置，无法运行 Antigravity Keeper")
	}
	runID := 0
	if options.PersistRun {
		runID, err = a.createAntigravityKeeperRun(ctx, options.Mode)
		if err != nil {
			return antigravityKeeperStats{}, "", err
		}
	}
	targetNames, err := normalizeOptionalAntigravityKeeperAuthNames(options.AuthNames)
	if err != nil {
		if runID > 0 {
			_ = a.finishAntigravityKeeperRun(ctx, runID, "failed", err.Error(), antigravityKeeperStats{})
		}
		return antigravityKeeperStats{}, "", err
	}
	targetSet := map[string]bool{}
	for _, name := range targetNames {
		targetSet[name] = true
	}
	if options.Mode == "conditional" {
		logFn(fmt.Sprintf("开始按条件刷新 %d 个 Antigravity 账号", len(targetSet)))
	} else if len(targetSet) > 0 {
		logFn(fmt.Sprintf("开始刷新 %d 个 Antigravity 账号", len(targetSet)))
	} else {
		logFn("开始 Antigravity 账号巡检")
	}
	stats := antigravityKeeperStats{}
	detail := "巡检完成"
	authFiles, err := a.listAntigravityKeeperRemoteAuthFiles(ctx, cfg)
	if err != nil {
		if runID > 0 {
			_ = a.finishAntigravityKeeperRun(ctx, runID, "failed", err.Error(), stats)
		}
		return stats, "", err
	}
	filtered := make([]map[string]any, 0, len(authFiles))
	remoteAntigravityNames := map[string]bool{}
	for _, item := range authFiles {
		if antigravityKeeperString(item["type"]) != "antigravity" {
			continue
		}
		name := antigravityKeeperString(item["name"])
		if name != "" {
			remoteAntigravityNames[name] = true
		}
		if len(targetSet) == 0 || targetSet[name] {
			filtered = append(filtered, item)
		}
	}
	if len(targetSet) == 0 {
		pruned, err := a.pruneAntigravityKeeperMissingAuthStates(ctx, remoteAntigravityNames)
		if err != nil {
			if runID > 0 {
				_ = a.finishAntigravityKeeperRun(ctx, runID, "failed", err.Error(), stats)
			}
			return stats, "", err
		}
		if pruned > 0 {
			logFn(fmt.Sprintf("清理本地已不存在的 Antigravity 账号 %d 个", pruned))
		}
	}
	stats.Total = len(filtered)
	if cfg.AntigravityKeeper.EnableCredentialWebsockets && !cfg.AntigravityKeeper.DryRun {
		var websocketFailures []antigravityKeeperAccountResult
		filtered, websocketFailures = a.ensureAntigravityKeeperAuthWebsockets(ctx, cfg, options.Mode, filtered, logFn, options.TryLockAuthName, options.UnlockAuthName)
		for _, result := range websocketFailures {
			a.mergeAntigravityKeeperStats(&stats, result)
			if runID > 0 {
				if err := a.recordAntigravityKeeperRunAccount(ctx, runID, result); err != nil {
					logFn("写入巡检账号历史失败：" + err.Error())
				}
			}
		}
	}
	if options.UseRefreshCache {
		var skippedNames []string
		filtered, skippedNames, err = a.filterAntigravityKeeperCachedAuthItems(ctx, filtered, cfg)
		if err != nil {
			if runID > 0 {
				_ = a.finishAntigravityKeeperRun(ctx, runID, "failed", err.Error(), stats)
			}
			return stats, "", err
		}
		stats.Skipped += len(skippedNames)
		if err := a.addAntigravityKeeperCachedAuthStats(ctx, &stats, skippedNames); err != nil {
			if runID > 0 {
				_ = a.finishAntigravityKeeperRun(ctx, runID, "failed", err.Error(), stats)
			}
			return stats, "", err
		}
	}
	if len(filtered) == 0 {
		if stats.NetworkError > 0 {
			detail = fmt.Sprintf("巡检完成：网络错误 %d", stats.NetworkError)
		} else if stats.Total > 0 && options.UseRefreshCache {
			detail = "缓存时间内没有需要自动刷新的 Antigravity auth file"
		} else if len(targetSet) > 0 {
			detail = "未发现指定 Antigravity auth file"
		} else {
			detail = "未发现 Antigravity auth file"
		}
		if runID > 0 {
			_ = a.finishAntigravityKeeperRun(ctx, runID, "completed", detail, stats)
		}
		return stats, detail, nil
	}
	for _, item := range filtered {
		name := antigravityKeeperString(item["name"])
		locked := false
		if options.TryLockAuthName != nil && name != "" {
			if !options.TryLockAuthName(options.Mode, name) {
				stats.Skipped++
				logFn(name + ": 正在其他 Keeper 任务处理中，跳过")
				continue
			}
			locked = true
		}
		unlock := func() {
			if locked && options.UnlockAuthName != nil {
				options.UnlockAuthName(name)
				locked = false
			}
		}
		if locked && options.UseRefreshCache {
			cutoff := time.Now().In(appTimeLocation).Add(-antigravityKeeperRefreshCacheDuration(cfg))
			cached, err := a.antigravityKeeperAuthCheckedSince(ctx, name, cutoff)
			if err != nil {
				unlock()
				if runID > 0 {
					_ = a.finishAntigravityKeeperRun(ctx, runID, "failed", err.Error(), stats)
				}
				return stats, "", err
			}
			if cached {
				unlock()
				stats.Skipped++
				if err := a.addAntigravityKeeperCachedAuthStats(ctx, &stats, []string{name}); err != nil {
					if runID > 0 {
						_ = a.finishAntigravityKeeperRun(ctx, runID, "failed", err.Error(), stats)
					}
					return stats, "", err
				}
				logFn(name + ": 缓存时间内已刷新，跳过")
				continue
			}
		}
		result := a.processAntigravityKeeperAuth(ctx, cfg, item, logFn, options.ManualRefresh)
		unlock()
		a.mergeAntigravityKeeperStats(&stats, result)
		if runID > 0 {
			if err := a.recordAntigravityKeeperRunAccount(ctx, runID, result); err != nil {
				logFn("写入巡检账号历史失败：" + err.Error())
			}
		}
	}
	if options.Mode == "conditional" {
		detail = fmt.Sprintf("条件刷新完成：健康 %d，坏凭证禁用 %d，恢复启用 %d，优先级降级 %d，优先级恢复 %d，网络错误 %d，缓存跳过 %d", stats.Healthy, stats.StatusDisabled, stats.StatusEnabled, stats.PriorityDegraded, stats.PriorityRestored, stats.NetworkError, stats.Skipped)
	} else if len(targetSet) > 0 {
		detail = fmt.Sprintf("账号刷新完成：健康 %d，凭证异常 %d，恢复启用 %d，优先级降级 %d，优先级恢复 %d，网络错误 %d", stats.Healthy, stats.StatusDisabled, stats.StatusEnabled, stats.PriorityDegraded, stats.PriorityRestored, stats.NetworkError)
	} else {
		detail = fmt.Sprintf("巡检完成：健康 %d，坏凭证禁用 %d，恢复启用 %d，优先级降级 %d，网络错误 %d，缓存跳过 %d", stats.Healthy, stats.StatusDisabled, stats.StatusEnabled, stats.PriorityDegraded, stats.NetworkError, stats.Skipped)
	}
	if runID > 0 {
		_ = a.finishAntigravityKeeperRun(ctx, runID, "completed", detail, stats)
	}
	return stats, detail, nil
}

func antigravityKeeperRefreshCacheDuration(cfg AppConfig) time.Duration {
	minutes := cfg.AntigravityKeeper.AccountRefreshCacheMinutes
	if minutes < 1 {
		minutes = 10
	}
	return time.Duration(minutes) * time.Minute
}

func (a *App) conditionalAntigravityKeeperRefreshCandidates(ctx context.Context, cfg AppConfig) ([]string, error) {
	cacheWindow := antigravityKeeperRefreshCacheDuration(cfg)
	since := time.Now().In(appTimeLocation).Add(-cacheWindow)
	aliases, err := a.antigravityKeeperAuthNameAliases(ctx)
	if err != nil {
		return nil, err
	}
	names := []string{}
	seen := map[string]bool{}
	usageIdentifiers := []string{}
	seenUsageIdentifiers := map[string]bool{}
	addName := func(name string) {
		normalized := strings.TrimSpace(name)
		if normalized == "" || seen[normalized] {
			return
		}
		seen[normalized] = true
		names = append(names, normalized)
	}
	addUsageIdentifier := func(identifier string, allowOpaque bool) bool {
		normalized := strings.TrimSpace(identifier)
		aliasKey := antigravityKeeperAuthAliasKey(normalized)
		if aliasKey == "" || seenUsageIdentifiers[aliasKey] {
			return len(antigravityKeeperAuthNamesForUsageIdentifier(normalized, aliases)) > 0
		}
		if !allowOpaque && !antigravityKeeperLooksLikeAuthIdentifier(normalized, aliases) {
			return false
		}
		seenUsageIdentifiers[aliasKey] = true
		usageIdentifiers = append(usageIdentifiers, normalized)
		resolved := false
		for _, name := range antigravityKeeperAuthNamesForUsageIdentifier(normalized, aliases) {
			addName(name)
			resolved = true
		}
		return resolved
	}

	rows, err := a.db.QueryContext(ctx, `
		SELECT source, raw_json
		FROM usage_records
		WHERE timestamp >= ?
		ORDER BY timestamp DESC
	`, dbTime(since))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var source sql.NullString
		var rawJSON string
		if err := rows.Scan(&source, &rawJSON); err != nil {
			_ = rows.Close()
			return nil, err
		}
		sourceResolved := false
		if source.Valid {
			sourceResolved = addUsageIdentifier(source.String, false)
		}
		if identifier := rawJSONStringField(rawJSON, "source"); identifier != nil {
			sourceResolved = addUsageIdentifier(*identifier, false) || sourceResolved
		}
		if sourceResolved {
			continue
		}
		for _, field := range []string{"auth_index", "authIndex", "index", "auth_name", "authName", "account_id", "accountId"} {
			if identifier := rawJSONStringField(rawJSON, field); identifier != nil {
				addUsageIdentifier(*identifier, true)
			}
		}
		for _, field := range []string{"email", "account_email", "accountEmail", "user_email", "userEmail"} {
			if identifier := rawJSONStringField(rawJSON, field); identifier != nil {
				addUsageIdentifier(*identifier, false)
			}
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, name := range a.antigravityKeeperAuthNamesFromRemoteUsageIdentifiers(ctx, cfg, usageIdentifiers, aliases) {
		addName(name)
	}
	if err := a.reconcileAntigravityKeeperConditionalRemoteAuthStates(ctx, cfg, addName); err != nil {
		return nil, err
	}

	rows, err = a.db.QueryContext(ctx, `
		SELECT auth_name
		FROM antigravity_keeper_auth_states
		WHERE disabled = 0
		  AND (
		      (primary_reset_at IS NOT NULL AND primary_reset_at <= ?)
		   OR (secondary_reset_at IS NOT NULL AND secondary_reset_at <= ?)
		  )
		ORDER BY auth_name
	`, dbTime(time.Now().In(appTimeLocation)), dbTime(time.Now().In(appTimeLocation)))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, err
		}
		addName(name)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = a.db.QueryContext(ctx, `
		SELECT auth_name
		FROM antigravity_keeper_auth_states
		WHERE disabled = 0
		  AND last_error IS NOT NULL
		  AND TRIM(last_error) <> ''
		ORDER BY auth_name
	`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, err
		}
		addName(name)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	enabledNames, err := a.filterAntigravityKeeperEnabledAuthNames(ctx, names)
	if err != nil {
		return nil, err
	}
	filtered, _, err := a.filterAntigravityKeeperCachedAuthNames(ctx, enabledNames, cfg)
	return filtered, err
}

func (a *App) reconcileAntigravityKeeperConditionalRemoteAuthStates(ctx context.Context, cfg AppConfig, addName func(string)) error {
	if strings.TrimSpace(cfg.Collector.CLIProxyURL) == "" {
		return nil
	}
	authFiles, err := a.listAntigravityKeeperRemoteAuthFiles(ctx, cfg)
	if err != nil {
		return err
	}
	remoteNames := map[string]bool{}
	refreshableRemoteNames := map[string]bool{}
	for _, item := range authFiles {
		if antigravityKeeperString(item["type"]) != "antigravity" {
			continue
		}
		name := antigravityKeeperString(item["name"])
		if name == "" {
			continue
		}
		remoteNames[name] = true
		if !antigravityKeeperBool(item["disabled"]) {
			refreshableRemoteNames[name] = true
		}
	}
	localNames, err := a.antigravityKeeperAuthStateNameSet(ctx)
	if err != nil {
		return err
	}
	for name := range refreshableRemoteNames {
		if !localNames[name] && !a.antigravityKeeperRemoteAuthDisabledForConditional(ctx, cfg, name, authFiles) {
			addName(name)
		}
	}
	_, err = a.pruneAntigravityKeeperMissingAuthStates(ctx, remoteNames)
	return err
}

func (a *App) antigravityKeeperRemoteAuthDisabledForConditional(ctx context.Context, cfg AppConfig, name string, authFiles []map[string]any) bool {
	for _, item := range authFiles {
		if antigravityKeeperString(item["name"]) != name {
			continue
		}
		if antigravityKeeperBool(item["disabled"]) {
			return true
		}
		if _, ok := item["disabled"]; ok {
			return false
		}
		detail, err := a.getAntigravityKeeperRemoteAuthFile(ctx, cfg, name)
		if err != nil || detail == nil {
			return false
		}
		return antigravityKeeperBool(mergeAntigravityKeeperObjects(item, detail)["disabled"])
	}
	return false
}

func (a *App) antigravityKeeperAuthStateNameSet(ctx context.Context) (map[string]bool, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT auth_name FROM antigravity_keeper_auth_states`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

func (a *App) antigravityKeeperAuthNameAliases(ctx context.Context) (map[string][]string, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT auth_name, email
		FROM antigravity_keeper_auth_states
		WHERE disabled = 0
		ORDER BY auth_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	aliases := map[string][]string{}
	for rows.Next() {
		var name string
		var email sql.NullString
		if err := rows.Scan(&name, &email); err != nil {
			return nil, err
		}
		addAntigravityKeeperAuthAlias(aliases, name, name)
		if email.Valid {
			addAntigravityKeeperAuthAlias(aliases, email.String, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return aliases, nil
}

func (a *App) antigravityKeeperAuthNamesFromRemoteUsageIdentifiers(ctx context.Context, cfg AppConfig, identifiers []string, aliases map[string][]string) []string {
	if !antigravityKeeperHasUnresolvedUsageIdentifiers(identifiers, aliases) || strings.TrimSpace(cfg.Collector.CLIProxyURL) == "" {
		return nil
	}
	authFiles, err := a.listAntigravityKeeperRemoteAuthFiles(ctx, cfg)
	if err != nil {
		return nil
	}
	antigravityFiles := make([]map[string]any, 0, len(authFiles))
	for _, item := range authFiles {
		if antigravityKeeperString(item["type"]) != "antigravity" {
			continue
		}
		if antigravityKeeperBool(item["disabled"]) {
			continue
		}
		antigravityFiles = append(antigravityFiles, item)
		addAntigravityKeeperAuthObjectAliases(aliases, item)
	}
	if !antigravityKeeperHasUnresolvedUsageIdentifiers(identifiers, aliases) {
		return antigravityKeeperAuthNamesForUsageIdentifiers(identifiers, aliases)
	}
	for _, item := range antigravityFiles {
		name := antigravityKeeperString(item["name"])
		if name == "" {
			continue
		}
		detail, err := a.getAntigravityKeeperRemoteAuthFile(ctx, cfg, name)
		if err != nil || detail == nil {
			continue
		}
		merged := mergeAntigravityKeeperObjects(item, detail)
		if antigravityKeeperBool(merged["disabled"]) {
			continue
		}
		addAntigravityKeeperAuthObjectAliases(aliases, merged)
		if !antigravityKeeperHasUnresolvedUsageIdentifiers(identifiers, aliases) {
			break
		}
	}
	return antigravityKeeperAuthNamesForUsageIdentifiers(identifiers, aliases)
}

func antigravityKeeperHasUnresolvedUsageIdentifiers(identifiers []string, aliases map[string][]string) bool {
	for _, identifier := range identifiers {
		if len(antigravityKeeperAuthNamesForUsageIdentifier(identifier, aliases)) == 0 {
			return true
		}
	}
	return false
}

func antigravityKeeperAuthNamesForUsageIdentifiers(identifiers []string, aliases map[string][]string) []string {
	names := []string{}
	seen := map[string]bool{}
	for _, identifier := range identifiers {
		for _, name := range antigravityKeeperAuthNamesForUsageIdentifier(identifier, aliases) {
			normalized := strings.TrimSpace(name)
			if normalized == "" || seen[normalized] {
				continue
			}
			seen[normalized] = true
			names = append(names, normalized)
		}
	}
	return names
}

func antigravityKeeperAuthNamesForUsageIdentifier(identifier string, aliases map[string][]string) []string {
	normalized := strings.TrimSpace(identifier)
	if normalized == "" {
		return nil
	}
	if names := aliases[antigravityKeeperAuthAliasKey(normalized)]; len(names) > 0 {
		return names
	}
	if strings.HasSuffix(strings.ToLower(normalized), ".json") {
		return []string{normalized}
	}
	return nil
}

func antigravityKeeperLooksLikeAuthIdentifier(identifier string, aliases map[string][]string) bool {
	normalized := strings.TrimSpace(identifier)
	if normalized == "" {
		return false
	}
	if len(aliases[antigravityKeeperAuthAliasKey(normalized)]) > 0 {
		return true
	}
	lower := strings.ToLower(normalized)
	return strings.HasSuffix(lower, ".json") || strings.Contains(normalized, "@")
}

func addAntigravityKeeperAuthObjectAliases(aliases map[string][]string, object map[string]any) {
	name := antigravityKeeperString(object["name"])
	if name == "" {
		return
	}
	addAntigravityKeeperAuthAlias(aliases, name, name)
	for _, key := range []string{"auth_name", "authName", "auth_index", "authIndex", "index", "source", "email", "account_email", "accountEmail", "user_email", "userEmail", "account_id", "accountId"} {
		if value := antigravityKeeperAliasString(object[key]); value != "" {
			addAntigravityKeeperAuthAlias(aliases, value, name)
		}
	}
}

func addAntigravityKeeperAuthAlias(aliases map[string][]string, alias string, name string) {
	normalizedAlias := antigravityKeeperAuthAliasKey(alias)
	normalizedName := strings.TrimSpace(name)
	if normalizedAlias == "" || normalizedName == "" {
		return
	}
	for _, existing := range aliases[normalizedAlias] {
		if existing == normalizedName {
			return
		}
	}
	aliases[normalizedAlias] = append(aliases[normalizedAlias], normalizedName)
}

func antigravityKeeperAliasString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return strings.TrimSpace(strconv.FormatFloat(typed, 'f', -1, 64))
	case int:
		return strconv.Itoa(typed)
	default:
		return ""
	}
}

func antigravityKeeperAuthAliasKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func (a *App) filterAntigravityKeeperCachedAuthItems(ctx context.Context, items []map[string]any, cfg AppConfig) ([]map[string]any, []string, error) {
	if len(items) == 0 {
		return items, nil, nil
	}
	names := make([]string, 0, len(items))
	for _, item := range items {
		if name := antigravityKeeperString(item["name"]); name != "" {
			names = append(names, name)
		}
	}
	allowedNames, skippedNames, err := a.filterAntigravityKeeperCachedAuthNames(ctx, names, cfg)
	if err != nil {
		return nil, nil, err
	}
	allowed := map[string]bool{}
	for _, name := range allowedNames {
		allowed[name] = true
	}
	filtered := make([]map[string]any, 0, len(items))
	for _, item := range items {
		name := antigravityKeeperString(item["name"])
		if name == "" || allowed[name] {
			filtered = append(filtered, item)
		}
	}
	return filtered, skippedNames, nil
}

func (a *App) filterAntigravityKeeperCachedAuthNames(ctx context.Context, names []string, cfg AppConfig) ([]string, []string, error) {
	normalized, err := normalizeOptionalAntigravityKeeperAuthNames(names)
	if err != nil {
		return nil, nil, err
	}
	if len(normalized) == 0 {
		return normalized, nil, nil
	}
	cutoff := time.Now().In(appTimeLocation).Add(-antigravityKeeperRefreshCacheDuration(cfg))
	filtered := make([]string, 0, len(normalized))
	skipped := []string{}
	for _, name := range normalized {
		cached, err := a.antigravityKeeperAuthCheckedSince(ctx, name, cutoff)
		if err != nil {
			return nil, nil, err
		}
		if cached {
			skipped = append(skipped, name)
			continue
		}
		filtered = append(filtered, name)
	}
	return filtered, skipped, nil
}

func (a *App) filterAntigravityKeeperEnabledAuthNames(ctx context.Context, names []string) ([]string, error) {
	normalized, err := normalizeOptionalAntigravityKeeperAuthNames(names)
	if err != nil {
		return nil, err
	}
	if len(normalized) == 0 {
		return normalized, nil
	}
	rows, err := a.db.QueryContext(ctx, `
		SELECT auth_name
		FROM antigravity_keeper_auth_states
		WHERE disabled = 1
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	disabledNames := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		disabledNames[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(normalized))
	for _, name := range normalized {
		if disabledNames[name] {
			continue
		}
		filtered = append(filtered, name)
	}
	return filtered, nil
}

func (a *App) addAntigravityKeeperCachedAuthStats(ctx context.Context, stats *antigravityKeeperStats, names []string) error {
	cachedStats, err := a.antigravityKeeperCachedAuthStats(ctx, names)
	if err != nil {
		return err
	}
	stats.add(cachedStats)
	return nil
}

func (a *App) antigravityKeeperCachedAuthStats(ctx context.Context, names []string) (antigravityKeeperStats, error) {
	normalized, err := normalizeOptionalAntigravityKeeperAuthNames(names)
	if err != nil {
		return antigravityKeeperStats{}, err
	}
	stats := antigravityKeeperStats{}
	for _, name := range normalized {
		state, err := a.getAntigravityKeeperState(ctx, name)
		if err != nil {
			var appErr *AppError
			if errors.As(err, &appErr) && appErr.Code == "not_found" {
				continue
			}
			return antigravityKeeperStats{}, err
		}
		stats.mergeCachedState(*state)
	}
	return stats, nil
}

func (a *App) antigravityKeeperAuthCheckedSince(ctx context.Context, name string, cutoff time.Time) (bool, error) {
	var lastChecked sql.NullString
	err := a.db.QueryRowContext(ctx, `
		SELECT CAST(last_checked_at AS TEXT)
		FROM antigravity_keeper_auth_states
		WHERE auth_name = ?
	`, name).Scan(&lastChecked)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !lastChecked.Valid {
		return false, nil
	}
	checkedAt, ok := parseDBTime(lastChecked.String)
	if !ok {
		return false, nil
	}
	return checkedAt.After(cutoff) || checkedAt.Equal(cutoff), nil
}

func (a *App) ensureAntigravityKeeperAuthWebsockets(
	ctx context.Context,
	cfg AppConfig,
	mode string,
	items []map[string]any,
	logFn func(string),
	tryLock func(string, string) bool,
	unlock func(string),
) ([]map[string]any, []antigravityKeeperAccountResult) {
	if len(items) == 0 {
		return items, nil
	}
	remaining := make([]map[string]any, 0, len(items))
	failures := []antigravityKeeperAccountResult{}
	now := time.Now().In(appTimeLocation)
	for _, item := range items {
		name := antigravityKeeperString(item["name"])
		if name == "" || antigravityKeeperBool(item["websockets"]) {
			remaining = append(remaining, item)
			continue
		}
		locked := false
		if tryLock != nil && unlock != nil {
			if !tryLock(mode, name) {
				remaining = append(remaining, item)
				continue
			}
			locked = true
		}
		unlockIfNeeded := func() {
			if locked && unlock != nil {
				unlock(name)
				locked = false
			}
		}
		if err := a.setAntigravityKeeperRemoteWebsockets(ctx, cfg, name); err != nil {
			message := "启用 WebSocket 传输失败：" + err.Error()
			disabled := antigravityKeeperBool(item["disabled"])
			result := antigravityKeeperAccountResult{
				Name:         name,
				AccountType:  antigravityKeeperStringPtr(item["account_type"], item["accountType"]),
				Disabled:     &disabled,
				Priority:     antigravityKeeperIntPtr(item["priority"]),
				Result:       "network_error",
				LastError:    &message,
				LatestAction: &message,
				CheckedAt:    now,
			}
			_ = a.upsertAntigravityKeeperState(ctx, result)
			logFn(name + ": " + message)
			failures = append(failures, result)
			unlockIfNeeded()
			continue
		}
		item["websockets"] = true
		unlockIfNeeded()
		logFn(name + ": 已启用 WebSocket 传输")
		remaining = append(remaining, item)
	}
	return remaining, failures
}

func (a *App) processAntigravityKeeperAuth(ctx context.Context, cfg AppConfig, authInfo map[string]any, logFn func(string), manualRefresh bool) antigravityKeeperAccountResult {
	now := time.Now().In(appTimeLocation)
	name := antigravityKeeperString(authInfo["name"])
	if name == "" {
		name = "unknown"
	}
	result := antigravityKeeperAccountResult{Name: name, Result: "skipped", CheckedAt: now}
	detail, err := a.getAntigravityKeeperRemoteAuthFile(ctx, cfg, name)
	if err != nil {
		message := "读取 auth file 详情失败：" + err.Error()
		result.Result = "network_error"
		result.LastError = &message
		result.LatestAction = &message
		_ = a.upsertAntigravityKeeperState(ctx, result)
		logFn(name + ": " + message)
		return result
	}
	if detail == nil {
		message := "读取 auth file 详情失败"
		result.Result = "network_error"
		result.LastError = &message
		result.LatestAction = &message
		_ = a.upsertAntigravityKeeperState(ctx, result)
		return result
	}
	merged := mergeAntigravityKeeperObjects(authInfo, detail)
	result.Email = antigravityKeeperStringPtr(merged["email"], merged["account_email"], merged["user_email"])
	result.Priority = antigravityKeeperIntPtr(merged["priority"])
	disabled := antigravityKeeperBool(merged["disabled"])
	result.Disabled = &disabled
	result.AccountType = accountTypeFromAntigravityKeeperDetail(merged, nil)
	var state *antigravityKeeperAuthState
	var restorePriority *int
	if loadedState, err := a.getAntigravityKeeperState(ctx, name); err == nil {
		state = loadedState
		restorePriority = loadedState.RestorePriority
	}
	recoverableUnauthorizedDisabled := disabled && isAntigravityKeeperRecoverableUnauthorizedDisabledState(state)
	if disabled && !manualRefresh && !recoverableUnauthorizedDisabled {
		result.Result = "disabled"
		a.preserveAntigravityKeeperBadCredentialDiagnosis(ctx, &result)
		_ = a.upsertAntigravityKeeperState(ctx, result)
		return result
	}
	if antigravityKeeperString(merged["access_token"]) == "" {
		message := "缺少 access token"
		action := "刷新发现凭证不可用：" + message
		if !cfg.AntigravityKeeper.DryRun {
			if !disabled {
				if err := a.setAntigravityKeeperRemoteDisabled(ctx, cfg, name, true); err != nil {
					message = "禁用坏凭证失败：" + err.Error()
					result.LastError = &message
					result.Result = "network_error"
					_ = a.upsertAntigravityKeeperState(ctx, result)
					return result
				}
			}
			_ = a.setAntigravityKeeperRemotePriority(ctx, cfg, name, nil)
			disabled = true
			result.Disabled = &disabled
			result.Priority = nil
			action = "禁用凭证：" + message
		} else {
			action = "模拟禁用：" + message
		}
		result.Result = "status_disabled"
		result.LastError = &message
		result.LatestAction = &action
		_ = a.upsertAntigravityKeeperState(ctx, result)
		logFn(name + ": " + action)
		return result
	}

	usageResult := a.checkAntigravityKeeperUsage(ctx, cfg, merged)
	if usageResult.StatusCode == nil {
		message := "网络检测失败：" + usageResult.Error
		result.Result = "network_error"
		result.LastError = &message
		result.LatestAction = &message
		_ = a.upsertAntigravityKeeperState(ctx, result)
		logFn(name + ": " + message)
		return result
	}
	result.LastStatusCode = usageResult.StatusCode
	if isBadAntigravityKeeperCredential(usageResult) {
		message := fmt.Sprintf("凭证不可用：HTTP %d", *usageResult.StatusCode)
		if usageResult.Brief != "" {
			message += "，" + usageResult.Brief
		}
		action := "刷新发现凭证不可用：" + message
		if !cfg.AntigravityKeeper.DryRun {
			if !disabled {
				if err := a.setAntigravityKeeperRemoteDisabled(ctx, cfg, name, true); err != nil {
					message = "禁用坏凭证失败：" + err.Error()
					result.Result = "network_error"
					result.LastError = &message
					_ = a.upsertAntigravityKeeperState(ctx, result)
					return result
				}
			}
			_ = a.setAntigravityKeeperRemotePriority(ctx, cfg, name, nil)
			disabled = true
			result.Disabled = &disabled
			result.Priority = nil
			action = "禁用凭证：" + message
		} else {
			action = "模拟禁用：" + message
		}
		result.Result = "status_disabled"
		result.LastError = &message
		result.LatestAction = &action
		_ = a.upsertAntigravityKeeperState(ctx, result)
		logFn(name + ": " + action)
		return result
	}
	if *usageResult.StatusCode < 200 || *usageResult.StatusCode >= 300 {
		message := fmt.Sprintf("usage 检测失败：HTTP %d", *usageResult.StatusCode)
		if usageResult.Brief != "" {
			message += "，" + usageResult.Brief
		}
		result.Result = "network_error"
		result.LastError = &message
		result.LatestAction = &message
		_ = a.upsertAntigravityKeeperState(ctx, result)
		return result
	}
	usage := parseAntigravityKeeperUsageInfo(usageResult.JSONData)
	result.AccountType = accountTypeFromAntigravityKeeperDetail(merged, &usage)
	result.PrimaryUsedPercent = usage.PrimaryUsedPercent
	result.SecondaryUsedPercent = usage.SecondaryUsedPercent
	result.PrimaryResetAt = usage.PrimaryResetAt
	result.SecondaryResetAt = usage.SecondaryResetAt
	result.PrimaryWindowSeconds = usage.PrimaryWindowSeconds
	result.SecondaryWindowSeconds = usage.SecondaryWindowSeconds
	result.QuotaThreshold = &cfg.AntigravityKeeper.QuotaThreshold
	result.Result = "healthy"

	if recoverableUnauthorizedDisabled {
		action := fmt.Sprintf("恢复启用：usage 检测恢复 HTTP %d", *usageResult.StatusCode)
		if !cfg.AntigravityKeeper.DryRun {
			if err := a.setAntigravityKeeperRemoteDisabled(ctx, cfg, name, false); err != nil {
				message := "恢复启用失败：" + err.Error()
				result.Result = "network_error"
				result.LastError = &message
				result.LatestAction = &message
				_ = a.upsertAntigravityKeeperState(ctx, result)
				logFn(name + ": " + message)
				return result
			}
			disabled = false
			result.Disabled = &disabled
		} else {
			action = "模拟" + action
		}
		result.Result = "status_enabled"
		result.LatestAction = &action
		result.ClearRestorePriority = true
		result.LastError = nil
		_ = a.upsertAntigravityKeeperState(ctx, result)
		logFn(name + ": " + action)
		return result
	}

	action := a.applyAntigravityKeeperPriorityPolicy(ctx, cfg, name, result.AccountType, result.Priority, restorePriority, usage)
	if action != nil {
		result.LatestAction = &action.Message
		if action.Result == "priority_degraded" {
			result.Result = "priority_degraded"
			result.Priority = action.Priority
			result.RestorePriority = action.RestorePriority
		}
		if action.Result == "priority_restored" {
			result.Result = "priority_restored"
			result.Priority = action.Priority
			result.ClearRestorePriority = true
		}
		logFn(name + ": " + action.Message)
	} else {
		accountType := "unknown"
		if result.AccountType != nil && strings.TrimSpace(*result.AccountType) != "" {
			accountType = *result.AccountType
		}
		if manualRefresh {
			action := fmt.Sprintf("刷新完成，类型 %s", accountType)
			result.LatestAction = &action
			logFn(name + ": " + action)
		} else {
			logFn(fmt.Sprintf("%s: 巡检正常，类型 %s", name, accountType))
		}
	}
	if result.Priority == nil || *result.Priority != -1 {
		result.ClearRestorePriority = true
	}
	result.LastError = nil
	_ = a.upsertAntigravityKeeperState(ctx, result)
	return result
}

type antigravityKeeperPriorityPolicyAction struct {
	Message         string
	Result          string
	Priority        *int
	RestorePriority *int
}

func (a *App) applyAntigravityKeeperPriorityPolicy(ctx context.Context, cfg AppConfig, name string, accountType *string, priority *int, restorePriority *int, usage antigravityKeeperUsageInfo) *antigravityKeeperPriorityPolicyAction {
	quotaReached := (usage.PrimaryUsedPercent != nil && *usage.PrimaryUsedPercent >= cfg.AntigravityKeeper.QuotaThreshold) ||
		(usage.SecondaryUsedPercent != nil && *usage.SecondaryUsedPercent >= cfg.AntigravityKeeper.QuotaThreshold)
	currentPriority := antigravityKeeperEffectivePriority(priority)
	next := antigravityKeeperPriorityForType(accountType, cfg.AntigravityKeeperPriorityRule)
	if quotaReached {
		if currentPriority <= -1 {
			return nil
		}
		restoreTo := restorePriority
		if restoreTo == nil {
			restoreTo = next
		}
		if currentPriority > 20 {
			restoreTo = &currentPriority
		}
		if restoreTo == nil {
			restoreTo = &currentPriority
		}
		message := fmt.Sprintf("降为低优先级：额度使用率达到阈值 %d%%", cfg.AntigravityKeeper.QuotaThreshold)
		if cfg.AntigravityKeeper.DryRun {
			message = "模拟" + message
			low := -1
			return &antigravityKeeperPriorityPolicyAction{Message: message, Result: "priority_degraded", Priority: &low, RestorePriority: restoreTo}
		}
		low := -1
		if err := a.setAntigravityKeeperRemotePriority(ctx, cfg, name, &low); err != nil {
			message = "写入低优先级失败：" + err.Error()
			return &antigravityKeeperPriorityPolicyAction{Message: message}
		}
		return &antigravityKeeperPriorityPolicyAction{Message: message, Result: "priority_degraded", Priority: &low, RestorePriority: restoreTo}
	}
	if currentPriority == -1 {
		restoreTo := restorePriority
		if restoreTo == nil {
			restoreTo = next
		}
		if restoreTo == nil {
			zero := 0
			restoreTo = &zero
		}
		message := fmt.Sprintf("恢复优先级：priority %d", *restoreTo)
		if cfg.AntigravityKeeper.DryRun {
			message = "模拟" + message
			return &antigravityKeeperPriorityPolicyAction{Message: message, Result: "priority_restored", Priority: restoreTo}
		}
		if err := a.setAntigravityKeeperRemotePriority(ctx, cfg, name, restoreTo); err != nil {
			message = "恢复优先级失败：" + err.Error()
			return &antigravityKeeperPriorityPolicyAction{Message: message}
		}
		return &antigravityKeeperPriorityPolicyAction{Message: message, Result: "priority_restored", Priority: restoreTo}
	}
	if currentPriority < -1 || currentPriority > 20 {
		return nil
	}
	if next == nil {
		return nil
	}
	if currentPriority != *next {
		message := fmt.Sprintf("应用类型优先级：%s -> priority %d", valueOr(accountType, "unknown"), *next)
		if cfg.AntigravityKeeper.DryRun {
			message = "模拟" + message
			return &antigravityKeeperPriorityPolicyAction{Message: message, Result: "priority_restored", Priority: next}
		}
		if err := a.setAntigravityKeeperRemotePriority(ctx, cfg, name, next); err != nil {
			message = "写入类型优先级失败：" + err.Error()
			return &antigravityKeeperPriorityPolicyAction{Message: message}
		}
		return &antigravityKeeperPriorityPolicyAction{Message: message, Result: "priority_restored", Priority: next}
	}
	return nil
}

func antigravityKeeperEffectivePriority(priority *int) int {
	if priority == nil {
		return 0
	}
	return *priority
}

func antigravityKeeperDisplayPriority(priority *int) *int {
	if priority != nil {
		return priority
	}
	zero := 0
	return &zero
}

func (a *App) mergeAntigravityKeeperStats(stats *antigravityKeeperStats, result antigravityKeeperAccountResult) {
	switch result.Result {
	case "healthy":
		stats.Healthy++
	case "status_disabled":
		stats.StatusDisabled++
	case "status_enabled":
		stats.StatusEnabled++
	case "priority_degraded":
		stats.PriorityDegraded++
	case "priority_restored":
		stats.PriorityRestored++
	case "network_error":
		stats.NetworkError++
	default:
		stats.Skipped++
	}
}

func (stats *antigravityKeeperStats) add(delta antigravityKeeperStats) {
	stats.Total += delta.Total
	stats.Healthy += delta.Healthy
	stats.StatusDisabled += delta.StatusDisabled
	stats.StatusEnabled += delta.StatusEnabled
	stats.PriorityDegraded += delta.PriorityDegraded
	stats.PriorityRestored += delta.PriorityRestored
	stats.Skipped += delta.Skipped
	stats.NetworkError += delta.NetworkError
}

func (stats *antigravityKeeperStats) mergeCachedState(state antigravityKeeperAuthState) {
	if isAntigravityKeeperCachedBadCredentialState(state) {
		stats.StatusDisabled++
		return
	}
	if state.Disabled {
		return
	}
	if state.LastError != nil && strings.TrimSpace(*state.LastError) != "" {
		stats.NetworkError++
		return
	}
	if state.Priority != nil && *state.Priority == -1 {
		stats.PriorityDegraded++
		return
	}
	if state.LastHealthyAt != nil || state.LastCheckedAt != nil {
		stats.Healthy++
	}
}

func isAntigravityKeeperCachedBadCredentialState(state antigravityKeeperAuthState) bool {
	if isAntigravityKeeperBadCredentialDisableAction(state.LatestAction) {
		return true
	}
	if state.LastStatusCode != nil && (*state.LastStatusCode == http.StatusUnauthorized || *state.LastStatusCode == http.StatusPaymentRequired) {
		return true
	}
	if state.LatestAction != nil && strings.HasPrefix(strings.TrimSpace(*state.LatestAction), "模拟禁用") {
		return true
	}
	if state.LastError == nil {
		return false
	}
	message := strings.TrimSpace(*state.LastError)
	return strings.Contains(message, "缺少 access token") || strings.Contains(message, "凭证不可用")
}

func (a *App) listAntigravityKeeperRemoteAuthFiles(ctx context.Context, cfg AppConfig) ([]map[string]any, error) {
	_, payload, err := a.antigravityKeeperRequest(ctx, cfg, http.MethodGet, "/v0/management/auth-files", nil, nil, time.Duration(cfg.AntigravityKeeper.CPATimeoutSeconds)*time.Second)
	if err != nil {
		return nil, err
	}
	var raw any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, validationError("读取 auth files 失败：响应不是有效 JSON")
	}
	return extractAntigravityKeeperObjects(raw, []string{"files", "items", "data", "value"}), nil
}

func (a *App) getAntigravityKeeperRemoteAuthFile(ctx context.Context, cfg AppConfig, name string) (map[string]any, error) {
	query := url.Values{"name": []string{name}}
	response, payload, err := a.antigravityKeeperRequest(ctx, cfg, http.MethodGet, "/v0/management/auth-files/download", query, nil, time.Duration(cfg.AntigravityKeeper.CPATimeoutSeconds)*time.Second)
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, validationError("读取 auth file 详情失败：响应不是有效 JSON")
	}
	return raw, nil
}

func (a *App) setAntigravityKeeperRemoteDisabled(ctx context.Context, cfg AppConfig, name string, disabled bool) error {
	_, _, err := a.antigravityKeeperRequest(ctx, cfg, http.MethodPatch, "/v0/management/auth-files/status", nil, map[string]any{"name": name, "disabled": disabled}, time.Duration(cfg.AntigravityKeeper.CPATimeoutSeconds)*time.Second)
	return err
}

func (a *App) setAntigravityKeeperRemotePriority(ctx context.Context, cfg AppConfig, name string, priority *int) error {
	_, _, err := a.antigravityKeeperRequest(ctx, cfg, http.MethodPatch, "/v0/management/auth-files/fields", nil, map[string]any{"name": name, "priority": priority}, time.Duration(cfg.AntigravityKeeper.CPATimeoutSeconds)*time.Second)
	return err
}

func (a *App) setAntigravityKeeperRemoteWebsockets(ctx context.Context, cfg AppConfig, name string) error {
	_, _, err := a.antigravityKeeperRequest(ctx, cfg, http.MethodPatch, "/v0/management/auth-files/fields", nil, map[string]any{"name": name, "websockets": true}, time.Duration(cfg.AntigravityKeeper.CPATimeoutSeconds)*time.Second)
	return err
}

func (a *App) deleteAntigravityKeeperRemoteAuthFile(ctx context.Context, cfg AppConfig, name string) error {
	query := url.Values{"name": []string{name}}
	_, _, err := a.antigravityKeeperRequest(ctx, cfg, http.MethodDelete, "/v0/management/auth-files", query, nil, time.Duration(cfg.AntigravityKeeper.CPATimeoutSeconds)*time.Second)
	return err
}

func (a *App) antigravityKeeperRequest(ctx context.Context, cfg AppConfig, method, path string, query url.Values, body any, timeout time.Duration) (*http.Response, []byte, error) {
	attempts := antigravityKeeperRequestAttempts(cfg.AntigravityKeeper)
	target := makeURL(cfg.Collector.CLIProxyURL, path, query)
	headers := managementHeaders(cfg.Collector.ManagementKey)
	var lastResponse *http.Response
	var lastPayload []byte
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		response, payload, err := doJSON(ctx, httpClient(timeout), method, target, headers, body)
		lastResponse = response
		lastPayload = payload
		if err != nil {
			lastErr = validationError("CLIProxyAPI 管理请求失败：" + err.Error())
			if antigravityKeeperShouldRetryRequest(ctx, attempt, attempts, response, err) {
				continue
			}
			return nil, nil, lastErr
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			lastErr = validationError(fmt.Sprintf("CLIProxyAPI 管理请求失败：HTTP %d", response.StatusCode))
			if antigravityKeeperShouldRetryRequest(ctx, attempt, attempts, response, nil) {
				continue
			}
			return response, payload, lastErr
		}
		return response, payload, nil
	}
	return lastResponse, lastPayload, lastErr
}

func antigravityKeeperRequestAttempts(cfg KeeperConfig) int {
	return clampInt(cfg.MaxRetries, 0, 5, 2) + 1
}

func antigravityKeeperShouldRetryRequest(ctx context.Context, attempt, attempts int, response *http.Response, err error) bool {
	if attempt >= attempts || ctx.Err() != nil {
		return false
	}
	if err != nil {
		return true
	}
	return response != nil && response.StatusCode >= 500
}

func (a *App) checkAntigravityKeeperUsage(ctx context.Context, cfg AppConfig, detail map[string]any) antigravityKeeperHTTPResult {
	authIndex := antigravityKeeperAuthIndex(detail)
	header := map[string]string{
		"Authorization": "Bearer $TOKEN$",
		"Content-Type":  "application/json",
		"User-Agent":    "antigravity/1.11.5 windows/amd64",
	}
	projectID := antigravityKeeperProjectID(detail)
	requestBody, _ := json.Marshal(map[string]string{"project": projectID})

	var lastResult antigravityKeeperHTTPResult
	for _, endpoint := range antigravityKeeperQuotaURLs {
		body := map[string]any{
			"auth_index": authIndex,
			"method":     "POST",
			"url":        endpoint,
			"header":     header,
			"data":       string(requestBody),
		}
		response, payload, err := a.antigravityKeeperRequest(ctx, cfg, http.MethodPost, "/v0/management/api-call", nil, body, time.Duration(cfg.AntigravityKeeper.UsageTimeoutSeconds)*time.Second)
		if err != nil {
			lastResult = antigravityKeeperHTTPResult{Error: err.Error()}
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			lastResult = antigravityKeeperHTTPResult{Error: fmt.Sprintf("api-call 管理请求失败：HTTP %d", response.StatusCode), Brief: antigravityBriefPayload(payload)}
			continue
		}

		var raw map[string]any
		if err := json.Unmarshal(payload, &raw); err != nil {
			lastResult = antigravityKeeperHTTPResult{Error: "api-call 响应不是有效 JSON"}
			continue
		}
		statusCode := antigravityKeeperIntPtr(raw["status_code"], raw["statusCode"])
		if statusCode == nil {
			lastResult = antigravityKeeperHTTPResult{Error: "api-call 响应缺少 status_code"}
			continue
		}
		if err := a.appendAntigravityKeeperRawResponseLogFile(time.Now(), authIndex, endpoint, *statusCode, raw["body"]); err != nil {
			log.Printf("write antigravity keeper raw response log failed: %v", err)
		}
		bodyJSON := antigravityKeeperBodyJSON(raw["body"])
		lastResult = antigravityKeeperHTTPResult{
			StatusCode: statusCode,
			JSONData:   bodyJSON,
			Brief:      antigravityBriefAny(raw["body"]),
		}
		if *statusCode >= 200 && *statusCode < 300 && bodyJSON != nil {
			if models, ok := bodyJSON["models"].(map[string]any); ok && len(models) > 0 {
				if tierResult := a.checkAntigravityKeeperTier(ctx, cfg, authIndex, header); tierResult.JSONData != nil {
					for key, value := range tierResult.JSONData {
						if _, exists := bodyJSON[key]; !exists {
							bodyJSON[key] = value
						}
					}
				}
				lastResult.JSONData = bodyJSON
				return lastResult
			}
			lastResult.Error = "fetchAvailableModels 响应缺少 models"
			continue
		}
		if *statusCode != http.StatusForbidden && *statusCode != http.StatusNotFound {
			return lastResult
		}
	}
	if lastResult.Error == "" {
		lastResult.Error = "fetchAvailableModels 没有返回可用额度数据"
	}
	return lastResult
}

func (a *App) checkAntigravityKeeperTier(ctx context.Context, cfg AppConfig, authIndex string, header map[string]string) antigravityKeeperHTTPResult {
	requestBody, _ := json.Marshal(map[string]any{
		"metadata": map[string]string{"ideType": "ANTIGRAVITY"},
	})
	body := map[string]any{
		"auth_index": authIndex,
		"method":     "POST",
		"url":        antigravityKeeperTierURL,
		"header":     header,
		"data":       string(requestBody),
	}
	response, payload, err := a.antigravityKeeperRequest(ctx, cfg, http.MethodPost, "/v0/management/api-call", nil, body, time.Duration(cfg.AntigravityKeeper.UsageTimeoutSeconds)*time.Second)
	if err != nil {
		return antigravityKeeperHTTPResult{Error: err.Error()}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return antigravityKeeperHTTPResult{Error: fmt.Sprintf("api-call 管理请求失败：HTTP %d", response.StatusCode), Brief: antigravityBriefPayload(payload)}
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return antigravityKeeperHTTPResult{Error: "api-call 响应不是有效 JSON"}
	}
	statusCode := antigravityKeeperIntPtr(raw["status_code"], raw["statusCode"])
	if statusCode == nil {
		return antigravityKeeperHTTPResult{Error: "api-call 响应缺少 status_code"}
	}
	if err := a.appendAntigravityKeeperRawResponseLogFile(time.Now(), authIndex, antigravityKeeperTierURL, *statusCode, raw["body"]); err != nil {
		log.Printf("write antigravity keeper raw response log failed: %v", err)
	}
	bodyJSON := antigravityKeeperBodyJSON(raw["body"])
	return antigravityKeeperHTTPResult{
		StatusCode: statusCode,
		JSONData:   bodyJSON,
		Brief:      antigravityBriefAny(raw["body"]),
	}
}

func (a *App) listAntigravityKeeperAccounts(ctx context.Context) ([]antigravityKeeperAccount, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT auth_name, email, account_type, disabled, priority, primary_used_percent,
		       secondary_used_percent, CAST(primary_reset_at AS TEXT), CAST(secondary_reset_at AS TEXT), quota_threshold,
		       last_status_code, last_error, latest_action, CAST(last_checked_at AS TEXT), CAST(last_healthy_at AS TEXT),
		       primary_window_seconds, secondary_window_seconds, restore_priority, CAST(created_at AS TEXT), CAST(updated_at AS TEXT)
		FROM antigravity_keeper_auth_states
		ORDER BY COALESCE(email, ''), auth_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	accounts := []antigravityKeeperAccount{}
	for rows.Next() {
		state, err := scanAntigravityKeeperState(rows)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, state.antigravityKeeperAccount)
	}
	return accounts, rows.Err()
}

func (a *App) pruneAntigravityKeeperMissingAuthStates(ctx context.Context, remoteNames map[string]bool) (int, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT auth_name FROM antigravity_keeper_auth_states`)
	if err != nil {
		return 0, err
	}
	stale := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return 0, err
		}
		if !remoteNames[name] {
			stale = append(stale, name)
		}
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(stale) == 0 {
		return 0, nil
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `DELETE FROM antigravity_keeper_auth_states WHERE auth_name = ?`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	pruned := 0
	for _, name := range stale {
		result, err := stmt.ExecContext(ctx, name)
		if err != nil {
			return 0, err
		}
		affected, _ := result.RowsAffected()
		pruned += int(affected)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return pruned, nil
}

func (a *App) getAntigravityKeeperState(ctx context.Context, name string) (*antigravityKeeperAuthState, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT auth_name, email, account_type, disabled, priority, primary_used_percent,
		       secondary_used_percent, CAST(primary_reset_at AS TEXT), CAST(secondary_reset_at AS TEXT), quota_threshold,
		       last_status_code, last_error, latest_action, CAST(last_checked_at AS TEXT), CAST(last_healthy_at AS TEXT),
		       primary_window_seconds, secondary_window_seconds, restore_priority, CAST(created_at AS TEXT), CAST(updated_at AS TEXT)
		FROM antigravity_keeper_auth_states WHERE auth_name = ?
	`, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, notFoundError("账号状态不存在")
	}
	state, err := scanAntigravityKeeperState(rows)
	if err != nil {
		return nil, err
	}
	return &state, rows.Err()
}

func scanAntigravityKeeperState(scanner interface{ Scan(dest ...any) error }) (antigravityKeeperAuthState, error) {
	var state antigravityKeeperAuthState
	var email, accountType, primaryReset, secondaryReset, lastError, latestAction, lastChecked, lastHealthy, createdAt, updatedAt sql.NullString
	var priority, primaryUsed, secondaryUsed, quotaThreshold, lastStatus, primaryWindowSeconds, secondaryWindowSeconds, restorePriority sql.NullInt64
	err := scanner.Scan(
		&state.Name, &email, &accountType, &state.Disabled, &priority, &primaryUsed,
		&secondaryUsed, &primaryReset, &secondaryReset, &quotaThreshold, &lastStatus,
		&lastError, &latestAction, &lastChecked, &lastHealthy, &primaryWindowSeconds, &secondaryWindowSeconds, &restorePriority,
		&createdAt, &updatedAt,
	)
	if err != nil {
		return antigravityKeeperAuthState{}, err
	}
	state.Email = nullableString(email)
	state.AccountType = nullableString(accountType)
	state.Priority = nullableInt(priority)
	state.PrimaryUsedPercent = nullableInt(primaryUsed)
	state.SecondaryUsedPercent = nullableInt(secondaryUsed)
	state.PrimaryResetAt = timePtr(primaryReset)
	state.SecondaryResetAt = timePtr(secondaryReset)
	state.PrimaryWindowSeconds = nullableInt(primaryWindowSeconds)
	state.SecondaryWindowSeconds = nullableInt(secondaryWindowSeconds)
	state.QuotaThreshold = nullableInt(quotaThreshold)
	state.LastStatusCode = nullableInt(lastStatus)
	state.LastError = nullableString(lastError)
	state.LatestAction = nullableString(latestAction)
	state.LastCheckedAt = timePtr(lastChecked)
	state.LastHealthyAt = timePtr(lastHealthy)
	state.RestorePriority = nullableInt(restorePriority)
	if parsed, ok := parseDBTime(createdAt.String); ok {
		state.CreatedAt = parsed
	}
	if parsed, ok := parseDBTime(updatedAt.String); ok {
		state.UpdatedAt = parsed
	}
	return state, nil
}

func (a *App) upsertAntigravityKeeperState(ctx context.Context, result antigravityKeeperAccountResult) error {
	now := dbTime(time.Now())
	checkedAt := dbTime(result.CheckedAt)
	var lastHealthy any
	if result.Result == "healthy" || result.Result == "status_enabled" || result.Result == "priority_degraded" || result.Result == "priority_restored" {
		lastHealthy = checkedAt
	}
	_, err := a.db.ExecContext(ctx, `
		INSERT INTO antigravity_keeper_auth_states (
			auth_name, email, account_type, disabled, priority, restore_priority, latest_action, last_error,
			last_status_code, primary_used_percent, secondary_used_percent, quota_threshold,
			primary_reset_at, secondary_reset_at, primary_window_seconds, secondary_window_seconds,
			last_checked_at, last_healthy_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(auth_name) DO UPDATE SET
			email = excluded.email,
			account_type = excluded.account_type,
			disabled = excluded.disabled,
			priority = excluded.priority,
			restore_priority = CASE
				WHEN ? THEN NULL
				WHEN excluded.restore_priority IS NOT NULL THEN excluded.restore_priority
				ELSE antigravity_keeper_auth_states.restore_priority
			END,
			latest_action = excluded.latest_action,
			last_error = excluded.last_error,
			last_status_code = excluded.last_status_code,
			primary_used_percent = excluded.primary_used_percent,
			secondary_used_percent = excluded.secondary_used_percent,
			quota_threshold = excluded.quota_threshold,
			primary_reset_at = excluded.primary_reset_at,
			secondary_reset_at = excluded.secondary_reset_at,
			primary_window_seconds = excluded.primary_window_seconds,
			secondary_window_seconds = excluded.secondary_window_seconds,
			last_checked_at = excluded.last_checked_at,
			last_healthy_at = COALESCE(excluded.last_healthy_at, antigravity_keeper_auth_states.last_healthy_at),
			updated_at = excluded.updated_at
	`, result.Name, result.Email, result.AccountType, antigravityBoolValue(result.Disabled), result.Priority, result.RestorePriority, result.LatestAction, result.LastError, result.LastStatusCode, result.PrimaryUsedPercent, result.SecondaryUsedPercent, result.QuotaThreshold, dbTimePtr(result.PrimaryResetAt), dbTimePtr(result.SecondaryResetAt), result.PrimaryWindowSeconds, result.SecondaryWindowSeconds, checkedAt, lastHealthy, now, now, result.ClearRestorePriority)
	return err
}

func (a *App) setAntigravityKeeperAccountDisabled(ctx context.Context, authName string, disabled bool) error {
	cfg, err := a.loadConfig(ctx)
	if err != nil {
		return err
	}
	state, err := a.getAntigravityKeeperState(ctx, authName)
	if err != nil {
		return err
	}
	if err := a.setAntigravityKeeperRemoteDisabled(ctx, cfg, authName, disabled); err != nil {
		return err
	}
	now := dbTime(time.Now())
	var checkedAt any = now
	var lastHealthy any
	if !disabled {
		lastHealthy = now
	}
	_, err = a.db.ExecContext(ctx, `
		UPDATE antigravity_keeper_auth_states
		SET disabled = ?, restore_priority = NULL, latest_action = NULL, last_error = NULL,
		    last_status_code = NULL, primary_used_percent = CASE WHEN ? THEN NULL ELSE primary_used_percent END,
		    secondary_used_percent = CASE WHEN ? THEN NULL ELSE secondary_used_percent END,
		    primary_reset_at = CASE WHEN ? THEN NULL ELSE primary_reset_at END,
		    secondary_reset_at = CASE WHEN ? THEN NULL ELSE secondary_reset_at END,
		    quota_threshold = CASE WHEN ? THEN NULL ELSE quota_threshold END,
		    last_checked_at = ?, last_healthy_at = COALESCE(?, last_healthy_at), updated_at = ?
		WHERE auth_name = ?
	`, disabled, disabled, disabled, disabled, disabled, disabled, checkedAt, lastHealthy, now, state.Name)
	return err
}

func (a *App) deleteAntigravityKeeperAccount(ctx context.Context, authName string) error {
	cfg, err := a.loadConfig(ctx)
	if err != nil {
		return err
	}
	state, err := a.getAntigravityKeeperState(ctx, authName)
	if err != nil {
		return err
	}
	if !state.Disabled {
		return validationError("只能删除已禁用账号")
	}
	if err := a.deleteAntigravityKeeperRemoteAuthFile(ctx, cfg, authName); err != nil {
		return err
	}
	_, err = a.db.ExecContext(ctx, `DELETE FROM antigravity_keeper_auth_states WHERE auth_name = ?`, authName)
	return err
}

func (a *App) bulkDeleteAntigravityKeeperAccounts(w http.ResponseWriter, r *http.Request) error {
	var payload antigravityKeeperBulkDeleteRequest
	if err := decodeJSON(r, &payload); err != nil {
		return err
	}
	names, err := normalizeAntigravityKeeperAuthNames(payload.AuthNames)
	if err != nil {
		return err
	}
	deleted := []string{}
	failures := []map[string]string{}
	for _, name := range names {
		if err := a.deleteAntigravityKeeperAccount(r.Context(), name); err != nil {
			failures = append(failures, map[string]string{"name": name, "message": err.Error()})
			continue
		}
		deleted = append(deleted, name)
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "completed", "deleted": deleted, "failed": failures})
	return nil
}

func (a *App) updateAntigravityKeeperAccountPriority(ctx context.Context, authName string, priority int) error {
	cfg, err := a.loadConfig(ctx)
	if err != nil {
		return err
	}
	state, err := a.getAntigravityKeeperState(ctx, authName)
	if err != nil {
		return err
	}
	if err := validateAntigravityKeeperPriority(priority, state.AccountType, cfg.AntigravityKeeperPriorityRule); err != nil {
		return err
	}
	if err := a.setAntigravityKeeperRemotePriority(ctx, cfg, authName, &priority); err != nil {
		return err
	}
	_, err = a.db.ExecContext(ctx, `
		UPDATE antigravity_keeper_auth_states
		SET priority = ?, restore_priority = NULL, latest_action = NULL, last_error = NULL, updated_at = ?
		WHERE auth_name = ?
	`, priority, dbTime(time.Now()), authName)
	return err
}

func (a *App) createAntigravityKeeperRun(ctx context.Context, mode string) (int, error) {
	now := dbTime(time.Now())
	result, err := a.db.ExecContext(ctx, `
		INSERT INTO antigravity_keeper_runs (mode, state, detail, started_at, created_at, updated_at)
		VALUES (?, 'running', '', ?, ?, ?)
	`, mode, now, now, now)
	if err != nil {
		return 0, err
	}
	id, _ := result.LastInsertId()
	return int(id), nil
}

func (a *App) finishAntigravityKeeperRun(ctx context.Context, runID int, state, detail string, stats antigravityKeeperStats) error {
	_, err := a.db.ExecContext(ctx, `
		UPDATE antigravity_keeper_runs
		SET state = ?, detail = ?, finished_at = ?, total = ?, healthy = ?, status_disabled = ?,
		    status_enabled = ?, priority_degraded = ?, priority_restored = ?, skipped = ?,
		    network_error = ?, updated_at = ?
		WHERE id = ?
	`, state, detail, dbTime(time.Now()), stats.Total, stats.Healthy, stats.StatusDisabled, stats.StatusEnabled, stats.PriorityDegraded, stats.PriorityRestored, stats.Skipped, stats.NetworkError, dbTime(time.Now()), runID)
	return err
}

type antigravityKeeperRunRecord struct {
	Mode       *string
	State      string
	Detail     string
	StartedAt  *time.Time
	FinishedAt *time.Time
	Stats      antigravityKeeperStats
}

func (a *App) latestAntigravityKeeperRun(ctx context.Context) (*antigravityKeeperRunRecord, error) {
	row := a.db.QueryRowContext(ctx, `
		SELECT mode, state, detail, CAST(started_at AS TEXT), CAST(finished_at AS TEXT), total, healthy, status_disabled,
		       status_enabled, priority_degraded, priority_restored, skipped, network_error
		FROM antigravity_keeper_runs ORDER BY id DESC LIMIT 1
	`)
	return scanAntigravityKeeperRunRecord(row)
}

func (a *App) latestAntigravityKeeperRunByMode(ctx context.Context, mode string) (*antigravityKeeperRunRecord, error) {
	row := a.db.QueryRowContext(ctx, `
		SELECT mode, state, detail, CAST(started_at AS TEXT), CAST(finished_at AS TEXT), total, healthy, status_disabled,
		       status_enabled, priority_degraded, priority_restored, skipped, network_error
		FROM antigravity_keeper_runs WHERE mode = ? ORDER BY id DESC LIMIT 1
	`, mode)
	return scanAntigravityKeeperRunRecord(row)
}

func scanAntigravityKeeperRunRecord(row interface {
	Scan(dest ...any) error
}) (*antigravityKeeperRunRecord, error) {
	var run antigravityKeeperRunRecord
	var mode, startedAt, finishedAt sql.NullString
	err := row.Scan(&mode, &run.State, &run.Detail, &startedAt, &finishedAt, &run.Stats.Total, &run.Stats.Healthy, &run.Stats.StatusDisabled, &run.Stats.StatusEnabled, &run.Stats.PriorityDegraded, &run.Stats.PriorityRestored, &run.Stats.Skipped, &run.Stats.NetworkError)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	run.Mode = nullableString(mode)
	run.StartedAt = timePtr(startedAt)
	run.FinishedAt = timePtr(finishedAt)
	return &run, nil
}

func (a *App) recordAntigravityKeeperRunAccount(ctx context.Context, runID int, result antigravityKeeperAccountResult) error {
	_, err := a.db.ExecContext(ctx, `
		INSERT INTO antigravity_keeper_run_accounts (
			run_id, auth_name, email, result, account_type, priority, disabled,
			keeper_action, primary_used_percent, secondary_used_percent, quota_threshold,
			last_status_code, last_error, latest_action, checked_at, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, runID, result.Name, result.Email, result.Result, result.AccountType, result.Priority, result.Disabled, valueOr(result.LatestAction, "none"), result.PrimaryUsedPercent, result.SecondaryUsedPercent, result.QuotaThreshold, result.LastStatusCode, result.LastError, result.LatestAction, dbTime(result.CheckedAt), dbTime(time.Now()))
	return err
}

func extractAntigravityKeeperObjects(payload any, keys []string) []map[string]any {
	if items, ok := payload.([]any); ok {
		return antigravityMapItems(items)
	}
	object, ok := payload.(map[string]any)
	if !ok {
		return []map[string]any{}
	}
	for _, key := range keys {
		if items, ok := object[key].([]any); ok {
			return antigravityMapItems(items)
		}
	}
	return []map[string]any{}
}

func antigravityMapItems(items []any) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if object, ok := item.(map[string]any); ok {
			result = append(result, object)
		}
	}
	return result
}

func mergeAntigravityKeeperObjects(left, right map[string]any) map[string]any {
	result := map[string]any{}
	for key, value := range left {
		result[key] = value
	}
	for key, value := range right {
		result[key] = value
	}
	return result
}

func parseAntigravityKeeperUsageInfo(payload map[string]any) antigravityKeeperUsageInfo {
	usage := antigravityKeeperUsageInfo{PlanType: "unknown"}
	if payload == nil {
		return usage
	}
	if value := antigravityKeeperString(payload["plan_type"]); value != "" {
		usage.PlanType = value
	} else if value := antigravityKeeperString(payload["planType"]); value != "" {
		usage.PlanType = value
	} else if value := antigravityKeeperNestedString(payload, "currentTier", "id"); value != "" {
		usage.PlanType = value
	} else if value := antigravityKeeperNestedString(payload, "currentTier", "name"); value != "" {
		usage.PlanType = value
	}

	models, _ := payload["models"].(map[string]any)
	if len(models) > 0 {
		if group := antigravityKeeperQuotaGroup(models, []string{"claude-sonnet-4-6", "claude-opus-4-6-thinking", "gpt-oss-120b-medium"}); group != nil {
			usage.PrimaryUsedPercent = group.UsedPercent
			usage.PrimaryResetAt = group.ResetAt
		}
		if group := antigravityKeeperQuotaGroup(models, []string{"gemini-3.1-pro-high", "gemini-3.1-pro-low"}); group != nil {
			usage.SecondaryUsedPercent = group.UsedPercent
			usage.SecondaryResetAt = group.ResetAt
		} else if group := antigravityKeeperQuotaGroup(models, []string{"gemini-3-pro-high", "gemini-3-pro-low"}); group != nil {
			usage.SecondaryUsedPercent = group.UsedPercent
			usage.SecondaryResetAt = group.ResetAt
		}
		return usage
	}

	rateLimit, _ := payload["rate_limit"].(map[string]any)
	primary, _ := rateLimit["primary_window"].(map[string]any)
	secondary, _ := rateLimit["secondary_window"].(map[string]any)
	usage.PrimaryUsedPercent = antigravityKeeperIntPtr(primary["used_percent"])
	usage.SecondaryUsedPercent = antigravityKeeperIntPtr(secondary["used_percent"])
	usage.PrimaryResetAt = antigravityQuotaResetAt(primary, time.Now().In(appTimeLocation))
	usage.SecondaryResetAt = antigravityQuotaResetAt(secondary, time.Now().In(appTimeLocation))
	usage.PrimaryWindowSeconds = antigravityQuotaWindowSeconds(primary)
	usage.SecondaryWindowSeconds = antigravityQuotaWindowSeconds(secondary)
	return usage
}

type antigravityKeeperQuotaGroupResult struct {
	UsedPercent *int
	ResetAt     *time.Time
}

func antigravityKeeperQuotaGroup(models map[string]any, identifiers []string) *antigravityKeeperQuotaGroupResult {
	var remainingFractions []float64
	var resetAt *time.Time
	for _, identifier := range identifiers {
		entry := antigravityKeeperModelEntry(models, identifier)
		if entry == nil {
			continue
		}
		quotaInfo := antigravityKeeperQuotaInfo(entry)
		remaining := antigravityKeeperQuotaRemainingFraction(quotaInfo["remainingFraction"], quotaInfo["remaining_fraction"], quotaInfo["remaining"])
		entryResetAt := antigravityKeeperQuotaResetTime(quotaInfo["resetTime"], quotaInfo["reset_time"])
		if remaining == nil {
			if entryResetAt == nil {
				continue
			}
			limited := 0.0
			remaining = &limited
		}
		remainingFractions = append(remainingFractions, *remaining)
		if resetAt == nil && entryResetAt != nil {
			resetAt = entryResetAt
		}
	}
	if len(remainingFractions) == 0 {
		return nil
	}
	remaining := remainingFractions[0]
	for _, value := range remainingFractions[1:] {
		remaining = math.Min(remaining, value)
	}
	usedPercent := int(math.Round((1 - remaining) * 100))
	if usedPercent < 0 {
		usedPercent = 0
	} else if usedPercent > 100 {
		usedPercent = 100
	}
	return &antigravityKeeperQuotaGroupResult{UsedPercent: &usedPercent, ResetAt: resetAt}
}

func antigravityKeeperModelEntry(models map[string]any, identifier string) map[string]any {
	if entry, ok := models[identifier].(map[string]any); ok {
		return entry
	}
	for key, value := range models {
		entry, ok := value.(map[string]any)
		if !ok {
			continue
		}
		if key == identifier || antigravityKeeperString(entry["id"]) == identifier || antigravityKeeperString(entry["name"]) == identifier || antigravityKeeperString(entry["model"]) == identifier {
			return entry
		}
	}
	return nil
}

func antigravityKeeperQuotaInfo(entry map[string]any) map[string]any {
	if quotaInfo, ok := entry["quotaInfo"].(map[string]any); ok {
		return quotaInfo
	}
	if quotaInfo, ok := entry["quota_info"].(map[string]any); ok {
		return quotaInfo
	}
	return map[string]any{}
}

func antigravityKeeperQuotaRemainingFraction(values ...any) *float64 {
	for _, value := range values {
		parsed, ok := antigravityKeeperFloat(value)
		if !ok {
			continue
		}
		if parsed < 0 {
			parsed = 0
		} else if parsed > 1 {
			continue
		}
		return &parsed
	}
	return nil
}

func antigravityKeeperFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case string:
		text := strings.TrimSpace(typed)
		if text == "" {
			return 0, false
		}
		isPercent := strings.HasSuffix(text, "%")
		text = strings.TrimSpace(strings.TrimSuffix(text, "%"))
		parsed, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return 0, false
		}
		if isPercent {
			parsed /= 100
		}
		return parsed, true
	default:
		return 0, false
	}
}

func antigravityKeeperQuotaResetTime(values ...any) *time.Time {
	for _, value := range values {
		text := antigravityKeeperString(value)
		if text == "" {
			continue
		}
		if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
			parsed = parsed.In(appTimeLocation)
			return &parsed
		}
		if parsed, err := time.Parse(time.RFC3339, text); err == nil {
			parsed = parsed.In(appTimeLocation)
			return &parsed
		}
	}
	return nil
}

func antigravityQuotaWindowSeconds(window map[string]any) *int {
	if window == nil {
		return nil
	}
	value := antigravityKeeperIntPtr(
		window["limit_window_seconds"],
		window["limitWindowSeconds"],
		window["window_seconds"],
		window["windowSeconds"],
		window["rolling_window_seconds"],
		window["rollingWindowSeconds"],
	)
	if value == nil || *value <= 0 {
		return nil
	}
	return value
}

func antigravityQuotaResetAt(window map[string]any, base time.Time) *time.Time {
	if window == nil {
		return nil
	}
	if ts := antigravityKeeperIntPtr(window["reset_at"], window["resetAt"], window["reset_at_seconds"], window["resetAtSeconds"]); ts != nil {
		seconds := int64(*ts)
		if seconds > 10_000_000_000 {
			seconds /= 1000
		}
		parsed := time.Unix(seconds, 0).In(appTimeLocation)
		return &parsed
	}
	if after := antigravityKeeperIntPtr(window["reset_after_seconds"], window["resetAfterSeconds"]); after != nil && *after >= 0 {
		parsed := base.Add(time.Duration(*after) * time.Second)
		return &parsed
	}
	return nil
}

func accountTypeFromAntigravityKeeperDetail(detail map[string]any, usage *antigravityKeeperUsageInfo) *string {
	values := []string{}
	if usage != nil {
		values = append(values, usage.PlanType)
	}
	values = append(values, antigravityKeeperAccountTypeValues(detail)...)
	text := strings.ToLower(strings.Join(values, " "))
	text = strings.NewReplacer("-", "_", " ", "_", ".", "_", "@", "_", "/", "_", "\\", "_").Replace(text)
	bounded := "_" + text + "_"
	var result string
	switch {
	case strings.Contains(text, "prolite") || strings.Contains(text, "pro_lite") || strings.Contains(text, "5x") || strings.Contains(text, "pro_5"):
		result = "pro_5x"
	case strings.Contains(text, "20x") || strings.Contains(text, "pro_20") || strings.Contains(bounded, "_pro_"):
		result = "pro_20x"
	case strings.Contains(text, "team") || strings.Contains(text, "business"):
		result = "team"
	case strings.Contains(text, "plus"):
		result = "plus"
	case strings.Contains(text, "free"):
		result = "free"
	default:
		return nil
	}
	return &result
}

func antigravityKeeperAccountTypeValues(detail map[string]any) []string {
	if detail == nil {
		return nil
	}
	values := []string{}
	appendString := func(value any) {
		if text := antigravityKeeperString(value); text != "" {
			values = append(values, text)
		}
	}
	for _, key := range []string{"plan_type", "planType", "plan", "tier", "account_plan", "subscription_plan", "sku", "account_type", "accountType", "type"} {
		appendString(detail[key])
	}
	for _, key := range []string{"attributes", "metadata"} {
		if object, ok := detail[key].(map[string]any); ok {
			for _, nestedKey := range []string{"plan_type", "planType", "chatgpt_plan_type", "chatgptPlanType"} {
				appendString(object[nestedKey])
			}
			values = append(values, antigravityKeeperIDTokenPlanValues(object["id_token"])...)
		}
	}
	values = append(values, antigravityKeeperIDTokenPlanValues(detail["id_token"])...)
	for _, key := range []string{"name", "file_name", "filename"} {
		appendString(detail[key])
	}
	if path := antigravityKeeperString(detail["path"]); path != "" {
		values = append(values, filepath.Base(path))
	}
	return values
}

func antigravityKeeperIDTokenPlanValues(value any) []string {
	object := antigravityKeeperIDTokenClaims(value)
	if object == nil {
		return nil
	}
	values := []string{}
	appendString := func(value any) {
		if text := antigravityKeeperString(value); text != "" {
			values = append(values, text)
		}
	}
	for _, key := range []string{"plan_type", "planType", "chatgpt_plan_type", "chatgptPlanType"} {
		appendString(object[key])
	}
	if authInfo, ok := object["https://api.openai.com/auth"].(map[string]any); ok {
		for _, key := range []string{"plan_type", "planType", "chatgpt_plan_type", "chatgptPlanType"} {
			appendString(authInfo[key])
		}
	}
	return values
}

func antigravityKeeperIDTokenClaims(value any) map[string]any {
	switch typed := value.(type) {
	case nil:
		return nil
	case map[string]any:
		return typed
	case string:
		token := strings.TrimSpace(typed)
		if token == "" {
			return nil
		}
		var object map[string]any
		if json.Unmarshal([]byte(token), &object) == nil {
			return object
		}
		parts := strings.Split(token, ".")
		if len(parts) < 2 {
			return nil
		}
		decoded, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			decoded, err = base64.URLEncoding.DecodeString(parts[1])
		}
		if err != nil {
			return nil
		}
		if json.Unmarshal(decoded, &object) != nil {
			return nil
		}
		return object
	default:
		return nil
	}
}

func antigravityKeeperPriorityForType(accountType *string, rules map[string]int) *int {
	if accountType == nil {
		return nil
	}
	value, ok := normalizePriorityRules(rules)[strings.ToLower(strings.TrimSpace(*accountType))]
	if !ok {
		return nil
	}
	return &value
}

func validateAntigravityKeeperPriority(priority int, accountType *string, rules map[string]int) error {
	if priority < -1 || priority > 20 {
		return nil
	}
	expected := antigravityKeeperPriorityForType(accountType, rules)
	if expected != nil && *expected == priority {
		return nil
	}
	if accountType == nil || expected == nil {
		return validationError("该账号类型没有可设置的系统 priority")
	}
	return validationError(fmt.Sprintf("只能设置小于 -1、大于 20，或当前账号类型 %s 对应的 priority %d", *accountType, *expected))
}

func isBadAntigravityKeeperCredential(result antigravityKeeperHTTPResult) bool {
	if result.StatusCode != nil && (*result.StatusCode == 401 || *result.StatusCode == 402) {
		return true
	}
	text := strings.ToLower(result.Brief)
	if result.JSONData != nil {
		payload, _ := json.Marshal(result.JSONData)
		text += " " + strings.ToLower(string(payload))
	}
	return strings.Contains(text, "workspace") && (strings.Contains(text, "disabled") || strings.Contains(text, "deactivated"))
}

func (a *App) preserveAntigravityKeeperBadCredentialDiagnosis(ctx context.Context, result *antigravityKeeperAccountResult) {
	state, err := a.getAntigravityKeeperState(ctx, result.Name)
	if err != nil || !isAntigravityKeeperBadCredentialDisableAction(state.LatestAction) {
		return
	}
	result.LastStatusCode = state.LastStatusCode
	result.LastError = antigravityCloneStringPtr(state.LastError)
	result.LatestAction = antigravityCloneStringPtr(state.LatestAction)
}

func isAntigravityKeeperRecoverableUnauthorizedDisabledState(state *antigravityKeeperAuthState) bool {
	return state != nil &&
		state.Disabled &&
		state.LastStatusCode != nil &&
		*state.LastStatusCode == http.StatusUnauthorized &&
		isAntigravityKeeperBadCredentialDisableAction(state.LatestAction)
}

func isAntigravityKeeperBadCredentialDisableAction(action *string) bool {
	if action == nil {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(*action), "禁用凭证")
}

func antigravityKeeperBodyJSON(value any) map[string]any {
	if object, ok := value.(map[string]any); ok {
		return object
	}
	text, ok := value.(string)
	if !ok {
		return nil
	}
	var object map[string]any
	if json.Unmarshal([]byte(text), &object) != nil {
		return nil
	}
	return object
}

func antigravityKeeperAuthIndex(detail map[string]any) string {
	for _, key := range []string{"auth_index", "authIndex", "index", "name"} {
		if value := antigravityKeeperString(detail[key]); value != "" {
			return value
		}
	}
	return "unknown"
}

func antigravityKeeperProjectID(detail map[string]any) string {
	for _, value := range []string{
		antigravityKeeperString(detail["project_id"]),
		antigravityKeeperString(detail["projectId"]),
		antigravityKeeperString(detail["cloudaicompanionProject"]),
		antigravityKeeperString(detail["cloudAiCompanionProject"]),
		antigravityKeeperNestedString(detail, "installed", "project_id"),
		antigravityKeeperNestedString(detail, "installed", "projectId"),
		antigravityKeeperNestedString(detail, "web", "project_id"),
		antigravityKeeperNestedString(detail, "web", "projectId"),
	} {
		if value != "" {
			return value
		}
	}
	return antigravityKeeperDefaultProjectID
}

func antigravityKeeperNestedString(object map[string]any, keys ...string) string {
	var current any = object
	for _, key := range keys {
		next, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = next[key]
	}
	return antigravityKeeperString(current)
}

func antigravityKeeperString(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(text)
}

func antigravityKeeperStringPtr(values ...any) *string {
	for _, value := range values {
		if text := antigravityKeeperString(value); text != "" {
			return &text
		}
	}
	return nil
}

func antigravityKeeperIntPtr(values ...any) *int {
	for _, value := range values {
		switch typed := value.(type) {
		case nil:
			continue
		case int:
			return &typed
		case int64:
			converted := int(typed)
			return &converted
		case float64:
			converted := int(typed)
			return &converted
		case string:
			parsed, err := strconv.Atoi(strings.TrimSpace(typed))
			if err == nil {
				return &parsed
			}
		}
	}
	return nil
}

func antigravityKeeperBool(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		normalized := strings.ToLower(strings.TrimSpace(typed))
		return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on"
	case float64:
		return typed != 0
	default:
		return false
	}
}

func antigravityBoolValue(value *bool) bool {
	return value != nil && *value
}

func antigravityBriefPayload(payload []byte) string {
	text := strings.TrimSpace(string(payload))
	if len(text) > 160 {
		return text[:160] + "..."
	}
	return text
}

func antigravityBriefAny(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		if len(typed) > 160 {
			return typed[:160] + "..."
		}
		return typed
	default:
		payload, _ := json.Marshal(typed)
		return antigravityBriefPayload(payload)
	}
}

func normalizeAntigravityKeeperAuthNames(raw []string) ([]string, error) {
	result, err := normalizeOptionalAntigravityKeeperAuthNames(raw)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, validationError("账号名称不能为空")
	}
	return result, nil
}

func normalizeOptionalAntigravityKeeperAuthNames(raw []string) ([]string, error) {
	seen := map[string]bool{}
	result := []string{}
	for _, item := range raw {
		name := strings.TrimSpace(item)
		if name == "" {
			return nil, validationError("账号名称不能为空")
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		result = append(result, name)
	}
	return result, nil
}

func antigravityWaitForStop(stop <-chan struct{}, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-stop:
		return true
	case <-timer.C:
		return false
	}
}

func antigravityCloneStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func antigravityCloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func sortAntigravityKeeperAccounts(accounts []antigravityKeeperAccount) {
	sort.Slice(accounts, func(i, j int) bool {
		left := valueOr(accounts[i].Email, "") + accounts[i].Name
		right := valueOr(accounts[j].Email, "") + accounts[j].Name
		return left < right
	})
}
