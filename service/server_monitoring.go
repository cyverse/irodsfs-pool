package service

import (
	"fmt"
	"net/http"
	"syscall"
	"time"

	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"

	"github.com/cyverse/irodsfs-pool/commons"
)

type MonitoringHandler struct {
	poolServer *PoolServer
	config     *commons.Config
	startTime  time.Time
}

func NewMonitoringHandler(poolServer *PoolServer, config *commons.Config) *MonitoringHandler {
	return &MonitoringHandler{
		poolServer: poolServer,
		config:     config,
		startTime:  time.Now(),
	}
}

func (h *MonitoringHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	fmt.Fprint(w, `<!DOCTYPE html><html><head><meta charset="utf-8">`)
	fmt.Fprint(w, `<meta http-equiv="refresh" content="10">`)
	fmt.Fprint(w, `<title>irodsfs-pool Monitor</title>`)
	fmt.Fprint(w, `<style>
body { font-family: monospace; margin: 20px; background: #1a1a2e; color: #eee; }
h1 { color: #0af; }
h2 { color: #0af; margin-top: 30px; border-bottom: 1px solid #333; padding-bottom: 5px; }
table { border-collapse: collapse; width: 100%; margin-top: 10px; }
th, td { border: 1px solid #333; padding: 6px 10px; text-align: left; }
th { background: #16213e; }
tr:nth-child(even) { background: #1a1a2e; }
tr:nth-child(odd) { background: #0f3460; }
.bar { background: #222; border-radius: 4px; height: 20px; position: relative; }
.bar-fill { background: #0af; height: 100%; border-radius: 4px; }
.bar-text { position: absolute; top: 0; left: 8px; line-height: 20px; font-size: 12px; }
.info { color: #888; }
</style>`)
	fmt.Fprint(w, `</head><body>`)

	h.renderServerInfo(w)
	h.renderCacheInfo(w)
	h.renderStagingInfo(w)
	h.renderSessions(w)
	h.renderMetrics(w)

	fmt.Fprintf(w, `<p class="info">Last refreshed: %s</p>`, time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprint(w, `</body></html>`)
}

func (h *MonitoringHandler) renderServerInfo(w http.ResponseWriter) {
	version := commons.GetVersion()
	uptime := time.Since(h.startTime).Truncate(time.Second)

	fmt.Fprint(w, `<h1>irodsfs-pool Monitor</h1>`)
	fmt.Fprint(w, `<table>`)
	fmt.Fprintf(w, `<tr><th>Version</th><td>%s</td></tr>`, version.ServiceVersion)
	fmt.Fprintf(w, `<tr><th>Git Commit</th><td>%s</td></tr>`, version.GitCommit)
	fmt.Fprintf(w, `<tr><th>Uptime</th><td>%s</td></tr>`, uptime)
	fmt.Fprintf(w, `<tr><th>Endpoint</th><td>%s</td></tr>`, h.config.GetServiceEndpoint())
	fmt.Fprintf(w, `<tr><th>Data Root</th><td>%s</td></tr>`, h.config.DataRootPath)
	if h.config.MonitoringServicePort > 0 {
		fmt.Fprintf(w, `<tr><th>Monitoring</th><td>:%d/monitor</td></tr>`, h.config.MonitoringServicePort)
		fmt.Fprintf(w, `<tr><th>Prometheus</th><td>:%d/metrics</td></tr>`, h.config.MonitoringServicePort)
	}
	fmt.Fprint(w, `</table>`)
}

func (h *MonitoringHandler) renderCacheInfo(w http.ResponseWriter) {
	fmt.Fprint(w, `<h2>Memory Cache</h2>`)

	cacheManager := h.poolServer.GetSessionManager().GetCacheManager()

	var cacheUsed, cacheMax int64
	var cacheCount int
	if cacheManager != nil {
		cacheUsed = cacheManager.GetTotalSize()
		cacheMax = cacheManager.GetMaxSize()
		cacheCount = cacheManager.GetCount()
	}

	sysTotal, sysAvail := getSystemMemoryInfo()

	fmt.Fprint(w, `<table>`)
	fmt.Fprintf(w, `<tr><th>Cache Used</th><td>%s / %s</td></tr>`, formatBytes(cacheUsed), formatBytes(cacheMax))
	if cacheMax > 0 {
		pct := float64(cacheUsed) / float64(cacheMax) * 100
		fmt.Fprintf(w, `<tr><th>Cache Usage</th><td>%s</td></tr>`, renderBar(pct))
	}
	fmt.Fprintf(w, `<tr><th>Cache Entries</th><td>%d</td></tr>`, cacheCount)
	fmt.Fprintf(w, `<tr><th>System RAM</th><td>%s total, %s available</td></tr>`, formatBytes(int64(sysTotal)), formatBytes(int64(sysAvail)))
	if sysAvail > 0 && sysAvail < uint64(cacheMax) {
		fmt.Fprintf(w, `<tr><th style="color:#f44;background:#3a1010">⚠ WARNING</th><td style="color:#f44">Insufficient memory: available %s < configured cache %s</td></tr>`, formatBytes(int64(sysAvail)), formatBytes(cacheMax))
	}
	fmt.Fprint(w, `</table>`)
}

func (h *MonitoringHandler) renderStagingInfo(w http.ResponseWriter) {
	fmt.Fprint(w, `<h2>Staging</h2>`)

	var stagingPath string
	if len(h.config.StagingRootPath) > 0 {
		stagingPath = h.config.StagingRootPath
	} else {
		stagingPath = h.config.GetDataStagingRootPath()
	}

	diskTotal, diskFree := getDiskInfo(stagingPath)

	sessions := h.poolServer.GetSessionManager().GetAllSessions()

	var totalStagedSize int64
	var totalStagedMax int64
	type stagingItem struct {
		sessionUser string
		path        string
		action      string
		modified    time.Time
		failCount   int
	}
	var items []stagingItem

	for _, session := range sessions {
		if session.fsClient == nil {
			continue
		}
		bufferedClient, ok := session.fsClient.(*irodsfs_common_irods.IRODSFSClientBuffered)
		if !ok {
			continue
		}
		stagingFS := bufferedClient.GetStagingFS()
		if stagingFS == nil {
			continue
		}
		totalStagedSize += stagingFS.GetCurrentDataSize()
		totalStagedMax += stagingFS.GetMaxDataSize()

		user := session.irodsAccount.ClientUser
		allMeta := stagingFS.GetAll()
		for _, meta := range allMeta {
			items = append(items, stagingItem{
				sessionUser: user,
				path:        meta.Path,
				action:      meta.Action.String(),
				modified:    meta.LastModifiedAt,
				failCount:   meta.SyncFailCount,
			})
		}
	}

	minDiskRequired := uint64(1 << 30) // 1 GB

	fmt.Fprint(w, `<table>`)
	fmt.Fprintf(w, `<tr><th>Staged Data</th><td>%s / %s (configured max)</td></tr>`, formatBytes(totalStagedSize), formatBytes(totalStagedMax))
	fmt.Fprintf(w, `<tr><th>Disk</th><td>%s total, %s free</td></tr>`, formatBytes(int64(diskTotal)), formatBytes(int64(diskFree)))
	fmt.Fprintf(w, `<tr><th>Staging Path</th><td>%s</td></tr>`, stagingPath)
	fmt.Fprintf(w, `<tr><th>Staged Files</th><td>%d</td></tr>`, len(items))
	if diskFree > 0 && diskFree < minDiskRequired {
		fmt.Fprintf(w, `<tr><th style="color:#f44;background:#3a1010">⚠ WARNING</th><td style="color:#f44">Low disk space: available %s < recommended minimum %s</td></tr>`, formatBytes(int64(diskFree)), formatBytes(int64(minDiskRequired)))
	}
	fmt.Fprint(w, `</table>`)

	if len(items) > 0 {
		fmt.Fprint(w, `<table><tr><th>User</th><th>Path</th><th>Action</th><th>Modified</th><th>Failures</th></tr>`)
		for _, item := range items {
			fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%d</td></tr>`,
				item.sessionUser, item.path, item.action, item.modified.Format("15:04:05"), item.failCount)
		}
		fmt.Fprint(w, `</table>`)
	}
}

func (h *MonitoringHandler) renderSessions(w http.ResponseWriter) {
	fmt.Fprint(w, `<h2>Sessions</h2>`)

	sessions := h.poolServer.GetSessionManager().GetAllSessions()

	fmt.Fprintf(w, `<p>Total: %d</p>`, len(sessions))
	if len(sessions) == 0 {
		return
	}

	fmt.Fprint(w, `<table><tr><th>ID</th><th>User</th><th>Host</th><th>Last Access</th><th>Connections</th><th>File Handles</th></tr>`)
	for _, session := range sessions {
		session.mutex.RLock()
		handleCount := len(session.poolFileHandles)
		connCount := len(session.connections)
		lastAccess := session.lastAccessTime
		session.mutex.RUnlock()

		account := session.irodsAccount
		userInfo := fmt.Sprintf("%s@%s", account.ClientUser, account.ClientZone)
		hostInfo := fmt.Sprintf("%s:%d", account.Host, account.Port)

		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%d</td></tr>`,
			session.id, userInfo, hostInfo, lastAccess.Format("15:04:05"), connCount, handleCount)
	}
	fmt.Fprint(w, `</table>`)

	fmt.Fprint(w, `<h2>Open File Handles</h2>`)
	hasHandles := false
	for _, session := range sessions {
		session.mutex.RLock()
		if len(session.poolFileHandles) == 0 {
			session.mutex.RUnlock()
			continue
		}
		if !hasHandles {
			fmt.Fprint(w, `<table><tr><th>User</th><th>Path</th><th>Mode</th></tr>`)
			hasHandles = true
		}
		user := session.irodsAccount.ClientUser
		for _, handle := range session.poolFileHandles {
			fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td>%s</td></tr>`,
				user, handle.GetEntryPath(), handle.GetOpenMode())
		}
		session.mutex.RUnlock()
	}
	if hasHandles {
		fmt.Fprint(w, `</table>`)
	} else {
		fmt.Fprint(w, `<p>No open file handles</p>`)
	}
}

func (h *MonitoringHandler) renderMetrics(w http.ResponseWriter) {
	fmt.Fprint(w, `<h2>I/O Metrics (cumulative)</h2>`)

	total := h.poolServer.GetTotalMetrics()
	m := &total

	var hitRatio float64
	if m.CacheHit+m.CacheMiss > 0 {
		hitRatio = float64(m.CacheHit) / float64(m.CacheHit+m.CacheMiss) * 100
	}

	fmt.Fprint(w, `<table>`)
	fmt.Fprintf(w, `<tr><th>Bytes Sent (to iRODS)</th><td>%s</td></tr>`, formatBytes(int64(m.BytesSent)))
	fmt.Fprintf(w, `<tr><th>Bytes Received (from iRODS)</th><td>%s</td></tr>`, formatBytes(int64(m.BytesReceived)))
	fmt.Fprintf(w, `<tr><th>Cache Hit / Miss</th><td>%d / %d (%.1f%% hit)</td></tr>`, m.CacheHit, m.CacheMiss, hitRatio)
	fmt.Fprintf(w, `<tr><th>Request Failures</th><td>%d</td></tr>`, m.RequestFailures)
	fmt.Fprintf(w, `<tr><th>Connection Failures</th><td>%d</td></tr>`, m.ConnectionFailures)
	fmt.Fprint(w, `</table>`)
}

func renderBar(pct float64) string {
	if pct > 100 {
		pct = 100
	}
	return fmt.Sprintf(`<div class="bar"><div class="bar-fill" style="width:%.1f%%"></div><span class="bar-text">%.1f%%</span></div>`, pct, pct)
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func getSystemMemoryInfo() (total, available uint64) {
	var info syscall.Sysinfo_t
	err := syscall.Sysinfo(&info)
	if err != nil {
		return 0, 0
	}
	total = info.Totalram * uint64(info.Unit)
	available = info.Freeram * uint64(info.Unit)
	return
}

func getDiskInfo(path string) (total, free uint64) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	if err != nil {
		return 0, 0
	}
	total = stat.Blocks * uint64(stat.Bsize)
	free = stat.Bavail * uint64(stat.Bsize)
	return
}
