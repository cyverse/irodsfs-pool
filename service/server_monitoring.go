package service

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
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
	fmt.Fprint(w, `<title>irodsfs-pool Monitor</title>`)
	fmt.Fprint(w, `<style>
body { font-family: monospace; margin: 20px; background: #1a1a2e; color: #eee; }
h1 { color: #0af; }
h2 { color: #0af; margin-top: 30px; border-bottom: 1px solid #333; padding-bottom: 5px; }
h3 { color: #0cf; margin-top: 16px; }
table { border-collapse: collapse; width: 100%; margin-top: 10px; }
th, td { border: 1px solid #333; padding: 6px 10px; text-align: left; }
th { background: #16213e; }
tr:nth-child(even) { background: #1a1a2e; }
tr:nth-child(odd) { background: #0f3460; }
tr.clickable:hover { background: #1a4a80; cursor: pointer; }
.bar { background: #222; border-radius: 4px; height: 20px; position: relative; }
.bar-fill { background: #0af; height: 100%; border-radius: 4px; }
.bar-text { position: absolute; top: 0; left: 8px; line-height: 20px; font-size: 12px; }
.info { color: #888; }
.badge { display: inline-block; background: #1e3a5f; color: #8cf; padding: 1px 6px; border-radius: 3px; font-size: 11px; margin: 1px; font-family: monospace; }
.grace { background: #3d2600; color: #ffc875; }
.dirty { color: #f84; font-weight: bold; }
.cached { color: #4c4; }
#modal-overlay { display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.75); z-index:100; overflow:auto; }
#modal-box { background:#16213e; margin:40px auto; max-width:960px; padding:24px; border-radius:8px; position:relative; border:1px solid #333; }
#modal-close { position:absolute; top:12px; right:14px; background:none; border:1px solid #555; color:#ccc; font-size:18px; cursor:pointer; padding:2px 10px; border-radius:4px; }
#modal-close:hover { background:#333; }
#modal-box table tr:nth-child(even) { background:#1a2a4e; }
#modal-box table tr:nth-child(odd) { background:#0f2040; }
</style>`)
	fmt.Fprint(w, `</head><body>`)

	h.renderServerInfo(w)
	h.renderCacheInfo(w)
	h.renderStagingInfo(w)
	h.renderSessions(w)
	h.renderMetrics(w)

	h.renderSessionDetails(w)

	fmt.Fprintf(w, `<p class="info">Last refreshed: %s</p>`, time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprint(w, `<script>
setTimeout(function(){ location.reload(); }, 10000);
function showDetail(id) {
  var src = document.getElementById('detail-' + id);
  if (!src) {
    try { sessionStorage.removeItem('_openDetail'); } catch(e) {}
    return;
  }
  document.getElementById('modal-content').innerHTML = src.innerHTML;
  document.getElementById('modal-overlay').style.display = 'block';
  try { sessionStorage.setItem('_openDetail', id); } catch(e) {}
}
function closeDetail() {
  document.getElementById('modal-overlay').style.display = 'none';
  try { sessionStorage.removeItem('_openDetail'); } catch(e) {}
}
(function(){
  try {
    var id = sessionStorage.getItem('_openDetail');
    if (id) showDetail(id);
  } catch(e) {}
})();
</script>`)
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
	var totalStagedFiles int
	stagingMax := h.config.MaxStagingDataSize

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
		totalStagedFiles += len(stagingFS.GetAll())
	}

	fmt.Fprint(w, `<table>`)
	fmt.Fprintf(w, `<tr><th>Staged Data</th><td>%s / %s (configured max)</td></tr>`, formatBytes(totalStagedSize), formatBytes(stagingMax))
	fmt.Fprintf(w, `<tr><th>Disk</th><td>%s total, %s free</td></tr>`, formatBytes(int64(diskTotal)), formatBytes(int64(diskFree)))
	fmt.Fprintf(w, `<tr><th>Staging Path</th><td>%s</td></tr>`, stagingPath)
	fmt.Fprintf(w, `<tr><th>Staged Files</th><td>%d (click a session row for details)</td></tr>`, totalStagedFiles)
	if diskFree > 0 && diskFree < uint64(stagingMax) {
		fmt.Fprintf(w, `<tr><th style="color:#f44;background:#3a1010">⚠ WARNING</th><td style="color:#f44">Insufficient disk space: available %s &lt; configured staging max %s</td></tr>`, formatBytes(int64(diskFree)), formatBytes(stagingMax))
	}
	fmt.Fprint(w, `</table>`)
}

func (h *MonitoringHandler) renderSessions(w http.ResponseWriter) {
	fmt.Fprint(w, `<h2>Sessions</h2>`)

	sessions := h.poolServer.GetSessionManager().GetAllSessions()

	fmt.Fprintf(w, `<p>Total: %d</p>`, len(sessions))
	if len(sessions) == 0 {
		return
	}

	fmt.Fprint(w, `<table><tr><th>ID</th><th>User</th><th>Host</th><th>Last Access</th><th>Clients</th><th>File Handles</th><th>iRODS Conns</th></tr>`)
	for _, session := range sessions {
		session.mutex.RLock()
		handleCount := len(session.poolFileHandles)
		type connEntry struct{ id, app, desc string }
		connEntries := make([]connEntry, 0, len(session.connections))
		for id, ci := range session.connections {
			connEntries = append(connEntries, connEntry{id, ci.appName, ci.description})
		}
		lastAccess := session.lastAccessTime
		session.mutex.RUnlock()

		var irodsConns int
		if session.fsClient != nil {
			irodsConns = session.fsClient.GetOpenConnections()
		}

		sort.Slice(connEntries, func(i, j int) bool { return connEntries[i].id < connEntries[j].id })

		account := session.irodsAccount
		userInfo := fmt.Sprintf("%s@%s", account.ClientUser, account.ClientZone)
		hostInfo := fmt.Sprintf("%s:%d", account.Host, account.Port)

		var clientsCell string
		if len(connEntries) == 0 {
			clientsCell = `<span class="badge grace">⏳ grace period</span>`
		} else {
			var sb strings.Builder
			fmt.Fprintf(&sb, `<span style="margin-right:4px">%d</span>`, len(connEntries))
			for _, e := range connEntries {
				tooltip := e.app
				if e.desc != "" {
					tooltip = e.app + ": " + e.desc
				}
				fmt.Fprintf(&sb, `<span class="badge" title="%s">%s(%s)</span>`, tooltip, e.id, e.app)
			}
			clientsCell = sb.String()
		}

		fmt.Fprintf(w, `<tr class="clickable" onclick="showDetail('%s')"><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%d</td></tr>`,
			session.id, session.id[:12]+"…", userInfo, hostInfo, lastAccess.Format("15:04:05"), clientsCell, handleCount, irodsConns)
	}
	fmt.Fprint(w, `</table>`)
	fmt.Fprint(w, `<p class="info">Click a row to see staged files, sync status, and open file handles.</p>`)
}

func (h *MonitoringHandler) renderSessionDetails(w http.ResponseWriter) {
	// Modal overlay (hidden; JS populates #modal-content on row click)
	fmt.Fprint(w, `<div id="modal-overlay"><div id="modal-box"><button id="modal-close" onclick="closeDetail()">✕</button><div id="modal-content"></div></div></div>`)

	sessions := h.poolServer.GetSessionManager().GetAllSessions()
	for _, session := range sessions {
		h.renderOneSessionDetail(w, session)
	}
}

func (h *MonitoringHandler) renderOneSessionDetail(w http.ResponseWriter, session *PoolSession) {
	// Collect data under lock
	session.mutex.RLock()
	type connEntry struct{ id, app, desc string }
	connEntries := make([]connEntry, 0, len(session.connections))
	for id, ci := range session.connections {
		connEntries = append(connEntries, connEntry{id, ci.appName, ci.description})
	}
	type handleEntry struct{ path, mode string }
	handleEntries := make([]handleEntry, 0, len(session.poolFileHandles))
	for _, h2 := range session.poolFileHandles {
		handleEntries = append(handleEntries, handleEntry{h2.GetEntryPath(), string(h2.GetOpenMode())})
	}
	lastAccess := session.lastAccessTime
	session.mutex.RUnlock()

	sort.Slice(connEntries, func(i, j int) bool { return connEntries[i].id < connEntries[j].id })
	sort.Slice(handleEntries, func(i, j int) bool { return handleEntries[i].path < handleEntries[j].path })

	// Collect staging data
	type stagingEntry struct {
		path      string
		oldPath   string
		action    string
		fileState string
		modified  time.Time
		failCount int
	}
	var stagingEntries []stagingEntry
	if session.fsClient != nil {
		if bufferedClient, ok := session.fsClient.(*irodsfs_common_irods.IRODSFSClientBuffered); ok {
			if stagingFS := bufferedClient.GetStagingFS(); stagingFS != nil {
				for _, meta := range stagingFS.GetAll() {
					stagingEntries = append(stagingEntries, stagingEntry{
						path:      meta.Path,
						oldPath:   meta.OldPath,
						action:    meta.Action.String(),
						fileState: meta.FileState.String(),
						modified:  meta.LastModifiedAt,
						failCount: meta.SyncFailCount,
					})
				}
			}
		}
	}
	sort.Slice(stagingEntries, func(i, j int) bool { return stagingEntries[i].path < stagingEntries[j].path })

	account := session.irodsAccount
	userInfo := fmt.Sprintf("%s@%s", account.ClientUser, account.ClientZone)
	hostInfo := fmt.Sprintf("%s:%d", account.Host, account.Port)

	fmt.Fprintf(w, `<div id="detail-%s" style="display:none">`, session.id)
	fmt.Fprintf(w, `<h3>Session Detail</h3>`)
	fmt.Fprint(w, `<table>`)
	fmt.Fprintf(w, `<tr><th>ID</th><td>%s</td></tr>`, session.id)
	fmt.Fprintf(w, `<tr><th>User</th><td>%s</td></tr>`, userInfo)
	fmt.Fprintf(w, `<tr><th>Host</th><td>%s</td></tr>`, hostInfo)
	fmt.Fprintf(w, `<tr><th>Last Access</th><td>%s</td></tr>`, lastAccess.Format("2006-01-02 15:04:05"))
	fmt.Fprint(w, `</table>`)

	// Clients
	fmt.Fprintf(w, `<h3>Clients (%d)</h3>`, len(connEntries))
	if len(connEntries) == 0 {
		fmt.Fprint(w, `<p><span class="badge grace">⏳ grace period — no connected clients</span></p>`)
	} else {
		fmt.Fprint(w, `<table><tr><th>Connection ID</th><th>Application</th><th>Description</th></tr>`)
		for _, e := range connEntries {
			tooltip := e.app
			if e.desc != "" {
				tooltip = e.app + ": " + e.desc
			}
			fmt.Fprintf(w, `<tr><td>%s</td><td title="%s">%s</td><td>%s</td></tr>`, e.id, tooltip, e.app, e.desc)
		}
		fmt.Fprint(w, `</table>`)
	}

	// Staged files
	fmt.Fprintf(w, `<h3>Staged Files (%d)</h3>`, len(stagingEntries))
	if len(stagingEntries) == 0 {
		fmt.Fprint(w, `<p>No staged files.</p>`)
	} else {
		fmt.Fprint(w, `<table><tr><th>Path</th><th>Action</th><th>Sync Status</th><th>Modified</th><th>Failures</th></tr>`)
		for _, e := range stagingEntries {
			stateClass := "cached"
			if e.fileState == "DIRTY" {
				stateClass = "dirty"
			}
			pathCell := e.path
			if e.oldPath != "" {
				pathCell = fmt.Sprintf("%s<br><span style='color:#888;font-size:11px'>← %s</span>", e.path, e.oldPath)
			}
			fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td class="%s">%s</td><td>%s</td><td>%d</td></tr>`,
				pathCell, e.action, stateClass, e.fileState, e.modified.Format("15:04:05"), e.failCount)
		}
		fmt.Fprint(w, `</table>`)
	}

	// Open file handles
	fmt.Fprintf(w, `<h3>Open File Handles (%d)</h3>`, len(handleEntries))
	if len(handleEntries) == 0 {
		fmt.Fprint(w, `<p>No open file handles.</p>`)
	} else {
		fmt.Fprint(w, `<table><tr><th>Path</th><th>Mode</th></tr>`)
		for _, e := range handleEntries {
			fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td></tr>`, e.path, e.mode)
		}
		fmt.Fprint(w, `</table>`)
	}

	fmt.Fprint(w, `</div>`) // end detail div
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
