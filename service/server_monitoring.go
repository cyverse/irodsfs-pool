package service

import (
	"bufio"
	"fmt"
	"html"
	"html/template"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_packedfs "github.com/cyverse/irodsfs-common/irods/packedfs"

	"github.com/cyverse/irodsfs-pool/commons"
)

type MonitoringHandler struct {
	poolServer *PoolServer
	config     *commons.Config
	startTime  time.Time
}

func NewMonitoringHandler(poolServer *PoolServer, config *commons.Config) *MonitoringHandler {
	return newMonitoringHandler(poolServer, config, time.Now())
}

func newMonitoringHandler(poolServer *PoolServer, config *commons.Config, startTime time.Time) *MonitoringHandler {
	return &MonitoringHandler{
		poolServer: poolServer,
		config:     config,
		startTime:  startTime,
	}
}

func (h *MonitoringHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case strings.HasPrefix(path, "/monitor/session/"):
		sessionID := strings.TrimPrefix(path, "/monitor/session/")
		h.serveSessionDetailPage(w, sessionID)
		return
	case strings.HasPrefix(path, "/monitor/recovery-session/"):
		sessionID := strings.TrimPrefix(path, "/monitor/recovery-session/")
		h.serveFailedSessionDetailPage(w, sessionID)
		return
	}
	h.serveMainPage(w)
}

func (h *MonitoringHandler) writePageHeader(w http.ResponseWriter, title string) {
	fmt.Fprint(w, `<!DOCTYPE html><html><head><meta charset="utf-8">`)
	fmt.Fprintf(w, `<title>%s</title>`, title)
	fmt.Fprint(w, `<style>`)
	h.writeCommonCSS(w)
	fmt.Fprint(w, `</style></head><body>`)
}

func (h *MonitoringHandler) writeCommonCSS(w http.ResponseWriter) {
	fmt.Fprint(w, `
body { font-family: monospace; margin: 20px; background: #1a1a2e; color: #eee; }
h1 { color: #0af; }
h2 { color: #0af; margin-top: 30px; border-bottom: 1px solid #333; padding-bottom: 5px; }
h3 { color: #0cf; margin-top: 16px; }
table { border-collapse: collapse; width: 100%; margin-top: 10px; }
th, td { border: 1px solid #333; padding: 6px 10px; text-align: left; }
.clients-table { table-layout: fixed; }
.clients-table th, .clients-table td { overflow-wrap: anywhere; word-break: break-word; white-space: normal; }
.clients-table th:nth-child(1) { width: 25%; }
.clients-table th:nth-child(2) { width: 15%; }
.clients-table th:nth-child(3) { width: 60%; }
.staged-files-table { table-layout: fixed; }
.staged-files-table th, .staged-files-table td { overflow-wrap: anywhere; word-break: break-word; white-space: normal; }
.staged-files-table th:nth-child(1) { width: 48%; }
.staged-files-table th:nth-child(2) { width: 14%; }
.staged-files-table th:nth-child(3) { width: 14%; }
.staged-files-table th:nth-child(4) { width: 14%; }
.staged-files-table th:nth-child(5) { width: 10%; }
.handles-table { table-layout: fixed; }
.handles-table th, .handles-table td { overflow-wrap: anywhere; word-break: break-word; white-space: normal; }
.handles-table th:nth-child(1) { width: 85%; }
.handles-table th:nth-child(2) { width: 15%; }
.packed-dirs-table { table-layout: fixed; }
.packed-dirs-table th, .packed-dirs-table td { overflow-wrap: anywhere; word-break: break-word; white-space: normal; }
.packed-dirs-table th:nth-child(1) { width: 30%; }
.packed-dirs-table th:nth-child(2) { width: 30%; }
.packed-dirs-table th:nth-child(3) { width: 12%; }
.packed-dirs-table th:nth-child(4) { width: 10%; }
.packed-dirs-table th:nth-child(5) { width: 9%; }
.packed-dirs-table th:nth-child(6) { width: 9%; }
th { background: #16213e; }
tr:nth-child(even) { background: #1a1a2e; }
tr:nth-child(odd) { background: #0f3460; }
tr.clickable:hover { background: #1a4a80; cursor: pointer; }
.bar { display:flex; height:20px; border-radius:4px; overflow:hidden; background:#222; position:relative; }
.bar-used { background:#0af; height:100%; }
.bar-free { background:#555; height:100%; }
.bar-text { position:absolute; top:0; left:8px; line-height:20px; font-size:12px; color:#fff; }
.usage-table { table-layout: fixed; }
.usage-table td { overflow-wrap: anywhere; }
.usage-table col.usage-name { width: 12%; }
.hm-bar { display:flex; height:20px; border-radius:4px; overflow:hidden; background:#222; position:relative; }
.hm-hit { background:#4c4; height:100%; }
.hm-miss { background:#f44; height:100%; }
.hm-text { position:absolute; top:0; left:8px; line-height:20px; font-size:12px; color:#fff; }
.bar-text-right { left:auto; right:8px; }
.info { color: #888; }
.badge { display: inline-block; background: #1e3a5f; color: #8cf; padding: 1px 6px; border-radius: 3px; font-size: 11px; margin: 1px; font-family: monospace; }
.grace { background: #3d2600; color: #ffc875; }
.releasing { background: #1f2d4d; color: #8ab4ff; }
.dirty { color: #f84; font-weight: bold; }
.cached { color: #4c4; }
.action-btn { background:#1e3a5f; color:#8cf; border:1px solid #335; padding:6px 14px; border-radius:4px; cursor:pointer; font-family:monospace; font-size:13px; }
.action-btn:hover { background:#2a4a7f; }
.action-btn-warn { background:#3d2600; color:#ffc875; border-color:#554400; }
.action-btn-warn:hover { background:#5a3a00; }
.action-result-ok  { color:#4c4; margin:4px 0; }
.action-result-err { color:#f44; margin:4px 0; }
.errors-table { table-layout: fixed; }
.errors-table th, .errors-table td { overflow-wrap: anywhere; word-break: break-word; white-space: normal; }
.errors-table th:nth-child(1) { width: 13%; }
.errors-table th:nth-child(2) { width: 6%; }
.errors-table th:nth-child(3) { width: 10%; }
.errors-table th:nth-child(4) { width: 14%; }
.errors-table th:nth-child(5) { width: 24%; }
.errors-table th:nth-child(6) { width: 33%; }
.stack-trace { white-space: pre-wrap; overflow-wrap: anywhere; background: #0b1530; border: 1px solid #333; padding: 10px; font-size: 12px; max-height: 60vh; overflow: auto; }
#modal-overlay { display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.75); z-index:100; overflow:auto; }
#modal-box { background:#16213e; margin:40px auto; max-width:960px; padding:24px; border-radius:8px; position:relative; border:1px solid #333; }
#modal-close { position:absolute; top:12px; right:14px; background:none; border:1px solid #555; color:#ccc; font-size:18px; cursor:pointer; padding:2px 10px; border-radius:4px; }
#modal-close:hover { background:#333; }
.scroll-section { max-height: 400px; overflow: auto; border: 1px solid #333; border-radius: 4px; margin-top: 4px; }
.scroll-section table { margin-top: 0; }
.detail-header { display: flex; justify-content: space-between; align-items: flex-start; }
.detail-header h1 { margin: 0; }
.detail-actions { display: flex; gap: 10px; align-items: center; flex-shrink: 0; }
.back-link { color: #0af; text-decoration: none; }
.back-link:hover { text-decoration: underline; }
`)
}

func (h *MonitoringHandler) serveMainPage(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	fmt.Fprint(w, `<!DOCTYPE html><html><head><meta charset="utf-8">`)
	fmt.Fprint(w, `<title>iRODS FS Pool Server Monitor</title>`)
	fmt.Fprint(w, `<style>`)
	h.writeCommonCSS(w)
	fmt.Fprint(w, `</style></head><body>`)

	h.renderServerInfo(w)
	h.renderCacheInfo(w)
	h.renderStagingInfo(w)
	h.renderSessions(w)
	h.renderFailedSessions(w)
	h.renderMetrics(w)
	errorGroups, errorTotal := h.poolServer.GetErrors()
	renderErrors(w, errorGroups, errorTotal)

	// Modal for error details
	fmt.Fprint(w, `<div id="modal-overlay"><div id="modal-box"><button id="modal-close" onclick="closeDetail()">✕</button><div id="modal-content"></div></div></div>`)
	renderErrorDetails(w, errorGroups)

	fmt.Fprintf(w, `<p class="info">Last refreshed: %s</p>`, time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprint(w, `<script>
function showDetail(id) {
  var src = document.getElementById('detail-' + id);
  if (!src) return;
  document.getElementById('modal-content').innerHTML = src.innerHTML;
  document.getElementById('modal-overlay').style.display = 'block';
}
function closeDetail() {
  document.getElementById('modal-overlay').style.display = 'none';
}
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') closeDetail();
});
</script>`)
	fmt.Fprint(w, `</body></html>`)
}

func (h *MonitoringHandler) renderServerInfo(w http.ResponseWriter) {
	version := commons.GetVersion()
	uptime := time.Since(h.startTime).Truncate(time.Second)

	fmt.Fprint(w, `<h1>iRODS FS Pool Server Monitor</h1>`)
	fmt.Fprint(w, `<table>`)
	fmt.Fprintf(w, `<tr><th>Version</th><td>%s</td><th>Git Commit</th><td>%s</td></tr>`, version.ServiceVersion, version.GitCommit)
	fmt.Fprintf(w, `<tr><th>Started</th><td>%s (up %s)</td><th>Build Date</th><td>%s</td></tr>`, h.startTime.Format("2006-01-02 15:04:05 MST"), formatUptime(uptime), version.BuildDate)
	fmt.Fprintf(w, `<tr><th>Data Root</th><td>%s</td><th>Staging Path</th><td>%s</td></tr>`, html.EscapeString(h.config.DataRootPath), html.EscapeString(h.getStagingPath()))
	if endpoint := h.config.GetManagementServiceEndpoint(); endpoint != "" {
		fmt.Fprintf(w, `<tr><th>Endpoint</th><td>%s</td><th>Monitoring</th><td>%s/monitor</td></tr>`, h.config.GetServiceEndpoint(), endpoint)
		fmt.Fprintf(w, `<tr><th>REST API</th><td>%s/api</td><th>Prometheus</th><td>%s/metrics</td></tr>`, endpoint, endpoint)
	} else {
		fmt.Fprintf(w, `<tr><th>Endpoint</th><td colspan="3">%s</td></tr>`, h.config.GetServiceEndpoint())
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

	var cacheFree int64
	if cacheMax > cacheUsed {
		cacheFree = cacheMax - cacheUsed
	}

	fmt.Fprint(w, `<table class="usage-table"><colgroup><col class="usage-name"><col></colgroup>`)
	fmt.Fprintf(w, `<tr><th>Cache Usage</th><td>%s</td></tr>`, renderUsageBar(cacheUsed, cacheMax, fmt.Sprintf("%s free, %d entries", formatBytes(cacheFree), cacheCount)))
	fmt.Fprintf(w, `<tr><th>RAM Usage</th><td>%s</td></tr>`, renderUsageBar(int64(sysTotal)-int64(sysAvail), int64(sysTotal), fmt.Sprintf("%s available", formatBytes(int64(sysAvail)))))
	if isMemoryInsufficient(sysAvail, cacheUsed, cacheMax) {
		fmt.Fprintf(w, `<tr><th style="color:#f44;background:#3a1010">⚠ WARNING</th><td style="color:#f44">Insufficient memory: available %s &lt; remaining cache %s</td></tr>`, formatBytes(int64(sysAvail)), formatBytes(cacheMax-cacheUsed))
	}
	fmt.Fprint(w, `</table>`)
}

// getStagingPath returns the directory staged data is stored in
func (h *MonitoringHandler) getStagingPath() string {
	if len(h.config.StagingRootPath) > 0 {
		return h.config.StagingRootPath
	}
	return h.config.GetDataStagingRootPath()
}

func (h *MonitoringHandler) renderStagingInfo(w http.ResponseWriter) {
	fmt.Fprint(w, `<h2>Staging</h2>`)

	diskTotal, diskFree := getDiskInfo(h.getStagingPath())

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

	var stagingFree int64
	if stagingMax > totalStagedSize {
		stagingFree = stagingMax - totalStagedSize
	}

	fmt.Fprint(w, `<table class="usage-table"><colgroup><col class="usage-name"><col></colgroup>`)
	fmt.Fprintf(w, `<tr><th>Staging Usage</th><td>%s</td></tr>`, renderUsageBar(totalStagedSize, stagingMax, fmt.Sprintf("%s free, %d files", formatBytes(stagingFree), totalStagedFiles)))
	fmt.Fprintf(w, `<tr><th>Disk Usage</th><td>%s</td></tr>`, renderUsageBar(int64(diskTotal)-int64(diskFree), int64(diskTotal), fmt.Sprintf("%s free", formatBytes(int64(diskFree)))))
	if isDiskSpaceInsufficient(diskFree, totalStagedSize, stagingMax) {
		fmt.Fprintf(w, `<tr><th style="color:#f44;background:#3a1010">⚠ WARNING</th><td style="color:#f44">Insufficient disk space: available %s &lt; remaining staging %s</td></tr>`, formatBytes(int64(diskFree)), formatBytes(stagingMax-totalStagedSize))
	}
	fmt.Fprint(w, `</table>`)
}

func (h *MonitoringHandler) renderSessions(w http.ResponseWriter) {
	fmt.Fprint(w, `<h2>Sessions</h2>`)

	sessionManager := h.poolServer.GetSessionManager()
	sessions := sessionManager.GetAllSessions()
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].id < sessions[j].id })

	fmt.Fprintf(w, `<p>Total: %d active, %d served since start</p>`, len(sessions), sessionManager.GetCreatedSessionCount())
	if len(sessions) == 0 {
		return
	}

	fmt.Fprint(w, `<table><tr><th>ID</th><th>User</th><th>Host</th><th>Last Access</th><th>Last Operation</th><th>Clients</th><th>Staged Files</th><th>Staged Size</th><th>Packed Directories</th><th>File Handles</th><th>iRODS Conns</th></tr>`)
	for _, session := range sessions {
		session.mutex.RLock()
		handleCount := len(session.poolFileHandles)
		type connEntry struct{ id, clientID, app, desc string }
		connEntries := make([]connEntry, 0, len(session.connections))
		for id, ci := range session.connections {
			connEntries = append(connEntries, connEntry{id, ci.clientID, ci.appName, ci.description})
		}
		lastAccess := session.lastAccessTime
		lastOperation := session.lastOperationTime
		releasing := session.releasing
		session.mutex.RUnlock()

		var irodsConns int
		var stagedFiles int
		var stagedSize int64
		var packedDirectories int
		if session.fsClient != nil {
			irodsConns = session.fsClient.GetOpenConnections()
			if bufferedClient, ok := session.fsClient.(*irodsfs_common_irods.IRODSFSClientBuffered); ok {
				if stagingFS := bufferedClient.GetStagingFS(); stagingFS != nil {
					stagedFiles = len(stagingFS.GetAll())
					stagedSize = stagingFS.GetCurrentDataSize()
				}
				if packedFS := bufferedClient.GetPackedFS(); packedFS != nil {
					packedDirectories = len(packedFS.Statuses())
				}
			}
		}

		sort.Slice(connEntries, func(i, j int) bool { return connEntries[i].id < connEntries[j].id })

		account := session.irodsAccount
		hostInfo := fmt.Sprintf("%s:%d", account.Host, account.Port)
		displayID := session.id
		if len(displayID) > 8 {
			displayID = displayID[:4] + "…" + displayID[len(displayID)-4:]
		}

		var clientsCell string
		if releasing {
			// Past the grace period and still working: the staging upload runs
			// long after the last client is gone, and the session stays listed
			// until it finishes.
			clientsCell = `<span class="badge releasing">&#x1F504; releasing — syncing to iRODS</span>`
		} else if len(connEntries) == 0 {
			clientsCell = `<span class="badge grace">⏳ grace period</span>`
		} else {
			var sb strings.Builder
			fmt.Fprintf(&sb, `<span style="margin-right:4px">%d</span>`, len(connEntries))
			for _, e := range connEntries {
				tooltip := e.app
				if e.desc != "" {
					tooltip = e.app + ": " + e.desc
				}
				fmt.Fprintf(&sb, `<span class="badge" title="%s">%s(%s)</span>`,
					html.EscapeString(tooltip), html.EscapeString(e.id), html.EscapeString(e.app))
			}
			clientsCell = sb.String()
		}

		// clientsCell is markup this function built from already-escaped parts,
		// so it is the one value here that must not be escaped again.
		fmt.Fprintf(w, `<tr class="clickable" onclick="location.href='/monitor/session/%s'"><td title="%s">%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%s</td><td>%d</td><td>%d</td><td>%d</td></tr>`,
			template.JSEscapeString(session.id), html.EscapeString(session.id), html.EscapeString(displayID),
			html.EscapeString(formatIRODSUser(account.ClientUser, account.ClientZone)), html.EscapeString(hostInfo),
			lastAccess.Format("15:04:05"), formatOperationTime(lastOperation, "15:04:05"), clientsCell, stagedFiles, formatBytes(stagedSize), packedDirectories, handleCount, irodsConns)
	}
	fmt.Fprint(w, `</table>`)
	fmt.Fprint(w, `<p class="info">Click a row to see staged files, sync status, and open file handles.</p>`)
}

func (h *MonitoringHandler) renderFailedSessions(w http.ResponseWriter) {
	fmt.Fprint(w, `<h2>Sessions Pending Recovery</h2>`)

	sessions, err := h.poolServer.GetSessionManager().GetFailedSessions()
	if err != nil {
		fmt.Fprint(w, `<p class="dirty">Unable to load persisted recovery information.</p>`)
		return
	}

	fmt.Fprintf(w, `<p>Total: %d</p>`, len(sessions))
	if len(sessions) == 0 {
		fmt.Fprint(w, `<p class="info">No sessions are pending recovery.</p>`)
		return
	}

	fmt.Fprint(w, `<table><tr><th>ID</th><th>Status</th><th>User</th><th>Host</th><th>Last Access</th><th>Clients</th></tr>`)
	for _, session := range sessions {
		displayID := session.ID
		if len(displayID) > 8 {
			displayID = displayID[:4] + "…" + displayID[len(displayID)-4:]
		}
		account := session.IRODSAccount
		userInfo := formatIRODSUser(account.ClientUser, account.ClientZone)
		hostInfo := fmt.Sprintf("%s:%d", account.Host, account.Port)

		var clientsCell strings.Builder
		fmt.Fprintf(&clientsCell, `<span style="margin-right:4px">%d</span>`, len(session.Connections))
		for _, connection := range session.Connections {
			tooltip := connection.Application
			if connection.Description != "" {
				tooltip += ": " + connection.Description
			}
			fmt.Fprintf(&clientsCell, `<span class="badge" title="%s">%s(%s)</span>`,
				html.EscapeString(tooltip), html.EscapeString(connection.ConnectionID), html.EscapeString(connection.Application))
		}

		fmt.Fprintf(w, `<tr class="clickable" onclick="location.href='/monitor/recovery-session/%s'"><td title="%s">%s</td><td><span class="badge">%s</span></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>`,
			template.JSEscapeString(session.ID), html.EscapeString(session.ID), html.EscapeString(displayID), html.EscapeString(string(session.Status)), html.EscapeString(userInfo), html.EscapeString(hostInfo),
			session.LastAccessTime.Format("2006-01-02 15:04:05"), clientsCell.String())
	}
	fmt.Fprint(w, `</table>`)
	fmt.Fprint(w, `<p class="info">These sessions were interrupted, are recovering, or failed during release. Click a row for recovery metadata.</p>`)
}

func (h *MonitoringHandler) serveSessionDetailPage(w http.ResponseWriter, sessionID string) {
	session, err := h.poolServer.GetSessionManager().GetSession(sessionID)
	if err != nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	h.writePageHeader(w, "Session Detail")
	h.renderOneSessionDetail(w, session)
	fmt.Fprint(w, `</body></html>`)
}

func (h *MonitoringHandler) serveFailedSessionDetailPage(w http.ResponseWriter, sessionID string) {
	session, err := h.poolServer.GetSessionManager().GetFailedSession(sessionID)
	if err != nil || session == nil {
		http.Error(w, "recovery session not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	h.writePageHeader(w, "Recovery Session Detail")
	h.renderOneFailedSessionDetail(w, *session)
	fmt.Fprint(w, `</body></html>`)
}

func (h *MonitoringHandler) renderOneFailedSessionDetail(w http.ResponseWriter, session FailedSessionInfo) {
	escape := html.EscapeString
	account := session.IRODSAccount

	fmt.Fprint(w, `<div class="detail-header">`)
	fmt.Fprint(w, `<div><a class="back-link" href="/monitor">&larr; Back</a><h1>Session Pending Recovery</h1></div>`)
	fmt.Fprintf(w, `<div class="detail-actions">`)
	fmt.Fprintf(w, `<button class="action-btn" onclick="recoverSession('%s')">&#x21BA; Recover</button>`,
		template.JSEscapeString(session.ID))
	fmt.Fprintf(w, `<button class="action-btn action-btn-warn" onclick="discardSession('%s')">&#x1F5D1; Discard</button>`,
		template.JSEscapeString(session.ID))
	fmt.Fprint(w, `</div></div>`)
	fmt.Fprintf(w, `<div id="action-result-%s" style="margin-top:10px"></div>`, escape(session.ID))

	fmt.Fprint(w, `<table>`)
	fmt.Fprintf(w, `<tr><th>ID</th><td>%s</td><th>Last Access</th><td>%s</td></tr>`, escape(session.ID), session.LastAccessTime.Format("2006-01-02 15:04:05 MST"))
	fmt.Fprintf(w, `<tr><th>Host</th><td>%s:%d</td><th>User</th><td>%s</td></tr>`, escape(account.Host), account.Port, escape(formatIRODSUser(account.ClientUser, account.ClientZone)))
	fmt.Fprintf(w, `<tr><th>Status</th><td><span class="badge">%s</span></td><th>Account Key</th><td>%s</td></tr>`, escape(string(session.Status)), escape(session.AccountKey))
	fmt.Fprintf(w, `<tr><th>Auth Scheme</th><td>%s</td><th>Proxy User</th><td>%s</td></tr>`, escape(account.AuthenticationScheme), escape(formatIRODSUser(account.ProxyUser, account.ProxyZone)))
	fmt.Fprintf(w, `<tr><th>CS Negotiation</th><td>%t / %s</td><th>Default Resource</th><td>%s</td></tr>`, account.ClientServerNegotiation, escape(account.CSNegotiationPolicy), escape(account.DefaultResource))
	fmt.Fprintf(w, `<tr><th>Default Hash</th><td>%s</td><th>PAM TTL</th><td>%d</td></tr>`, escape(account.DefaultHashScheme), account.PAMTTL)
	fmt.Fprint(w, `</table>`)

	if ssl := account.SSLConfiguration; ssl != nil {
		fmt.Fprint(w, `<h2>SSL Configuration</h2><table>`)
		fmt.Fprintf(w, `<tr><th>CA Certificate File</th><td>%s</td><th>CA Certificate Path</th><td>%s</td></tr>`, escape(ssl.CACertificateFile), escape(ssl.CACertificatePath))
		fmt.Fprintf(w, `<tr><th>Encryption</th><td>%s, key %d, salt %d, rounds %d</td><th>Verify Server</th><td>%s</td></tr>`, escape(ssl.EncryptionAlgorithm), ssl.EncryptionKeySize, ssl.EncryptionSaltSize, ssl.EncryptionNumHashRounds, escape(ssl.VerifyServer))
		fmt.Fprintf(w, `<tr><th>DH Params File</th><td>%s</td><th>Server Name</th><td>%s</td></tr>`, escape(ssl.DHParamsFile), escape(ssl.ServerName))
		fmt.Fprint(w, `</table>`)
	}

	fmt.Fprintf(w, `<h2>Clients (%d)</h2>`, len(session.Connections))
	if len(session.Connections) == 0 {
		fmt.Fprint(w, `<p>No recorded clients.</p>`)
	} else {
		fmt.Fprint(w, `<table class="clients-table"><tr><th>Client</th><th>Application</th><th>Description</th></tr>`)
		for _, connection := range session.Connections {
			fmt.Fprintf(w, `<tr><td title="connection %s">%s</td><td>%s</td><td>%s</td></tr>`,
				escape(connection.ConnectionID), escape(clientLabel(connection.ClientID, connection.ConnectionID)),
				escape(connection.Application), escape(connection.Description))
		}
		fmt.Fprint(w, `</table>`)
	}
	fmt.Fprint(w, `<p class="info">Credentials, tickets, and PAM tokens are not stored.</p>`)

	h.writeRecoveryScripts(w)
}

func (h *MonitoringHandler) renderOneSessionDetail(w http.ResponseWriter, session *PoolSession) {
	session.mutex.RLock()
	type connEntry struct{ id, clientID, app, desc string }
	connEntries := make([]connEntry, 0, len(session.connections))
	for id, ci := range session.connections {
		connEntries = append(connEntries, connEntry{id, ci.clientID, ci.appName, ci.description})
	}
	type handleEntry struct{ path, mode string }
	handleEntries := make([]handleEntry, 0, len(session.poolFileHandles))
	for _, h2 := range session.poolFileHandles {
		handleEntries = append(handleEntries, handleEntry{h2.GetEntryPath(), string(h2.GetOpenMode())})
	}
	created := session.createdTime
	lastAccess := session.lastAccessTime
	lastOperation := session.lastOperationTime
	releasing := session.releasing
	session.mutex.RUnlock()

	sort.Slice(connEntries, func(i, j int) bool { return connEntries[i].id < connEntries[j].id })
	sort.Slice(handleEntries, func(i, j int) bool { return handleEntries[i].path < handleEntries[j].path })

	var stagingEntries []stagedFileEntry
	var stagedSize int64
	if session.fsClient != nil {
		if bufferedClient, ok := session.fsClient.(*irodsfs_common_irods.IRODSFSClientBuffered); ok {
			if stagingFS := bufferedClient.GetStagingFS(); stagingFS != nil {
				stagedSize = stagingFS.GetCurrentDataSize()
				for _, meta := range stagingFS.GetAll() {
					stagingEntries = append(stagingEntries, stagedFileEntry{
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

	var packedEntries []irodsfs_common_packedfs.Status
	if session.fsClient != nil {
		if bufferedClient, ok := session.fsClient.(*irodsfs_common_irods.IRODSFSClientBuffered); ok {
			packedEntries = bufferedClient.GetPackedFS().Statuses()
		}
	}
	sort.Slice(packedEntries, func(i, j int) bool { return packedEntries[i].Root < packedEntries[j].Root })

	account := session.irodsAccount
	userInfo := formatIRODSUser(account.ClientUser, account.ClientZone)
	hostInfo := fmt.Sprintf("%s:%d", account.Host, account.Port)

	// Header with back link and action buttons at top-right
	fmt.Fprint(w, `<div class="detail-header">`)
	fmt.Fprint(w, `<div><a class="back-link" href="/monitor">&larr; Back</a><h1>Session Detail</h1></div>`)
	fmt.Fprintf(w, `<div class="detail-actions">`)
	fmt.Fprintf(w, `<button class="action-btn sync-staging-btn" onclick="syncSessionStaging('%s')">&#x21BA; Sync Staging</button>`,
		template.JSEscapeString(session.id))
	fmt.Fprint(w, `</div></div>`)
	fmt.Fprint(w, `<div class="session-action-result" style="margin-top:10px"></div>`)

	fmt.Fprint(w, `<table>`)
	fmt.Fprintf(w, `<tr><th>ID</th><td>%s</td><th>Created</th><td>%s</td></tr>`, html.EscapeString(session.id), created.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, `<tr><th>Host</th><td>%s</td><th>User</th><td>%s</td></tr>`, html.EscapeString(hostInfo), html.EscapeString(userInfo))
	fmt.Fprintf(w, `<tr><th>Last Access</th><td>%s</td><th>Last Operation</th><td>%s</td></tr>`, lastAccess.Format("2006-01-02 15:04:05"), formatOperationTime(lastOperation, "2006-01-02 15:04:05"))
	fmt.Fprint(w, `</table>`)

	// Clients
	fmt.Fprintf(w, `<h2>Clients (%d)</h2>`, len(connEntries))
	if releasing {
		fmt.Fprint(w, `<p><span class="badge releasing">&#x1F504; releasing — syncing staged data to iRODS</span></p>`)
	} else if len(connEntries) == 0 {
		fmt.Fprint(w, `<p><span class="badge grace">⏳ grace period — no connected clients</span></p>`)
	} else {
		fmt.Fprint(w, `<table class="clients-table"><tr><th>Client</th><th>Application</th><th>Description</th></tr>`)
		for _, e := range connEntries {
			tooltip := e.app
			if e.desc != "" {
				tooltip = e.app + ": " + e.desc
			}
			fmt.Fprintf(w, `<tr><td title="connection %s">%s</td><td title="%s">%s</td><td>%s</td></tr>`,
				html.EscapeString(e.id), html.EscapeString(clientLabel(e.clientID, e.id)),
				html.EscapeString(tooltip), html.EscapeString(e.app), html.EscapeString(e.desc))
		}
		fmt.Fprint(w, `</table>`)
	}

	// Staged Files (scrollable)
	fmt.Fprintf(w, `<h2>Staged Files (%d, %s)</h2>`, len(stagingEntries), formatBytes(stagedSize))
	if len(stagingEntries) > 0 {
		fmt.Fprint(w, `<div class="scroll-section">`)
		renderStagedFilesTable(w, stagingEntries)
		fmt.Fprint(w, `</div>`)
	} else {
		fmt.Fprint(w, `<p>No staged files.</p>`)
	}

	// Packed Directories (scrollable)
	fmt.Fprintf(w, `<h2>Packed Directories (%d)</h2>`, len(packedEntries))
	if len(packedEntries) > 0 {
		fmt.Fprint(w, `<div class="scroll-section">`)
		renderPackedDirectoriesTable(w, packedEntries)
		fmt.Fprint(w, `</div>`)
	} else {
		fmt.Fprint(w, `<p>No packed directories.</p>`)
	}

	// Open File Handles (scrollable)
	fmt.Fprintf(w, `<h2>Open File Handles (%d)</h2>`, len(handleEntries))
	if len(handleEntries) > 0 {
		fmt.Fprint(w, `<div class="scroll-section">`)
		fmt.Fprint(w, `<table class="handles-table"><tr><th>Path</th><th>Mode</th></tr>`)
		for _, e := range handleEntries {
			fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td></tr>`,
				html.EscapeString(e.path), html.EscapeString(e.mode))
		}
		fmt.Fprint(w, `</table></div>`)
	} else {
		fmt.Fprint(w, `<p>No open file handles.</p>`)
	}

	h.writeSyncScript(w)
}

func (h *MonitoringHandler) writeSyncScript(w http.ResponseWriter) {
	fmt.Fprint(w, `<script>
function syncSessionStaging(id) {
  var el = document.querySelector('.session-action-result');
  var button = document.querySelector('.sync-staging-btn');
  if (button) button.disabled = true;
  if (el) el.innerHTML = '<span class="action-result-ok">Syncing staged data…</span>';
  fetch('/api/sessions/' + encodeURIComponent(id) + '/staging/sync', {method:'POST'})
    .then(function(r){ return r.json().then(function(data){ return {ok:r.ok, data:data}; }); })
    .then(function(result){
      if (!el) return;
      if (result.ok && result.data.success) {
        el.innerHTML = '<span class="action-result-ok">&#x2713; Staging sync succeeded. Refresh the page to see updates.</span>';
      } else {
        el.innerHTML = '<span class="action-result-err">&#x2717; ' + (result.data.error || 'unknown error') + '</span>';
        if (button) button.disabled = false;
      }
    })
    .catch(function(e){
      if (el) el.innerHTML = '<span class="action-result-err">&#x2717; ' + e + '</span>';
      if (button) button.disabled = false;
    });
}
</script>`)
}

func (h *MonitoringHandler) writeRecoveryScripts(w http.ResponseWriter) {
	fmt.Fprint(w, `<script>
function recoverSession(id) {
  var el = document.getElementById('action-result-' + id);
  if (el) el.innerHTML = '<span class="action-result-ok">Recovering…</span>';
  fetch('/api/recovery-sessions/' + encodeURIComponent(id) + '/recover', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data){
      if (!el) return;
      if (data.success) {
        el.innerHTML = '<span class="action-result-ok">&#x2713; Recovery succeeded. Refresh the page to see updates.</span>';
      } else if (confirm('Recovery failed: ' + (data.error || 'unknown error') + '\n\nAttempt force repair? This removes broken dependencies and retries blocked operations.')) {
        forceRepairSession(id);
      } else {
        el.innerHTML = '<span class="action-result-err">&#x2717; ' + (data.error || 'unknown error') + '</span>';
      }
    })
    .catch(function(e){ if (el) el.innerHTML = '<span class="action-result-err">&#x2717; ' + e + '</span>'; });
}
function forceRepairSession(id) {
  var el = document.getElementById('action-result-' + id);
  if (el) el.innerHTML = '<span class="action-result-ok">Repairing…</span>';
  fetch('/api/recovery-sessions/' + encodeURIComponent(id) + '/force-repair', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data){
      if (!el) return;
      if (data.success) {
        var msg = '&#x2713; Force repair succeeded';
        if (data.repaired > 0) msg += ' (' + data.repaired + ' issues fixed)';
        msg += '. Refresh the page to see updates.';
        el.innerHTML = '<span class="action-result-ok">' + msg + '</span>';
      } else {
        el.innerHTML = '<span class="action-result-err">&#x2717; ' + (data.error || 'unknown error') + '</span>';
      }
    })
    .catch(function(e){ if (el) el.innerHTML = '<span class="action-result-err">&#x2717; ' + e + '</span>'; });
}
function discardSession(id) {
  if (!confirm('Discard local staging data for this session?\n\nOnly do this if the data has already been pushed to iRODS manually.')) return;
  var el = document.getElementById('action-result-' + id);
  if (el) el.innerHTML = '<span class="action-result-ok">Discarding…</span>';
  fetch('/api/recovery-sessions/' + encodeURIComponent(id) + '/discard', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data){
      if (!el) return;
      if (data.success) {
        el.innerHTML = '<span class="action-result-ok">&#x2713; Staging discarded. Refresh the page to see updates.</span>';
      } else {
        el.innerHTML = '<span class="action-result-err">&#x2717; ' + (data.error || 'unknown error') + '</span>';
      }
    })
    .catch(function(e){ if (el) el.innerHTML = '<span class="action-result-err">&#x2717; ' + e + '</span>'; });
}
</script>`)
}

func (h *MonitoringHandler) renderMetrics(w http.ResponseWriter) {
	fmt.Fprint(w, `<h2>I/O Metrics (cumulative)</h2>`)

	total := h.poolServer.GetTotalMetrics()
	m := &total

	fmt.Fprint(w, `<table>`)
	fmt.Fprintf(w, `<tr><th>Bytes Sent</th><td>%s</td><th>Bytes Received</th><td>%s</td></tr>`, formatBytes(int64(m.BytesSent)), formatBytes(int64(m.BytesReceived)))
	fmt.Fprintf(w, `<tr><th>Cache Hit / Miss</th><td colspan="3">%s</td></tr>`, renderHitMissBar(m.CacheHit, m.CacheMiss))
	fmt.Fprintf(w, `<tr><th>Request Failures</th><td>%d</td><th>Connection Failures</th><td>%d</td></tr>`, m.RequestFailures, m.ConnectionFailures)
	fmt.Fprint(w, `</table>`)
}

// errorMessageDisplayLength caps the message shown in the errors table; the
// full message and the stack trace are in the row's detail.
const errorMessageDisplayLength = 300

// renderErrors lists the errors iRODS operations returned to clients, one row
// per distinct error.
func renderErrors(w io.Writer, groups []ErrorGroup, total uint64) {
	fmt.Fprint(w, `<h2>Errors</h2>`)
	fmt.Fprintf(w, `<p>Total: %d since start (%d distinct errors shown)</p>`, total, len(groups))
	if len(groups) == 0 {
		fmt.Fprint(w, `<p class="info">No errors have been recorded.</p>`)
		return
	}

	fmt.Fprint(w, `<table class="errors-table"><tr><th>Last Seen</th><th>Count</th><th>Operation</th><th>Users</th><th>Latest Path</th><th>Error</th></tr>`)
	for _, group := range groups {
		message := group.Message
		if len(message) > errorMessageDisplayLength {
			message = strings.ToValidUTF8(message[:errorMessageDisplayLength], "") + "…"
		}
		fmt.Fprintf(w, `<tr class="clickable" onclick="showDetail('error-%d')"><td>%s</td><td>%d</td><td>%s</td><td>%s</td><td>%s</td><td class="dirty">%s</td></tr>`,
			group.ID, group.LastSeen.Format("2006-01-02 15:04:05"), group.Count, html.EscapeString(group.Operation),
			html.EscapeString(formatErrorGroupUsers(group)), html.EscapeString(group.Path), html.EscapeString(message))
	}
	fmt.Fprint(w, `</table>`)
	fmt.Fprint(w, `<p class="info">Errors iRODS operations returned to clients, staged uploads the background sync gave up on, and failed session releases, grouped by operation and message. Errors recovered by a retry are not listed. Click a row for the stack trace of the latest occurrence.</p>`)
}

func formatErrorGroupUsers(group ErrorGroup) string {
	users := strings.Join(group.Users, ", ")
	if group.MoreUsers {
		users += ", …"
	}
	return users
}

func renderErrorDetails(w io.Writer, groups []ErrorGroup) {
	escape := html.EscapeString
	for _, group := range groups {
		fmt.Fprintf(w, `<div id="detail-error-%d" style="display:none">`, group.ID)
		fmt.Fprint(w, `<h3>Error</h3><table>`)
		fmt.Fprintf(w, `<tr><th>Operation</th><td>%s</td></tr>`, escape(group.Operation))
		fmt.Fprintf(w, `<tr><th>Message</th><td>%s</td></tr>`, escape(group.Message))
		fmt.Fprintf(w, `<tr><th>Count</th><td>%d</td></tr>`, group.Count)
		fmt.Fprintf(w, `<tr><th>First Seen</th><td>%s</td></tr>`, group.FirstSeen.Format("2006-01-02 15:04:05.000 MST"))
		fmt.Fprintf(w, `<tr><th>Last Seen</th><td>%s</td></tr>`, group.LastSeen.Format("2006-01-02 15:04:05.000 MST"))
		fmt.Fprintf(w, `<tr><th>Users</th><td>%s</td></tr>`, escape(formatErrorGroupUsers(group)))
		fmt.Fprint(w, `</table>`)
		fmt.Fprint(w, `<h3>Latest Occurrence</h3><table>`)
		fmt.Fprintf(w, `<tr><th>Session</th><td>%s</td></tr>`, escape(group.SessionID))
		fmt.Fprintf(w, `<tr><th>User</th><td>%s</td></tr>`, escape(group.User))
		fmt.Fprintf(w, `<tr><th>Path</th><td>%s</td></tr>`, escape(group.Path))
		fmt.Fprint(w, `</table>`)
		fmt.Fprintf(w, `<h3>Stack Trace</h3><pre class="stack-trace">%s</pre>`, escape(group.Detail))
		fmt.Fprint(w, `</div>`)
	}
}

// renderUsageBar renders a bar of used out of total, labeled with the amounts
// on the used part and with freeLabel on the free part
func renderUsageBar(used int64, total int64, freeLabel string) string {
	if used < 0 {
		used = 0
	}
	var pct float64
	if total > 0 {
		pct = float64(used) / float64(total) * 100
	}
	return renderLabeledBar(pct, fmt.Sprintf("%s / %s used (%.1f%%)", formatBytes(used), formatBytes(total), pct), freeLabel)
}

// renderLabeledBar renders a usage bar with a label on the used part, at the
// left, and a label on the free part, at the right
func renderLabeledBar(pct float64, usedLabel string, freeLabel string) string {
	if pct > 100 {
		pct = 100
	}
	freePct := 100 - pct
	return fmt.Sprintf(`<div class="bar"><div class="bar-used" style="width:%.1f%%"></div><div class="bar-free" style="width:%.1f%%"></div><span class="bar-text">%s</span><span class="bar-text bar-text-right">%s</span></div>`, pct, freePct, usedLabel, freeLabel)
}

// renderHitMissBar renders a bar of cache hits against misses, labeled with
// the counts and their percentages. The bar is empty while there has been
// neither.
func renderHitMissBar(hit uint64, miss uint64) string {
	var hitPct, missPct float64
	if hit+miss > 0 {
		hitPct = float64(hit) / float64(hit+miss) * 100
		missPct = 100 - hitPct
	}
	return fmt.Sprintf(`<div class="hm-bar"><div class="hm-hit" style="width:%.1f%%"></div><div class="hm-miss" style="width:%.1f%%"></div><span class="hm-text">%d hit (%.1f%%)</span><span class="hm-text bar-text-right">%d miss (%.1f%%)</span></div>`, hitPct, missPct, hit, hitPct, miss, missPct)
}

// stagedFileEntry is one row of the staged files table.
type stagedFileEntry struct {
	path      string
	oldPath   string
	action    string
	fileState string
	modified  time.Time
	failCount int
}

// renderStagedFiles lists the files a session has staged but not yet synced.
//
// Every string here comes from iRODS or from a client, so all of it is escaped:
// a path is whatever a user named a file, and this page is served to operators.
// clientLabel names a client row. The connection id is the server's own and
// means nothing to whoever runs the client, so the row carries it as a tooltip
// and shows it outright only for a client that sent no id of its own - an older
// one, which nothing else identifies. The server log keys on it either way.
func clientLabel(clientID string, connectionID string) string {
	if clientID != "" {
		return clientID
	}

	return connectionID
}

func renderStagedFilesTable(w io.Writer, entries []stagedFileEntry) {
	fmt.Fprint(w, `<table class="staged-files-table"><tr><th>Path</th><th>Action</th><th>Sync Status</th><th>Modified</th><th>Failures</th></tr>`)
	for _, entry := range entries {
		stateClass := "cached"
		if entry.fileState == "DIRTY" {
			stateClass = "dirty"
		}

		pathCell := html.EscapeString(entry.path)
		if entry.oldPath != "" {
			pathCell = fmt.Sprintf("%s<br><span style='color:#888;font-size:11px'>← %s</span>",
				pathCell, html.EscapeString(entry.oldPath))
		}

		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td class="%s">%s</td><td>%s</td><td>%d</td></tr>`,
			pathCell, html.EscapeString(entry.action), stateClass, html.EscapeString(entry.fileState),
			entry.modified.Format("15:04:05"), entry.failCount)
	}
	fmt.Fprint(w, `</table>`)
}

// renderPackedDirectories lists the packed directories of a session.
//
// It reports the directory and the data object it becomes rather than the files
// inside it. Those files are held as a local tree and sent as one archive, so
// they carry no staging metadata, listing them would be tens of thousands of
// rows, and what an operator needs to know is whether the archive has reached
// iRODS.
func renderPackedDirectoriesTable(w io.Writer, entries []irodsfs_common_packedfs.Status) {
	fmt.Fprint(w, `<table class="packed-dirs-table"><tr><th>Directory</th><th>Archive in iRODS</th><th>State</th><th>Pending</th><th>Size</th><th>Last Packed</th></tr>`)
	for _, entry := range entries {
		pendingClass, pendingText := "cached", "synced"
		if entry.Dirty {
			pendingClass, pendingText = "dirty", "pending"
		}

		lastPacked := "never"
		if !entry.LastPackedAt.IsZero() {
			lastPacked = entry.LastPackedAt.Format("15:04:05")
		}

		directoryCell := html.EscapeString(entry.Root)
		if entry.Error != "" {
			directoryCell = fmt.Sprintf("%s<br><span style='color:#f44;font-size:11px'>%s</span>",
				directoryCell, html.EscapeString(entry.Error))
		}

		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td>%s</td><td class="%s">%s</td><td>%s</td><td>%s</td></tr>`,
			directoryCell, html.EscapeString(entry.ArchivePath), html.EscapeString(entry.State),
			pendingClass, pendingText, formatBytes(entry.SizeBytes), lastPacked)
	}
	fmt.Fprint(w, `</table>`)
}

// formatIRODSUser renders an iRODS user the way iRODS itself writes it, user#zone.
func formatIRODSUser(user string, zone string) string {
	return user + "#" + zone
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

func formatUptime(d time.Duration) string {
	totalSeconds := int(d.Seconds())
	days := totalSeconds / 86400
	remaining := totalSeconds % 86400
	hours := remaining / 3600
	minutes := (remaining % 3600) / 60
	seconds := remaining % 60
	if days > 0 {
		return fmt.Sprintf("%d Days %02d:%02d:%02d", days, hours, minutes, seconds)
	}
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
}

// formatOperationTime formats the time of a session's last file system call,
// which is zero when there has been none
func formatOperationTime(t time.Time, layout string) string {
	if t.IsZero() {
		return "never"
	}
	return t.Format(layout)
}

func getSystemMemoryInfo() (total, available uint64) {
	var info syscall.Sysinfo_t
	err := syscall.Sysinfo(&info)
	if err != nil {
		return 0, 0
	}
	total = uint64(info.Totalram) * uint64(info.Unit)

	// MemAvailable accounts for reclaimable page cache and slab, unlike Freeram
	if memAvailable, ok := getMemAvailable(); ok {
		available = memAvailable
	} else {
		available = uint64(info.Freeram+info.Bufferram) * uint64(info.Unit)
	}
	return
}

// isMemoryInsufficient reports whether the system cannot hold the rest of the cache.
// Memory already used by the cache is excluded from sysAvail, so only the
// remaining growth (cacheMax - cacheUsed) needs to fit.
func isMemoryInsufficient(sysAvail uint64, cacheUsed int64, cacheMax int64) bool {
	if sysAvail == 0 || cacheMax <= cacheUsed {
		return false
	}
	return sysAvail < uint64(cacheMax-cacheUsed)
}

// getMemAvailable reads MemAvailable from /proc/meminfo (kernel 3.14+)
func getMemAvailable() (uint64, bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != "MemAvailable:" {
			continue
		}

		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}

		// values in /proc/meminfo are in kB
		return value * 1024, true
	}
	return 0, false
}

// isDiskSpaceInsufficient reports whether the disk cannot hold the rest of the staging quota.
// Data already staged is excluded from diskFree, so only the
// remaining growth (stagingMax - stagingUsed) needs to fit.
func isDiskSpaceInsufficient(diskFree uint64, stagingUsed int64, stagingMax int64) bool {
	if diskFree == 0 || stagingMax <= stagingUsed {
		return false
	}
	return diskFree < uint64(stagingMax-stagingUsed)
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
