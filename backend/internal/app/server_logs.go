package app

import (
	"net/http"
	"os"
	"path/filepath"
	"sort"
)

type ServerLogFile struct {
	Name string `json:"name"`
	Time string `json:"time"`
	Size int64  `json:"size"`
}

func (a *App) handleServerLogs(w http.ResponseWriter, r *http.Request) error {
	if err := requireMethod(r, http.MethodGet); err != nil {
		return err
	}
	dir := "/opt/cli-proxy-api/logs/"
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, http.StatusOK, []ServerLogFile{})
			return nil
		}
		return appError("read_logs_failed", http.StatusInternalServerError, "无法读取日志目录")
	}

	var logs []ServerLogFile
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		logs = append(logs, ServerLogFile{
			Name: entry.Name(),
			Time: apiDateTime(info.ModTime()),
			Size: info.Size(),
		})
	}
	sort.Slice(logs, func(i, j int) bool {
		return logs[i].Time > logs[j].Time
	})

	writeJSON(w, http.StatusOK, logs)
	return nil
}

func (a *App) handleServerLogDownload(w http.ResponseWriter, r *http.Request) error {
	if err := requireMethod(r, http.MethodGet); err != nil {
		return err
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		return validationError("文件名不能为空")
	}
	if filepath.Base(name) != name || name == "." || name == ".." {
		return validationError("非法的文件名")
	}

	filePath := filepath.Join("/opt/cli-proxy-api/logs/", name)
	http.ServeFile(w, r, filePath)
	return nil
}
