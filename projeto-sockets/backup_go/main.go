package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type DeviceInfo struct {
	DeviceID       string `json:"device_id"`
	Type           int    `json:"type"`
	Status         int    `json:"status"`
	IPAddress      string `json:"ip_address"`
	ControlPort    int    `json:"control_port"`
	IsControllable int    `json:"is_controllable"`
	LastSeen       int64  `json:"last_seen"`
	AggregatorID   string `json:"aggregator_id"`
	CoordX         int    `json:"coord_x"`
	CoordY         int    `json:"coord_y"`
}

type MetadataInfo struct {
	TotalMetricsProcessed int `json:"total_metrics_processed"`
	FirstMetricTimestamp  int64 `json:"first_metric_timestamp"`
	LastMetricTimestamp   int64 `json:"last_metric_timestamp"`
}

type SystemEfficacy struct {
	TotalDevices    int     `json:"total_devices"`
	OnlineDevices   int     `json:"online_devices"`
	OfflineDevices  int     `json:"offline_devices"`
	UptimePercent   float64 `json:"uptime_percent"`
	AggregatorStats map[string]int `json:"aggregator_stats"`
}

type BackupPayload struct {
	Timestamp      string         `json:"timestamp"`
	Devices        []DeviceInfo   `json:"devices"`
	Metadata       MetadataInfo   `json:"metadata"`
	Efficacy       SystemEfficacy `json:"efficacy"`
}

var lastBackupTime string

func getDBPath() string {
	return os.Getenv("DB_PATH")
}

func getBackupDir() string {
	dir := os.Getenv("BACKUP_DIR")
	if dir == "" {
		return "/backups"
	}
	return dir
}

func performBackup() error {
	log.Println("Iniciando processo de backup...")
	dbPath := getDBPath()
	if dbPath == "" {
		dbPath = "smartcity.db"
	}

	// Abrir em modo somente leitura
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro", dbPath))
	if err != nil {
		return fmt.Errorf("erro ao conectar no db: %v", err)
	}
	defer db.Close()

	payload := BackupPayload{
		Timestamp: time.Now().Format(time.RFC3339),
		Efficacy: SystemEfficacy{
			AggregatorStats: make(map[string]int),
		},
	}

	// 1. Extrair Devices
	rows, err := db.Query("SELECT device_id, type, status, ip_address, control_port, is_controllable, last_seen, aggregator_id, coord_x, coord_y FROM devices")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var d DeviceInfo
			var agg sql.NullString
			var cx, cy sql.NullInt64
			if err := rows.Scan(&d.DeviceID, &d.Type, &d.Status, &d.IPAddress, &d.ControlPort, &d.IsControllable, &d.LastSeen, &agg, &cx, &cy); err == nil {
				if agg.Valid {
					d.AggregatorID = agg.String
				} else {
					d.AggregatorID = "N/A"
				}
				if cx.Valid {
					d.CoordX = int(cx.Int64)
				}
				if cy.Valid {
					d.CoordY = int(cy.Int64)
				}
				payload.Devices = append(payload.Devices, d)
			}
		}
	} else {
		log.Printf("Erro lendo devices: %v", err)
	}

	// Calcular Efficacy
	payload.Efficacy.TotalDevices = len(payload.Devices)
	for _, d := range payload.Devices {
		if d.Status == 1 {
			payload.Efficacy.OnlineDevices++
		} else {
			payload.Efficacy.OfflineDevices++
		}
		payload.Efficacy.AggregatorStats[d.AggregatorID]++
	}
	if payload.Efficacy.TotalDevices > 0 {
		payload.Efficacy.UptimePercent = (float64(payload.Efficacy.OnlineDevices) / float64(payload.Efficacy.TotalDevices)) * 100.0
	}

	// 2. Extrair Metadata (Tabela metrics)
	row := db.QueryRow("SELECT count(*), min(timestamp), max(timestamp) FROM metrics")
	var count int
	var minTs, maxTs sql.NullInt64
	if err := row.Scan(&count, &minTs, &maxTs); err == nil {
		payload.Metadata.TotalMetricsProcessed = count
		if minTs.Valid {
			payload.Metadata.FirstMetricTimestamp = minTs.Int64
		}
		if maxTs.Valid {
			payload.Metadata.LastMetricTimestamp = maxTs.Int64
		}
	}

	// 3. Salvar o JSON
	backupDir := getBackupDir()
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("erro ao criar dir de backup: %v", err)
	}

	filename := fmt.Sprintf("backup_%s.json", time.Now().Format("20060102_150405"))
	filepath := filepath.Join(backupDir, filename)

	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("erro no json: %v", err)
	}

	if err := ioutil.WriteFile(filepath, data, 0644); err != nil {
		return fmt.Errorf("erro escrevendo arquivo: %v", err)
	}

	log.Printf("Backup salvo com sucesso: %s", filepath)
	lastBackupTime = payload.Timestamp

	// Rotacionar backups (manter max 3)
	rotateBackups(backupDir, 3)

	return nil
}

func rotateBackups(dir string, max int) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Printf("Erro lendo diretório para rotação: %v", err)
		return
	}

	var backups []os.FileInfo
	for _, f := range files {
		if !f.IsDir() && len(f.Name()) > 7 && f.Name()[:7] == "backup_" {
			backups = append(backups, f)
		}
	}

	if len(backups) <= max {
		return
	}

	// Sort oldest first
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].ModTime().Before(backups[j].ModTime())
	})

	toDelete := len(backups) - max
	for i := 0; i < toDelete; i++ {
		path := filepath.Join(dir, backups[i].Name())
		os.Remove(path)
		log.Printf("Backup antigo removido: %s", path)
	}
}

func main() {
	log.Println("Iniciando Serviço de Backup Go...")

	// Fazer um backup inicial
	performBackup()

	// Iniciar rotina de 6 minutos
	go func() {
		ticker := time.NewTicker(6 * time.Minute)
		for {
			<-ticker.C
			performBackup()
		}
	}()

	// Endpoints HTTP
	http.HandleFunc("/backup/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"last_backup_time": "%s"}`, lastBackupTime)
	})

	http.HandleFunc("/backup/trigger", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Metodo nao permitido", http.StatusMethodNotAllowed)
			return
		}
		err := performBackup()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status": "sucesso", "last_backup_time": "%s"}`, lastBackupTime)
	})

	log.Println("Servidor HTTP rodando na porta 8080...")
	log.Fatal(http.ListenAndServe("0.0.0.0:8080", nil))
}
