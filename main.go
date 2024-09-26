package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	BackupUser           string
	Hostname             string
	DBPort               string
	UserDB               string
	PGPASSWORD           string
	Database             string
	BackupDir            string
	SchemaOnlyList       string
	EnableCustomBackups  string
	EnablePlainBackups   string
	EnableGlobalsBackups string
	DayOfWeekToKeep      int
	DaysToKeep           int
	WeeksToKeep          int
	Format               string
}

var config *Config
var finalBackupDir string
var backupDate string

func loadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &Config{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		switch key {
		case "BACKUP_USER":
			config.BackupUser = value
		case "HOSTNAME":
			config.Hostname = value
		case "DBPORT":
			config.DBPort = value
		case "USERDB":
			config.UserDB = value
		case "PGPASSWORD":
			config.PGPASSWORD = value
		case "DATABASE":
			config.Database = value
		case "BACKUP_DIR":
			config.BackupDir = value
		case "SCHEMA_ONLY_LIST":
			config.SchemaOnlyList = value
		case "ENABLE_CUSTOM_BACKUPS":
			config.EnableCustomBackups = value
		case "ENABLE_PLAIN_BACKUPS":
			config.EnablePlainBackups = value
		case "ENABLE_GLOBALS_BACKUPS":
			config.EnableGlobalsBackups = value
		case "DAY_OF_WEEK_TO_KEEP":
			config.DayOfWeekToKeep, _ = strconv.Atoi(value)
		case "DAYS_TO_KEEP":
			config.DaysToKeep, _ = strconv.Atoi(value)
		case "WEEKS_TO_KEEP":
			config.WeeksToKeep, _ = strconv.Atoi(value)
		case "FORMAT":
			config.Format = value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return config, nil
}

func main() {
	carregaConfig()
	limpaBackups()
	execBackup()
	compactaBackup()
}

func carregaConfig() {
	execPath, err := os.Executable()
	if err != nil {
		fmt.Println("Error getting executable path:", err)
		return
	}
	configPath := filepath.Join(filepath.Dir(execPath), "pgbackup.cfg")
	config, err = loadConfig(configPath)
	if err != nil {
		fmt.Println("Error loading config:", err)
		return
	}
}

func limpaBackups() {
	backupDate = time.Now().Format("2006-01-02") // Define a data de backup aqui
	dayOfWeek := int(time.Now().Weekday())

	var suffix string

	if dayOfWeek == config.DayOfWeekToKeep {
		suffix = "weekly"
		finalBackupDir = filepath.Join(config.BackupDir, backupDate+"-"+suffix)
		cleanOldBackups(config.BackupDir, suffix, config.WeeksToKeep*7)
	} else {
		suffix = "daily"
		finalBackupDir = filepath.Join(config.BackupDir, backupDate+"-"+suffix)
		cleanOldBackups(config.BackupDir, suffix, config.DaysToKeep)
	}

	if _, err := os.Stat(finalBackupDir); os.IsNotExist(err) {
		os.MkdirAll(finalBackupDir, 0755)
	}
}

func cleanOldBackups(backupDir, suffix string, daysToKeep int) {
	cutoff := time.Now().AddDate(0, 0, -daysToKeep)
	files, _ := filepath.Glob(filepath.Join(backupDir, "*-"+suffix))
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			os.RemoveAll(file)
		}
	}
}

func execBackup() {
	var backupFile string
	// Verifica o formato e ajusta o nome do arquivo de acordo
	if config.Format == "c" || config.Format == "d" {
		backupFile = filepath.Join(finalBackupDir, config.Database+".dump")
	} else if config.Format == "t" {
		backupFile = filepath.Join(finalBackupDir, config.Database+".tar")
	} else {
		backupFile = filepath.Join(finalBackupDir, config.Database+".sql")
	}

	pgDumpArgs := []string{
		"--host=" + config.Hostname,
		"--port=" + config.DBPort,
		"--username=" + config.UserDB,
		"--format=" + config.Format,
		"-f", backupFile,
		config.Database,
	}

	// Exibir o comando que será executado
	fmt.Println("Executando o comando:", "pg_dump", strings.Join(pgDumpArgs, " "))

	// Ajustar os argumentos para o cmd
	cmdArgs := append([]string{"/C", "pg_dump"}, pgDumpArgs...)
	cmd := exec.Command("cmd", cmdArgs...)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+config.PGPASSWORD)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Println("pg_dump error:", err)
		fmt.Println(string(output))
		return
	}

	fmt.Println("Backup executado com sucesso:", backupFile)

	// Chama compactação adicional se o formato for 't'
	if config.Format == "t" {
		compactaTarBackup(backupFile)
	}
}
func compactaTarBackup(backupFile string) {
	// Nome do arquivo compactado
	compressedFile := backupFile + ".7z"

	fmt.Println("Compactando backup...")

	// Compactar usando 7z
	zipCmd := exec.Command("7z", "a", "-t7z", compressedFile, backupFile)
	if output, err := zipCmd.CombinedOutput(); err != nil {
		fmt.Println("Erro ao compactar com 7z:", err)
		fmt.Println(string(output))
		return
	}

	fmt.Println("Arquivo TAR compactado com sucesso:", compressedFile)

	// Remove o arquivo .tar original após a compactação
	if err := os.Remove(backupFile); err != nil {
		fmt.Println("Erro ao remover arquivo TAR original:", err)
	}
}

func compactaBackup() {
	// Verifica se o formato é 'p'. Se não for, sai da função.
	if config.Format != "p" {
		fmt.Println("Compactação ignorada. O formato do backup não é 'plain' (p).")
		return
	}

	fmt.Println("Compactando backup...")

	// Define a data do backup
	backupDate = time.Now().Format("2006-01-02")
	// Define o caminho do arquivo de backup e o arquivo zip
	backupFile := filepath.Join(finalBackupDir, config.Database+".sql")
	zipFile := filepath.Join(finalBackupDir, config.Database+"_"+backupDate+".7z")

	// Comando para compactar o arquivo de backup
	zipCmd := exec.Command("7z", "a", "-t7z", zipFile, backupFile)
	if output, err := zipCmd.CombinedOutput(); err != nil {
		fmt.Println("Erro ao compactar com 7z:", err)
		fmt.Println(string(output))
		return
	}

	// Remove o arquivo de backup original após compactação
	os.Remove(backupFile)
	fmt.Println("Backup compactado com sucesso:", zipFile)
}
