package logger

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelSuccess
)

var (
	colors = map[LogLevel]string{
		LevelDebug:   "\033[36m", // Cyan
		LevelInfo:    "\033[34m", // Blue
		LevelWarn:    "\033[33m", // Yellow
		LevelError:   "\033[31m", // Red
		LevelSuccess: "\033[32m", // Green
	}
	reset = "\033[0m"
	icons = map[LogLevel]string{
		LevelDebug:   "🔍",
		LevelInfo:    "ℹ️",
		LevelWarn:    "⚠️",
		LevelError:   "❌",
		LevelSuccess: "✅",
	}
	labels = map[LogLevel]string{
		LevelDebug:   "DEBUG",
		LevelInfo:    "INFO ",
		LevelWarn:    "WARN ",
		LevelError:   "ERROR",
		LevelSuccess: "SUCCESS",
	}
	useColors = true
)

func init() {
	// Disable colors if output is not a terminal
	if fileInfo, _ := os.Stdout.Stat(); (fileInfo.Mode() & os.ModeCharDevice) == 0 {
		useColors = false
	}
}

// Log prints a structured log message
func Log(level LogLevel, component, message string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	formattedMsg := fmt.Sprintf(message, args...)

	var color, resetStr, icon string
	if useColors {
		color = colors[level]
		resetStr = reset
	}
	icon = icons[level]
	label := labels[level]

	fmt.Printf("[%s] %s %s%-6s%s [%s] %s\n",
		timestamp,
		icon,
		color,
		label,
		resetStr,
		component,
		formattedMsg,
	)
}

// Debug logs a debug message
func Debug(component, message string, args ...interface{}) {
	Log(LevelDebug, component, message, args...)
}

// Info logs an info message
func Info(component, message string, args ...interface{}) {
	Log(LevelInfo, component, message, args...)
}

// Warn logs a warning message
func Warn(component, message string, args ...interface{}) {
	Log(LevelWarn, component, message, args...)
}

// Error logs an error message
func Error(component, message string, args ...interface{}) {
	Log(LevelError, component, message, args...)
}

// Success logs a success message
func Success(component, message string, args ...interface{}) {
	Log(LevelSuccess, component, message, args...)
}

// Section prints a section header
func Section(title string) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("  %s\n", title)
	fmt.Println(strings.Repeat("=", 80))
}

// SubSection prints a subsection header
func SubSection(title string) {
	fmt.Println()
	fmt.Printf("━━ %s ━━\n", title)
}

// Message formats a message processing log
func Message(operation, uuid, payload string, metadata map[string]string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	fmt.Printf("%s [MESSAGE] %-12s | UUID: %-20s | Payload: %s",
		timestamp, operation, uuid, payload)

	if len(metadata) > 0 {
		fmt.Print(" | Metadata: ")
		first := true
		for k, v := range metadata {
			if !first {
				fmt.Print(", ")
			}
			fmt.Printf("%s=%s", k, v)
			first = false
		}
	}
	fmt.Println()
}

// Processed formats a processed message log
func Processed(uuid, payload string, totalCount, uuidCount int) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	var color, resetStr string
	if useColors {
		color = "\033[32m" // Green
		resetStr = reset
	}

	fmt.Printf("%s %s[PROCESSED]%s UUID: %-20s | Payload: %-30s | Total: %3d | UUID Count: %2d\n",
		timestamp,
		color,
		resetStr,
		uuid,
		payload,
		totalCount,
		uuidCount,
	)
}

// Skipped formats a skipped (duplicate) message log
func Skipped(uuid, reason string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	var color, resetStr string
	if useColors {
		color = "\033[33m" // Yellow
		resetStr = reset
	}

	fmt.Printf("%s %s[SKIPPED]%s  UUID: %-20s | Reason: %s\n",
		timestamp,
		color,
		resetStr,
		uuid,
		reason,
	)
}

// Summary prints a summary table
func Summary(title string, data map[string]interface{}) {
	fmt.Println()
	fmt.Println(strings.Repeat("─", 80))
	fmt.Printf("  %s\n", title)
	fmt.Println(strings.Repeat("─", 80))

	for key, value := range data {
		fmt.Printf("  %-30s: %v\n", key, value)
	}

	fmt.Println(strings.Repeat("─", 80))
}
