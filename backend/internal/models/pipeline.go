package models

import "time"

// NodeType определяет тип модели
type NodeType string

const (
	TextToImage  NodeType = "text_to_image"
	TextToVideo  NodeType = "text_to_video"
	ImageToVideo NodeType = "image_to_video"
	AudioOverlay NodeType = "audio_overlay"
)

// Node представляет один блок в пайплайне
type Node struct {
	ID       string                 `json:"id"`
	Model    string                 `json:"model"`    // "nano_banana", "kling_2_5", etc.
	Type     NodeType               `json:"type"`     // text_to_image, image_to_video, etc.
	Params   map[string]interface{} `json:"params"`   // Параметры модели
	Position Position               `json:"position"` // Позиция на canvas
}

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// Connection связывает выход одной ноды со входом другой
type Connection struct {
	Source string `json:"source"` // ID ноды-источника
	Target string `json:"target"` // ID ноды-приёмника
}

// Pipeline описывает весь граф генерации
type Pipeline struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Nodes       []Node       `json:"nodes"`
	Connections []Connection `json:"connections"`
	CreatedAt   time.Time    `json:"created_at"`
}

// ExecutionRequest запрос на выполнение пайплайна
type ExecutionRequest struct {
	PipelineID string `json:"pipeline_id"`
}

// ExecutionStatus статус выполнения
type ExecutionStatus string

const (
	StatusPending    ExecutionStatus = "pending"
	StatusRunning    ExecutionStatus = "running"
	StatusCompleted  ExecutionStatus = "completed"
	StatusFailed     ExecutionStatus = "failed"
)

// Execution хранит состояние выполнения пайплайна
type Execution struct {
	ID           string                 `json:"id"`
	PipelineID   string                 `json:"pipeline_id"`
	Status       ExecutionStatus        `json:"status"`
	CurrentNode  string                 `json:"current_node"`
	NodeStatuses map[string]NodeStatus  `json:"node_statuses"` // ID ноды -> статус
	Results      map[string]interface{} `json:"results"`       // ID ноды -> результат
	Error        string                 `json:"error,omitempty"`
	StartedAt    time.Time              `json:"started_at"`
	CompletedAt  *time.Time             `json:"completed_at,omitempty"`
}

// NodeStatus статус выполнения одной ноды
type NodeStatus struct {
	Status    ExecutionStatus `json:"status"`
	JobID     string          `json:"job_id,omitempty"`
	OutputURL string          `json:"output_url,omitempty"`
	Error     string          `json:"error,omitempty"`
	StartedAt time.Time       `json:"started_at"`
	Duration  float64         `json:"duration"` // в секундах
}

// HiggsfieldJobRequest базовая структура запроса к Higgsfield API
type HiggsfieldJobRequest struct {
	Endpoint string                 `json:"endpoint"`
	Params   map[string]interface{} `json:"params"`
}

// HiggsfieldJobResponse ответ от Higgsfield API
type HiggsfieldJobResponse struct {
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
	Results struct {
		Raw struct {
			URL string `json:"url"`
		} `json:"raw"`
	} `json:"results"`
}