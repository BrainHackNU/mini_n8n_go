package api

import (
	"encoding/json"
	"net/http"

	"higgsfield-flow/internal/models"
	"higgsfield-flow/internal/services"
)

type Handler struct {
	executor *services.PipelineExecutor
}

func NewHandler(executor *services.PipelineExecutor) *Handler {
	return &Handler{
		executor: executor,
	}
}

// SetupRoutes настраивает маршруты
func (h *Handler) SetupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/pipeline/execute", h.handleExecutePipeline)
	mux.HandleFunc("/api/execution/", h.handleGetExecution)
	mux.HandleFunc("/health", h.handleHealth)

	return mux
}

// handleExecutePipeline POST /api/pipeline/execute
func (h *Handler) handleExecutePipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var pipeline models.Pipeline
	if err := json.NewDecoder(r.Body).Decode(&pipeline); err != nil {
		respondError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Валидация
	if len(pipeline.Nodes) == 0 {
		respondError(w, "Pipeline must contain at least one node", http.StatusBadRequest)
		return
	}

	// Запускаем выполнение
	execution, err := h.executor.ExecutePipeline(&pipeline)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, execution, http.StatusCreated)
}

// handleGetExecution GET /api/execution/{id}
func (h *Handler) handleGetExecution(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Извлекаем ID из URL
	executionID := r.URL.Path[len("/api/execution/"):]
	if executionID == "" {
		respondError(w, "Execution ID required", http.StatusBadRequest)
		return
	}

	execution, err := h.executor.GetExecution(executionID)
	if err != nil {
		respondError(w, err.Error(), http.StatusNotFound)
		return
	}

	respondJSON(w, execution, http.StatusOK)
}

// handleHealth GET /health
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
}

// Вспомогательные функции

func respondJSON(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, message string, status int) {
	respondJSON(w, map[string]string{"error": message}, status)
}