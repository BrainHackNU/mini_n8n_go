package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
)

// Models
type Node struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"` // nano-banana, kling-21-master-t2v, etc
	Label    string                 `json:"label"`
	Position map[string]interface{} `json:"position"`
	Data     map[string]interface{} `json:"data"` // params
}

type Edge struct {
	ID             string `json:"id"`
	Source         string `json:"source"`
	Target         string `json:"target"`
	SourceHandle   string `json:"sourceHandle"`
	TargetHandle   string `json:"targetHandle"`
}

type Workflow struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Nodes     []Node `json:"nodes"`
	Edges     []Edge `json:"edges"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type ExecutionResult struct {
	ID         string                 `json:"id"`
	WorkflowID string                 `json:"workflow_id"`
	Status     string                 `json:"status"`
	Results    map[string]interface{} `json:"results"`
	Error      string                 `json:"error,omitempty"`
	CreatedAt  string                 `json:"created_at"`
}

type HiggsFieldJobSet struct {
	ID            string `json:"id"`
	Type          string `json:"type"`
	CreatedAt     string `json:"created_at"`
	Jobs          []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Results struct {
			Min struct {
				URL  string `json:"url"`
				Type string `json:"type"`
			} `json:"min,omitempty"`
			Raw struct {
				URL  string `json:"url"`
				Type string `json:"type"`
			} `json:"raw,omitempty"`
		} `json:"results"`
	} `json:"jobs"`
	InputParams map[string]interface{} `json:"input_params"`
}

// Store
type SQLiteStore struct {
	db *sql.DB
	mu sync.RWMutex
}

func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	store := &SQLiteStore{db: db}
	if err := store.initDB(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *SQLiteStore) initDB() error {
	schema := `
	CREATE TABLE IF NOT EXISTS workflows (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		nodes TEXT NOT NULL,
		edges TEXT NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS executions (
		id TEXT PRIMARY KEY,
		workflow_id TEXT NOT NULL,
		status TEXT NOT NULL,
		results TEXT,
		error TEXT,
		created_at TEXT NOT NULL,
		FOREIGN KEY(workflow_id) REFERENCES workflows(id)
	);
	`

	_, err := s.db.Exec(schema)
	return err
}

func (s *SQLiteStore) CreateWorkflow(w *Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nodesJSON, _ := json.Marshal(w.Nodes)
	edgesJSON, _ := json.Marshal(w.Edges)

	_, err := s.db.Exec(
		"INSERT INTO workflows (id, name, nodes, edges, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		w.ID, w.Name, string(nodesJSON), string(edgesJSON), w.CreatedAt, w.UpdatedAt,
	)
	return err
}

func (s *SQLiteStore) GetWorkflow(id string) (*Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var w Workflow
	var nodesJSON, edgesJSON string

	err := s.db.QueryRow(
		"SELECT id, name, nodes, edges, created_at, updated_at FROM workflows WHERE id = ?",
		id,
	).Scan(&w.ID, &w.Name, &nodesJSON, &edgesJSON, &w.CreatedAt, &w.UpdatedAt)

	if err != nil {
		return nil, err
	}

	json.Unmarshal([]byte(nodesJSON), &w.Nodes)
	json.Unmarshal([]byte(edgesJSON), &w.Edges)

	return &w, nil
}

func (s *SQLiteStore) UpdateWorkflow(w *Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nodesJSON, _ := json.Marshal(w.Nodes)
	edgesJSON, _ := json.Marshal(w.Edges)
	w.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	_, err := s.db.Exec(
		"UPDATE workflows SET name = ?, nodes = ?, edges = ?, updated_at = ? WHERE id = ?",
		w.Name, string(nodesJSON), string(edgesJSON), w.UpdatedAt, w.ID,
	)
	return err
}

func (s *SQLiteStore) DeleteWorkflow(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("DELETE FROM workflows WHERE id = ?", id)
	return err
}

func (s *SQLiteStore) ListWorkflows() ([]Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query("SELECT id, name, nodes, edges, created_at, updated_at FROM workflows")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workflows []Workflow
	for rows.Next() {
		var w Workflow
		var nodesJSON, edgesJSON string

		if err := rows.Scan(&w.ID, &w.Name, &nodesJSON, &edgesJSON, &w.CreatedAt, &w.UpdatedAt); err != nil {
			continue
		}

		json.Unmarshal([]byte(nodesJSON), &w.Nodes)
		json.Unmarshal([]byte(edgesJSON), &w.Edges)
		workflows = append(workflows, w)
	}

	return workflows, nil
}

func (s *SQLiteStore) SaveExecution(e *ExecutionResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	resultsJSON, _ := json.Marshal(e.Results)

	_, err := s.db.Exec(
		"INSERT INTO executions (id, workflow_id, status, results, error, created_at) VALUES (?, ?, ?, ?, ?, ?)",
		e.ID, e.WorkflowID, e.Status, string(resultsJSON), e.Error, e.CreatedAt,
	)
	return err
}

func (s *SQLiteStore) GetExecution(id string) (*ExecutionResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var e ExecutionResult
	var resultsJSON sql.NullString

	err := s.db.QueryRow(
		"SELECT id, workflow_id, status, results, error, created_at FROM executions WHERE id = ?",
		id,
	).Scan(&e.ID, &e.WorkflowID, &e.Status, &resultsJSON, &e.Error, &e.CreatedAt)

	if err != nil {
		return nil, err
	}

	if resultsJSON.Valid {
		json.Unmarshal([]byte(resultsJSON.String), &e.Results)
	}

	return &e, nil
}

// Higgsfield Client
type HiggsFieldClient struct {
	apiKey    string
	apiSecret string
	baseURL   string
	client    *http.Client
}

func NewHiggsFieldClient(apiKey, apiSecret string) *HiggsFieldClient {
	return &HiggsFieldClient{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		baseURL:   "https://platform.higgsfield.ai/v1",
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (hf *HiggsFieldClient) Generate(modelType string, params map[string]interface{}) (string, error) {
	endpoint := fmt.Sprintf("%s/job-sets", hf.baseURL)

	payload := map[string]interface{}{
		"type":   modelType,
		"params": params,
	}

	payloadJSON, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", endpoint, bytes.NewBuffer(payloadJSON))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("api-key", hf.apiKey)
	req.Header.Set("api-secret", hf.apiSecret)

	resp, err := hf.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("API error: %d - %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	json.Unmarshal(body, &result)

	if id, ok := result["id"].(string); ok {
		return id, nil
	}

	return "", fmt.Errorf("no job set ID in response")
}

func (hf *HiggsFieldClient) GetStatus(jobSetID string) (*HiggsFieldJobSet, error) {
	endpoint := fmt.Sprintf("%s/job-sets/%s", hf.baseURL, jobSetID)

	req, _ := http.NewRequest("GET", endpoint, nil)
	req.Header.Set("api-key", hf.apiKey)
	req.Header.Set("api-secret", hf.apiSecret)

	resp, err := hf.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var jobSet HiggsFieldJobSet
	if err := json.Unmarshal(body, &jobSet); err != nil {
		return nil, err
	}

	return &jobSet, nil
}

// Executor
type Executor struct {
	store  *SQLiteStore
	hf     *HiggsFieldClient
	execMu sync.Mutex
}

func NewExecutor(store *SQLiteStore, hf *HiggsFieldClient) *Executor {
	return &Executor{store: store, hf: hf}
}

func (e *Executor) Execute(workflow *Workflow) *ExecutionResult {
	e.execMu.Lock()
	defer e.execMu.Unlock()

	execution := &ExecutionResult{
		ID:         uuid.New().String(),
		WorkflowID: workflow.ID,
		Status:     "running",
		Results:    make(map[string]interface{}),
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	e.store.SaveExecution(execution)

	// Build node map
	nodeMap := make(map[string]*Node)
	for i := range workflow.Nodes {
		nodeMap[workflow.Nodes[i].ID] = &workflow.Nodes[i]
	}

	// Execute nodes in order (simple topological sort)
	nodeResults := make(map[string]interface{})

	for _, node := range workflow.Nodes {
		// Get input from previous nodes
		nodeInput := make(map[string]interface{})
		for _, edge := range workflow.Edges {
			if edge.Target == node.ID {
				if result, ok := nodeResults[edge.Source]; ok {
					nodeInput[edge.TargetHandle] = result
				}
			}
		}

		// Merge with node data
		for k, v := range node.Data {
			nodeInput[k] = v
		}

		// Call Higgsfield API
		jobSetID, err := e.hf.Generate(node.Type, nodeInput)
		if err != nil {
			execution.Status = "failed"
			execution.Error = fmt.Sprintf("Node %s error: %v", node.ID, err)
			e.store.SaveExecution(execution)
			return execution
		}

		// Poll for result
		var jobSet *HiggsFieldJobSet
		maxRetries := 120 // 10 minutes with 5s interval
		for i := 0; i < maxRetries; i++ {
			time.Sleep(5 * time.Second)
			jobSet, err = e.hf.GetStatus(jobSetID)
			if err != nil {
				continue
			}

			if len(jobSet.Jobs) > 0 && jobSet.Jobs[0].Status == "completed" {
				break
			}
		}

		if jobSet == nil || (len(jobSet.Jobs) > 0 && jobSet.Jobs[0].Status != "completed") {
			execution.Status = "failed"
			execution.Error = fmt.Sprintf("Node %s: generation timeout or failed", node.ID)
			e.store.SaveExecution(execution)
			return execution
		}

		// Extract result URL
		if len(jobSet.Jobs) > 0 && jobSet.Jobs[0].Results.Min.URL != "" {
			nodeResults[node.ID] = jobSet.Jobs[0].Results.Min.URL
		} else if len(jobSet.Jobs) > 0 && jobSet.Jobs[0].Results.Raw.URL != "" {
			nodeResults[node.ID] = jobSet.Jobs[0].Results.Raw.URL
		}

		execution.Results[node.ID] = nodeResults[node.ID]
	}

	execution.Status = "completed"
	e.store.SaveExecution(execution)

	return execution
}

// App
type App struct {
	store    *SQLiteStore
	hf       *HiggsFieldClient
	executor *Executor
	router   *gin.Engine
}

func NewApp(store *SQLiteStore, hf *HiggsFieldClient) *App {
	app := &App{
		store:  store,
		hf:     hf,
		router: gin.Default(),
	}

	app.executor = NewExecutor(store, hf)
	app.setupRoutes()

	return app
}

func (app *App) setupRoutes() {
	// CORS
	app.router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// Workflows
	app.router.POST("/workflows", app.createWorkflow)
	app.router.GET("/workflows", app.listWorkflows)
	app.router.GET("/workflows/:id", app.getWorkflow)
	app.router.PUT("/workflows/:id", app.updateWorkflow)
	app.router.DELETE("/workflows/:id", app.deleteWorkflow)

	// Execute
	app.router.POST("/workflows/:id/execute", app.executeWorkflow)

	// Executions
	app.router.GET("/executions/:id", app.getExecution)

	// Models
	app.router.GET("/models", app.getAvailableModels)
}

func (app *App) createWorkflow(c *gin.Context) {
	var req struct {
		Name  string `json:"name"`
		Nodes []Node `json:"nodes"`
		Edges []Edge `json:"edges"`
	}

	if err := c.BindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	workflow := &Workflow{
		ID:        uuid.New().String(),
		Name:      req.Name,
		Nodes:     req.Nodes,
		Edges:     req.Edges,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	if err := app.store.CreateWorkflow(workflow); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(201, workflow)
}

func (app *App) getWorkflow(c *gin.Context) {
	id := c.Param("id")
	workflow, err := app.store.GetWorkflow(id)
	if err != nil {
		c.JSON(404, gin.H{"error": "workflow not found"})
		return
	}
	c.JSON(200, workflow)
}

func (app *App) listWorkflows(c *gin.Context) {
	workflows, err := app.store.ListWorkflows()
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, workflows)
}

func (app *App) updateWorkflow(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		Name  string `json:"name"`
		Nodes []Node `json:"nodes"`
		Edges []Edge `json:"edges"`
	}

	if err := c.BindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	workflow := &Workflow{
		ID:    id,
		Name:  req.Name,
		Nodes: req.Nodes,
		Edges: req.Edges,
	}

	if err := app.store.UpdateWorkflow(workflow); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	updated, _ := app.store.GetWorkflow(id)
	c.JSON(200, updated)
}

func (app *App) deleteWorkflow(c *gin.Context) {
	id := c.Param("id")
	if err := app.store.DeleteWorkflow(id); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "deleted"})
}

func (app *App) executeWorkflow(c *gin.Context) {
	id := c.Param("id")
	workflow, err := app.store.GetWorkflow(id)
	if err != nil {
		c.JSON(404, gin.H{"error": "workflow not found"})
		return
	}

	// Execute async
	execution := app.executor.Execute(workflow)
	c.JSON(200, execution)
}

func (app *App) getExecution(c *gin.Context) {
	id := c.Param("id")
	execution, err := app.store.GetExecution(id)
	if err != nil {
		c.JSON(404, gin.H{"error": "execution not found"})
		return
	}
	c.JSON(200, execution)
}

func (app *App) getAvailableModels(c *gin.Context) {
	models := gin.H{
		"text_to_image": []string{"nano-banana", "see-dream"},
		"text_to_video": []string{"kling-21-master-t2v", "minimax-t2v", "seedance-v1-lite-t2v"},
		"image_to_video": []string{"kling-2-5", "minimax", "veo3", "wan-25-fast"},
	}
	c.JSON(200, models)
}

func (app *App) Run(port string) error {
	return app.router.Run(":" + port)
}

func main() {
	godotenv.Load()

	apiKey := os.Getenv("HF_API_KEY")
	apiSecret := os.Getenv("HF_API_SECRET")
	port := os.Getenv("PORT")

	if port == "" {
		port = "8080"
	}

	if apiKey == "" || apiSecret == "" {
		log.Fatal("HF_API_KEY and HF_API_SECRET env vars required")
	}

	store, err := NewSQLiteStore("./workflows.db")
	if err != nil {
		log.Fatalf("Failed to init store: %v", err)
	}

	hf := NewHiggsFieldClient(apiKey, apiSecret)
	app := NewApp(store, hf)

	log.Printf("🚀 Server starting on port %s", port)
	if err := app.Run(port); err != nil {
		log.Fatal(err)
	}
}