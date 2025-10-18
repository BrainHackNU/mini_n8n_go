package services

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"higgsfield-flow/internal/models"
)

type PipelineExecutor struct {
	client     *HiggsfieldClient
	executions sync.Map // executionID -> *models.Execution
}

func NewPipelineExecutor(client *HiggsfieldClient) *PipelineExecutor {
	return &PipelineExecutor{
		client: client,
	}
}

// ExecutePipeline запускает выполнение пайплайна асинхронно
func (e *PipelineExecutor) ExecutePipeline(pipeline *models.Pipeline) (*models.Execution, error) {
	// Создаём execution record
	execution := &models.Execution{
		ID:           generateID(),
		PipelineID:   pipeline.ID,
		Status:       models.StatusPending,
		NodeStatuses: make(map[string]models.NodeStatus),
		Results:      make(map[string]interface{}),
		StartedAt:    time.Now(),
	}

	// Сохраняем в памяти
	e.executions.Store(execution.ID, execution)

	// Запускаем выполнение в горутине
	go e.runPipeline(pipeline, execution)

	return execution, nil
}

// GetExecution возвращает статус выполнения
func (e *PipelineExecutor) GetExecution(executionID string) (*models.Execution, error) {
	val, ok := e.executions.Load(executionID)
	if !ok {
		return nil, fmt.Errorf("execution not found: %s", executionID)
	}
	return val.(*models.Execution), nil
}

// runPipeline выполняет пайплайн последовательно
func (e *PipelineExecutor) runPipeline(pipeline *models.Pipeline, execution *models.Execution) {
	execution.Status = models.StatusRunning

	// Топологическая сортировка нод (простая версия - предполагаем линейную цепочку)
	sortedNodes := e.sortNodes(pipeline)

	for _, node := range sortedNodes {
		execution.CurrentNode = node.ID
		
		nodeStatus := models.NodeStatus{
			Status:    models.StatusRunning,
			StartedAt: time.Now(),
		}
		execution.NodeStatuses[node.ID] = nodeStatus

		// Подставляем результаты предыдущих нод
		params := e.resolveParams(node.Params, execution)

		// Выполняем ноду
		err := e.executeNode(node, params, execution)
		
		nodeStatus = execution.NodeStatuses[node.ID]
		nodeStatus.Duration = time.Since(nodeStatus.StartedAt).Seconds()
		
		if err != nil {
			nodeStatus.Status = models.StatusFailed
			nodeStatus.Error = err.Error()
			execution.NodeStatuses[node.ID] = nodeStatus
			
			execution.Status = models.StatusFailed
			execution.Error = fmt.Sprintf("Node %s failed: %v", node.ID, err)
			return
		}

		nodeStatus.Status = models.StatusCompleted
		execution.NodeStatuses[node.ID] = nodeStatus
	}

	// Всё прошло успешно
	now := time.Now()
	execution.Status = models.StatusCompleted
	execution.CompletedAt = &now
}

// executeNode выполняет одну ноду
func (e *PipelineExecutor) executeNode(node models.Node, params map[string]interface{}, execution *models.Execution) error {
	endpoint := e.client.GetEndpoint(node.Model)
	if endpoint == "" {
		return fmt.Errorf("unknown model: %s", node.Model)
	}

	// Отправляем задачу
	jobResp, err := e.client.SubmitJob(endpoint, params)
	if err != nil {
		return fmt.Errorf("failed to submit job: %w", err)
	}

	// Сохраняем jobID
	nodeStatus := execution.NodeStatuses[node.ID]
	nodeStatus.JobID = jobResp.JobID
	execution.NodeStatuses[node.ID] = nodeStatus

	// Ждём завершения (максимум 10 минут)
	result, err := e.client.WaitForJob(jobResp.JobID, 10*time.Minute)
	if err != nil {
		return fmt.Errorf("job failed: %w", err)
	}

	// Сохраняем результат
	nodeStatus = execution.NodeStatuses[node.ID]
	nodeStatus.OutputURL = result.Results.Raw.URL
	execution.NodeStatuses[node.ID] = nodeStatus
	
	execution.Results[node.ID] = map[string]interface{}{
		"url":    result.Results.Raw.URL,
		"job_id": jobResp.JobID,
	}

	return nil
}

// resolveParams заменяет переменные типа {{node_id.output_url}}
func (e *PipelineExecutor) resolveParams(params map[string]interface{}, execution *models.Execution) map[string]interface{} {
	resolved := make(map[string]interface{})
	
	for key, value := range params {
		switch v := value.(type) {
		case string:
			resolved[key] = e.resolveString(v, execution)
		case []interface{}:
			resolvedArr := make([]interface{}, len(v))
			for i, item := range v {
				if str, ok := item.(string); ok {
					resolvedArr[i] = e.resolveString(str, execution)
				} else {
					resolvedArr[i] = item
				}
			}
			resolved[key] = resolvedArr
		default:
			resolved[key] = value
		}
	}
	
	return resolved
}

// resolveString заменяет {{node_id.output_url}} на реальный URL
func (e *PipelineExecutor) resolveString(str string, execution *models.Execution) string {
	// Простой парсинг: {{node_id.output_url}}
	if strings.Contains(str, "{{") && strings.Contains(str, "}}") {
		start := strings.Index(str, "{{")
		end := strings.Index(str, "}}")
		if start >= 0 && end > start {
			variable := str[start+2 : end]
			parts := strings.Split(variable, ".")
			if len(parts) == 2 {
				nodeID := parts[0]
				if result, ok := execution.Results[nodeID]; ok {
					if resultMap, ok := result.(map[string]interface{}); ok {
						if url, ok := resultMap["url"].(string); ok {
							return url
						}
					}
				}
			}
		}
	}
	return str
}

// sortNodes выполняет топологическую сортировку (упрощённая версия)
func (e *PipelineExecutor) sortNodes(pipeline *models.Pipeline) []models.Node {
	// Для простоты - просто возвращаем ноды в порядке, как есть
	// В продакшене нужна настоящая топологическая сортировка по connections
	return pipeline.Nodes
}

func generateID() string {
	return fmt.Sprintf("exec_%d", time.Now().UnixNano())
}