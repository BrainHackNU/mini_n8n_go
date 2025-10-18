package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"higgsfield-flow/internal/models"
)

type HiggsfieldClient struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

func NewHiggsfieldClient(apiKey string) *HiggsfieldClient {
	return &HiggsfieldClient{
		apiKey:  apiKey,
		baseURL: "https://api.higgsfield.ai",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetEndpoint возвращает endpoint для модели
func (c *HiggsfieldClient) GetEndpoint(model string) string {
	endpoints := map[string]string{
		"nano_banana":       "/v1/job-sets/nano_banana",
		"see_dream":         "/v1/job-sets/see-dream",
		"minimax_t2v":       "/v1/job-sets/minimax-t2v",
		"seedance_lite":     "/v1/job-sets/seedance-v1-lite-t2v",
		"kling_2_5":         "/v1/job-sets/kling-2-5",
		"veo3":              "/v1/job-sets/veo3",
		"wan_25_fast":       "/v1/job-sets/wan-25-fast",
		"veo3_speak":        "/v1/job-sets/veo3-speak",
	}
	return endpoints[model]
}

// SubmitJob отправляет задачу в Higgsfield
func (c *HiggsfieldClient) SubmitJob(endpoint string, params map[string]interface{}) (*models.HiggsfieldJobResponse, error) {
	url := c.baseURL + endpoint

	jsonData, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal params: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
	}

	var jobResp models.HiggsfieldJobResponse
	if err := json.Unmarshal(body, &jobResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &jobResp, nil
}

// GetJobStatus проверяет статус задачи
func (c *HiggsfieldClient) GetJobStatus(jobID string) (*models.HiggsfieldJobResponse, error) {
	url := fmt.Sprintf("%s/v1/jobs/%s", c.baseURL, jobID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
	}

	var jobResp models.HiggsfieldJobResponse
	if err := json.Unmarshal(body, &jobResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &jobResp, nil
}

// WaitForJob ждёт завершения задачи (polling)
func (c *HiggsfieldClient) WaitForJob(jobID string, maxWaitTime time.Duration) (*models.HiggsfieldJobResponse, error) {
	startTime := time.Now()
	ticker := time.NewTicker(5 * time.Second) // Проверяем каждые 5 секунд
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			status, err := c.GetJobStatus(jobID)
			if err != nil {
				return nil, err
			}

			// Статусы: "pending", "running", "completed", "failed"
			if status.Status == "completed" {
				return status, nil
			}
			if status.Status == "failed" {
				return nil, fmt.Errorf("job failed: %s", jobID)
			}

			// Проверяем timeout
			if time.Since(startTime) > maxWaitTime {
				return nil, fmt.Errorf("job timeout: %s", jobID)
			}
		}
	}
}