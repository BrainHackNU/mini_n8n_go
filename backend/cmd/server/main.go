package main

import (
	"log"
	"net/http"
	"os"

	"higgsfield-flow/internal/api"
	"higgsfield-flow/internal/services"
)

func main() {
	// Получаем API ключ из env
	apiKey := os.Getenv("HIGGSFIELD_API_KEY")
	if apiKey == "" {
		log.Fatal("HIGGSFIELD_API_KEY environment variable is required")
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Инициализируем сервисы
	higgsfieldClient := services.NewHiggsfieldClient(apiKey)
	executor := services.NewPipelineExecutor(higgsfieldClient)

	// Настраиваем handlers
	handler := api.NewHandler(executor)
	mux := handler.SetupRoutes()

	// Применяем middleware
	wrappedMux := api.LoggingMiddleware(api.CORSMiddleware(mux))

	// Запускаем сервер
	log.Printf("🚀 Server starting on port %s", port)
	log.Printf("📡 Health check: http://localhost:%s/health", port)
	
	if err := http.ListenAndServe(":"+port, wrappedMux); err != nil {
		log.Fatal(err)
	}
}