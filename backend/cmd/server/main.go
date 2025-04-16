package main

import (
	"log"
	"os"

	"clickhouse-integration/internal/handlers"
	"clickhouse-integration/internal/services"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/rs/cors"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	// Initialize services
	clickHouseService := services.NewClickHouseService()
	fileService := services.NewFileService("uploads")

	// Initialize handlers
	clickHouseHandler := handlers.NewClickHouseHandler(clickHouseService)
	fileHandler := handlers.NewFileHandler(fileService, clickHouseService)

	// Initialize router
	router := gin.Default()

	// Configure CORS
	corsConfig := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Origin", "Content-Type", "Accept", "Authorization"},
		AllowCredentials: true,
	})

	// Apply CORS middleware
	router.Use(func(c *gin.Context) {
		corsConfig.HandlerFunc(c.Writer, c.Request)
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// API routes
	api := router.Group("/api")
	{
		// ClickHouse routes
		api.POST("/clickhouse/connect", clickHouseHandler.Connect)
		api.GET("/clickhouse/tables", clickHouseHandler.GetTables)
		api.GET("/clickhouse/columns/:table", clickHouseHandler.GetColumns)
		api.POST("/clickhouse/export", clickHouseHandler.ExportData)
		api.POST("/clickhouse/import", clickHouseHandler.ImportData)

		// File routes
		fileGroup := api.Group("/file")
		{
			fileGroup.POST("/upload", fileHandler.UploadFile)
			fileGroup.GET("/columns", fileHandler.GetColumns)
			fileGroup.GET("/preview", fileHandler.GetPreview)
			fileGroup.POST("/import", fileHandler.ImportFile)
			fileGroup.POST("/cleanup", fileHandler.Cleanup)
		}
	}

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server starting on port %s", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
