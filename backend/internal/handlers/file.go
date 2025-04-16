package handlers

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"clickhouse-integration/internal/models"
	"clickhouse-integration/internal/services"

	"github.com/gin-gonic/gin"
)

type FileHandler struct {
	service           *services.FileService
	clickHouseService *services.ClickHouseService
	maxSize           int64
}

func NewFileHandler(fileService *services.FileService, clickHouseService *services.ClickHouseService) *FileHandler {
	maxSize := int64(10 * 1024 * 1024) // Default 10MB
	if envSize := os.Getenv("MAX_UPLOAD_SIZE"); envSize != "" {
		if size, err := strconv.ParseInt(envSize, 10, 64); err == nil {
			maxSize = size
		}
	}
	return &FileHandler{
		service:           fileService,
		clickHouseService: clickHouseService,
		maxSize:           maxSize,
	}
}

func (h *FileHandler) UploadFile(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Failed to get file from request",
		})
		return
	}

	// Check file size
	if file.Size > h.maxSize {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   fmt.Sprintf("File size exceeds limit of %d bytes", h.maxSize),
		})
		return
	}

	fmt.Printf("Received file upload: %s (size: %d bytes)\n", file.Filename, file.Size)

	// Create uploads directory if it doesn't exist
	if err := os.MkdirAll("uploads", 0755); err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   "Failed to create uploads directory",
		})
		return
	}

	// Generate unique filename
	filename := fmt.Sprintf("%d_%s", time.Now().UnixNano(), file.Filename)
	filePath := filepath.Join("uploads", filename)

	// Save the file
	if err := c.SaveUploadedFile(file, filePath); err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   "Failed to save file",
		})
		return
	}

	fmt.Printf("File saved successfully: %s\n", filePath)

	c.JSON(http.StatusOK, models.Response{
		Success: true,
		Data:    map[string]string{"filePath": filePath},
	})
}

func (h *FileHandler) GetColumns(c *gin.Context) {
	filePath := c.Query("filePath")
	delimiter := c.Query("delimiter")
	if delimiter == "" {
		delimiter = ","
	}

	fmt.Printf("Reading columns from file: %s (delimiter: %s)\n", filePath, delimiter)

	file, err := os.Open(filePath)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Failed to open file",
		})
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = rune(delimiter[0])

	// Read header
	headers, err := reader.Read()
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Failed to read file header",
		})
		return
	}

	fmt.Printf("Found %d columns in file: %v\n", len(headers), headers)

	c.JSON(http.StatusOK, models.Response{
		Success: true,
		Data:    headers,
	})
}

func (h *FileHandler) GetPreview(c *gin.Context) {
	filePath := c.Query("filePath")
	delimiter := c.Query("delimiter")
	limit := c.DefaultQuery("limit", "100")
	if delimiter == "" {
		delimiter = ","
	}

	limitInt, err := strconv.Atoi(limit)
	if err != nil {
		limitInt = 100
	}

	fmt.Printf("Generating preview for file: %s (delimiter: %s, limit: %d)\n", filePath, delimiter, limitInt)

	file, err := os.Open(filePath)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Failed to open file",
		})
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = rune(delimiter[0])

	var data [][]string
	rowCount := 0

	// Read header
	headers, err := reader.Read()
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Failed to read file header",
		})
		return
	}

	fmt.Printf("Found headers: %v\n", headers)

	for rowCount < limitInt {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			c.JSON(http.StatusBadRequest, models.Response{
				Success: false,
				Error:   "Failed to read file row",
			})
			return
		}
		data = append(data, row)
		rowCount++
		if rowCount%1000 == 0 {
			fmt.Printf("Processed %d rows\n", rowCount)
		}
	}

	fmt.Printf("Generated preview with %d rows\n", len(data))
	if len(data) > 0 {
		fmt.Printf("First row sample: %v\n", data[0])
	}

	c.JSON(http.StatusOK, models.Response{
		Success: true,
		Data:    data,
	})
}

func (h *FileHandler) Cleanup(c *gin.Context) {
	filePath := c.Query("filePath")
	if filePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "File path is required",
		})
		return
	}

	if err := h.service.Cleanup(filePath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "File cleaned up successfully",
	})
}

func (h *FileHandler) ImportFile(c *gin.Context) {
	var req struct {
		FilePath  string          `json:"filePath"`
		Table     string          `json:"table"`
		Columns   []models.Column `json:"columns"`
		Delimiter string          `json:"delimiter"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	if req.Delimiter == "" {
		req.Delimiter = ","
	}

	fmt.Printf("Starting file import: %s to table %s\n", req.FilePath, req.Table)

	file, err := os.Open(req.FilePath)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Failed to open file",
		})
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = rune(req.Delimiter[0])

	// Skip header
	if _, err := reader.Read(); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Failed to read file header",
		})
		return
	}

	var data [][]interface{}
	rowCount := 0

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			c.JSON(http.StatusBadRequest, models.Response{
				Success: false,
				Error:   "Failed to read file row",
			})
			return
		}

		// Convert string row to interface{} slice
		interfaceRow := make([]interface{}, len(row))
		for i, val := range row {
			// Try to convert to appropriate type based on column type
			if i < len(req.Columns) {
				switch req.Columns[i].Type {
				case "Int32", "Int64", "UInt32", "UInt64":
					if num, err := strconv.ParseInt(val, 10, 64); err == nil {
						interfaceRow[i] = num
					} else {
						interfaceRow[i] = val
					}
				case "Float32", "Float64":
					if num, err := strconv.ParseFloat(val, 64); err == nil {
						interfaceRow[i] = num
					} else {
						interfaceRow[i] = val
					}
				case "Date", "DateTime":
					if t, err := time.Parse("2006-01-02", val); err == nil {
						interfaceRow[i] = t
					} else {
						interfaceRow[i] = val
					}
				default:
					interfaceRow[i] = val
				}
			} else {
				interfaceRow[i] = val
			}
		}
		data = append(data, interfaceRow)
		rowCount++

		if rowCount%1000 == 0 {
			fmt.Printf("Processed %d rows\n", rowCount)
		}
	}

	fmt.Printf("Read %d rows from file\n", rowCount)

	// Create import request
	importReq := models.ImportRequest{
		Table:   req.Table,
		Columns: req.Columns,
		Data:    data,
	}

	// Get ClickHouse connection
	conn, err := h.clickHouseService.Connect(models.ClickHouseConfig{
		Host:     c.GetString("clickhouse_host"),
		Port:     c.GetInt("clickhouse_port"),
		Database: c.GetString("clickhouse_database"),
		User:     c.GetString("clickhouse_user"),
		JWTToken: c.GetString("clickhouse_jwt_token"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   fmt.Sprintf("Failed to connect to ClickHouse: %v", err),
		})
		return
	}
	defer conn.Close()

	// Import data
	if err := h.clickHouseService.ImportData(conn, importReq); err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   fmt.Sprintf("Failed to import data: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Success: true,
		Data:    map[string]int{"rows_imported": rowCount},
	})
}
