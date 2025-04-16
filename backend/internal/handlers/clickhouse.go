package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"clickhouse-integration/internal/models"
	"clickhouse-integration/internal/services"
)

type ClickHouseHandler struct {
	service *services.ClickHouseService
}

func NewClickHouseHandler(service *services.ClickHouseService) *ClickHouseHandler {
	return &ClickHouseHandler{service: service}
}

func (h *ClickHouseHandler) Connect(c *gin.Context) {
	var config models.ClickHouseConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	conn, err := h.service.Connect(config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	defer conn.Close()

	c.JSON(http.StatusOK, models.Response{
		Success: true,
		Message: "Successfully connected to ClickHouse",
	})
}

func (h *ClickHouseHandler) GetTables(c *gin.Context) {
	var config models.ClickHouseConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	conn, err := h.service.Connect(config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	defer conn.Close()

	tables, err := h.service.GetTables(conn, config.Database)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Success: true,
		Data:    tables,
	})
}

func (h *ClickHouseHandler) GetColumns(c *gin.Context) {
	var config models.ClickHouseConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	table := c.Param("table")
	if table == "" {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Table name is required",
		})
		return
	}

	conn, err := h.service.Connect(config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	defer conn.Close()

	columns, err := h.service.GetColumns(conn, config.Database, table)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Success: true,
		Data:    columns,
	})
}

func (h *ClickHouseHandler) ExportData(c *gin.Context) {
	var req models.ExportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	conn, err := h.service.Connect(req.Config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	defer conn.Close()

	data, err := h.service.ExportData(conn, req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Success: true,
		Data:    data,
	})
}

func (h *ClickHouseHandler) ImportData(c *gin.Context) {
	var req models.ImportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	conn, err := h.service.Connect(req.Config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	defer conn.Close()

	if err := h.service.ImportData(conn, req); err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Success: true,
		Message: "Data imported successfully",
	})
} 