package models

type ClickHouseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	JWTToken string `json:"jwtToken"`
}

type TableInfo struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type ExportRequest struct {
	Config  ClickHouseConfig `json:"config"`
	Table   string          `json:"table"`
	Columns []string        `json:"columns"`
	Query   string          `json:"query,omitempty"`
}

type ImportRequest struct {
	Config    ClickHouseConfig `json:"config"`
	Table     string          `json:"table"`
	Columns   []Column        `json:"columns"`
	Data      [][]interface{} `json:"data"`
	Delimiter string          `json:"delimiter"`
}

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}
	