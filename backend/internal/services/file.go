package services

import (
	"encoding/csv"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
)

type FileService struct {
	UploadDir string
}

func NewFileService(uploadDir string) *FileService {
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		fmt.Printf("Warning: failed to create upload directory: %v\n", err)
	}
	return &FileService{UploadDir: uploadDir}
}

func (s *FileService) SaveUploadedFile(file *multipart.FileHeader) (string, error) {
	src, err := file.Open()
	if err != nil {
		return "", fmt.Errorf("failed to open uploaded file: %v", err)
	}
	defer src.Close()

	dstPath := filepath.Join(s.UploadDir, file.Filename)
	dst, err := os.Create(dstPath)
	if err != nil {
		return "", fmt.Errorf("failed to create destination file: %v", err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return "", fmt.Errorf("failed to copy file: %v", err)
	}

	return dstPath, nil
}

func (s *FileService) ReadCSV(filePath string, delimiter rune) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = delimiter

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %v", err)
	}

	return records, nil
}

func (s *FileService) WriteCSV(filePath string, data [][]interface{}, delimiter rune) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = delimiter
	defer writer.Flush()

	for _, row := range data {
		strRow := make([]string, len(row))
		for i, val := range row {
			strRow[i] = fmt.Sprintf("%v", val)
		}
		if err := writer.Write(strRow); err != nil {
			return fmt.Errorf("failed to write row: %v", err)
		}
	}

	return nil
}

func (s *FileService) GetFileColumns(filePath string, delimiter rune) ([]string, error) {
	records, err := s.ReadCSV(filePath, delimiter)
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	return records[0], nil
}

func (s *FileService) PreviewFile(filePath string, delimiter rune, limit int) ([][]string, error) {
	records, err := s.ReadCSV(filePath, delimiter)
	if err != nil {
		return nil, err
	}

	if len(records) > limit {
		return records[:limit], nil
	}

	return records, nil
}

func (s *FileService) Cleanup(filePath string) error {
	return os.Remove(filePath)
} 