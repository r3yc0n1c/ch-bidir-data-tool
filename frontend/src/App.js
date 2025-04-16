import React, { useState, useRef } from 'react';
import {
  Container,
  Box,
  Typography,
  Paper,
  TextField,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Checkbox,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  CircularProgress,
  Alert,
  IconButton,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow
} from '@mui/material';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import PreviewIcon from '@mui/icons-material/Preview';
import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8080/api';

function PreviewDialog({ open, onClose, columns, data }) {
  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>
      <DialogTitle>Data Preview (First {data?.length || 0} Rows)</DialogTitle>
      <DialogContent dividers>
        {data && data.length > 0 ? (
          <TableContainer component={Paper} sx={{ maxHeight: 600 }}>
            <Table stickyHeader size="small">
              <TableHead>
                <TableRow>
                  {columns.map((col) => (
                    <TableCell key={col} sx={{ fontWeight: 'bold' }}>{col}</TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {data.map((row, rowIndex) => (
                  <TableRow key={rowIndex}>
                    {row.map((cell, cellIndex) => (
                      <TableCell key={`${rowIndex}-${cellIndex}`}>{String(cell)}</TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        ) : (
          <Typography>No data to preview or data is empty.</Typography>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
}

function App() {
  const [sourceType, setSourceType] = useState('clickhouse');
  const [targetType, setTargetType] = useState('file');

  const [connection, setConnection] = useState({
    host: '127.0.0.1',
    port: '9000',
    database: 'default',
    user: 'default',
    jwt_token: 'password',
  });
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');

  const [fileConfig, setFileConfig] = useState({
    file_path: '',
    delimiter: ',',
  });
  const [selectedFile, setSelectedFile] = useState(null);
  const fileInputRef = useRef(null);

  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loadingMessage, setLoadingMessage] = useState('');
  const [message, setMessage] = useState({ type: '', text: '' });

  const [isPreviewOpen, setIsPreviewOpen] = useState(false);
  const [previewColumns, setPreviewColumns] = useState([]);
  const [previewData, setPreviewData] = useState([]);

  const handleSourceTypeChange = (event) => {
    const newSourceType = event.target.value;
    setSourceType(newSourceType);
    setTables([]);
    setSelectedTable('');
    setColumns([]);
    setSelectedColumns([]);
    setSelectedFile(null);
    setMessage({ type: '', text: '' });
    setTargetType(newSourceType === 'clickhouse' ? 'file' : 'clickhouse');
  };

  const handleConnectionChange = (event) => {
    setConnection({ ...connection, [event.target.name]: event.target.value });
  };

  const handleFileConfigChange = (event) => {
    setFileConfig({ ...fileConfig, [event.target.name]: event.target.value });
  };

  const handleFileSelect = (event) => {
    const file = event.target.files[0];
    if (file) {
      setSelectedFile(file);
      setMessage({ type: '', text: '' });
      loadColumnsFromFile(file);
    } else {
      setSelectedFile(null);
      setColumns([]);
      setSelectedColumns([]);
    }
    event.target.value = null;
  };

  const handleTableChange = (event) => {
    const newTable = event.target.value;
    setSelectedTable(newTable);
    setColumns([]);
    setSelectedColumns([]);
    if (newTable) {
      loadColumnsFromTable(newTable);
    }
  };

  const handleColumnToggle = (column) => () => {
    const currentIndex = selectedColumns.findIndex(c => c.name === column.name);
    const newChecked = [...selectedColumns];

    if (currentIndex === -1) {
      newChecked.push(column);
    } else {
      newChecked.splice(currentIndex, 1);
    }
    setSelectedColumns(newChecked);
  };

  const withLoading = (message, asyncFn) => async (...args) => {
    setLoading(true);
    setLoadingMessage(message);
    setMessage({ type: '', text: '' });
    try {
      await asyncFn(...args);
    } catch (error) {
      const errorMessage = error.response?.data?.error || error.message || 'Failed to perform operation';
      console.error(`${message} failed:`, error);
      setMessage({ type: 'error', text: errorMessage });
    } finally {
      setLoading(false);
      setLoadingMessage('');
    }
  };

  const connectToClickHouse = withLoading('Connecting to ClickHouse', async () => {
    setTables([]);
    setSelectedTable('');
    setColumns([]);
    setSelectedColumns([]);
    try {
      const response = await axios.post(`${API_URL}/clickhouse/connect`, {
        host: connection.host,
        port: parseInt(connection.port),
        database: connection.database,
        user: connection.user,
        jwtToken: connection.jwt_token
      });
      if (response.data.success) {
        const tablesResponse = await axios.post(`${API_URL}/clickhouse/tables`, {
          host: connection.host,
          port: parseInt(connection.port),
          database: connection.database,
          user: connection.user,
          jwtToken: connection.jwt_token
        });
        if (tablesResponse.data.success) {
          setTables(tablesResponse.data.data);
          setMessage({ type: 'success', text: `Connected! Found ${tablesResponse.data.data.length} tables.` });
        }
      }
    } catch (error) {
      const errorMessage = error.response?.data?.error || error.message || 'Connection failed';
      setMessage({ type: 'error', text: errorMessage });
    }
  });

  const loadColumnsFromTable = withLoading('Loading columns', async (tableName) => {
    setColumns([]);
    setSelectedColumns([]);
    try {
      const response = await axios.post(`${API_URL}/clickhouse/columns/${tableName}`, {
        host: connection.host,
        port: parseInt(connection.port),
        database: connection.database,
        user: connection.user,
        jwtToken: connection.jwt_token
      });
      if (response.data.success) {
        setColumns(response.data.data);
        setSelectedColumns(response.data.data.map(col => col.name));
        setMessage({ type: 'success', text: `Loaded ${response.data.data.length} columns from ${tableName}` });
      }
    } catch (error) {
      const errorMessage = error.response?.data?.error || error.message || 'Failed to load columns';
      setMessage({ type: 'error', text: errorMessage });
    }
  });

  const loadColumnsFromFile = withLoading('Reading file columns', async (file) => {
    setColumns([]);
    setSelectedColumns([]);
    const formData = new FormData();
    formData.append('file', file);

    try {
      const uploadResponse = await axios.post(`${API_URL}/file/upload`, formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });

      if (uploadResponse.data.success) {
        const filePath = uploadResponse.data.data.filePath;
        const columnsResponse = await axios.get(`${API_URL}/file/columns?filePath=${filePath}&delimiter=${fileConfig.delimiter}`);
        if (columnsResponse.data.success) {
          // Convert string columns to Column objects with inferred types
          const columnObjects = columnsResponse.data.data.map(col => ({
            name: col,
            type: 'String', // Default type
            nullable: true
          }));
          setColumns(columnObjects);
          setSelectedColumns(columnObjects);
          setMessage({ type: 'success', text: `Found ${columnsResponse.data.data.length} columns in ${file.name}` });
        }
      }
    } catch (error) {
      const errorMessage = error.response?.data?.error || error.message || 'Failed to read file header';
      setMessage({ type: 'error', text: errorMessage });
      setSelectedFile(null);
    }
  });

  const handlePreview = withLoading('Fetching preview data', async () => {
    if (selectedColumns.length === 0) {
      setMessage({ type: 'warning', text: 'Please select columns to preview.' });
      return;
    }
    
    setPreviewData([]);
    setPreviewColumns([]);

    try {
      let response;
      if (sourceType === 'clickhouse') {
        if (!selectedTable) {
          setMessage({ type: 'warning', text: 'Please select a table first.' });
          return;
        }
        response = await axios.post(`${API_URL}/clickhouse/export`, {
          config: {
            host: connection.host,
            port: parseInt(connection.port),
            database: connection.database,
            user: connection.user,
            jwtToken: connection.jwt_token
          },
          table: selectedTable,
          columns: selectedColumns
        });
      } else {
        if (!selectedFile) {
          setMessage({ type: 'warning', text: 'Please select a file first.' });
          return;
        }
        response = await axios.get(`${API_URL}/file/preview?filePath=${selectedFile.name}&delimiter=${fileConfig.delimiter}&limit=100`);
      }

      if (response.data.success) {
        setPreviewColumns(selectedColumns);
        setPreviewData(response.data.data);
        setIsPreviewOpen(true);
      }
    } catch (error) {
      const errorMessage = error.response?.data?.error || error.message || 'Failed to fetch preview data';
      setMessage({ type: 'error', text: errorMessage });
    }
  });

  const handleStartIngestion = withLoading('Starting ingestion', async () => {
    if (selectedColumns.length === 0) {
      setMessage({ type: 'warning', text: 'Please select columns to ingest.' });
      return;
    }

    try {
      let response;
      if (sourceType === 'clickhouse') {
        if (!selectedTable) {
          setMessage({ type: 'warning', text: 'Please select a table first.' });
          return;
        }
        response = await axios.post(`${API_URL}/clickhouse/export`, {
          config: {
            host: connection.host,
            port: parseInt(connection.port),
            database: connection.database,
            user: connection.user,
            jwtToken: connection.jwt_token
          },
          table: selectedTable,
          columns: selectedColumns
        });
      } else {
        if (!selectedFile) {
          setMessage({ type: 'warning', text: 'Please select a file first.' });
          return;
        }

        // First, upload the file
        const formData = new FormData();
        formData.append('file', selectedFile);

        const uploadResponse = await axios.post(`${API_URL}/file/upload`, formData, {
          headers: {
            'Content-Type': 'multipart/form-data',
          },
        });

        if (!uploadResponse.data.success) {
          throw new Error('Failed to upload file');
        }

        const filePath = uploadResponse.data.data.filePath;

        // Then import the data
        response = await axios.post(`${API_URL}/clickhouse/import`, {
          config: {
            host: connection.host,
            port: parseInt(connection.port),
            database: connection.database,
            user: connection.user,
            jwtToken: connection.jwt_token
          },
          table: selectedTable || selectedFile.name.split('.')[0],
          columns: selectedColumns,
          filePath: filePath,
          delimiter: fileConfig.delimiter
        });
      }

      if (response.data.success) {
        setMessage({ type: 'success', text: `Successfully processed ${response.data.data?.length || 0} records` });
      }
    } catch (error) {
      const errorMessage = error.response?.data?.error || error.message || 'Failed to process data';
      setMessage({ type: 'error', text: errorMessage });
    }
  });

  const isIngestDisabled = loading || selectedColumns.length === 0 || 
                           (sourceType === 'clickhouse' && (!selectedTable || !fileConfig.file_path)) ||
                           (sourceType === 'file' && !selectedFile);

  const isPreviewDisabled = loading || selectedColumns.length === 0 ||
                            (sourceType === 'clickhouse' && !selectedTable) ||
                            (sourceType === 'file' && !selectedFile);

  const targetTableName = sourceType === 'file' && selectedFile 
                          ? connection.database + "." + selectedFile.name.split('.')[0].replace(/[^a-zA-Z0-9_]/g, '_') 
                          : '[select file]';

  return (
    <Container maxWidth="md">
      <Box sx={{ my: 4 }}>
        <Typography variant="h4" component="h1" gutterBottom>
          ClickHouse {'<>'} Flat File Ingestion
        </Typography>

        {loading && (
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 2, p: 1, bgcolor: 'action.hover', borderRadius: 1 }}>
            <CircularProgress size={20} sx={{ mr: 1 }} />
            <Typography variant="body2">{loadingMessage || 'Loading...'}</Typography>
          </Box>
        )}

        {message.text && (
          <Alert severity={message.type} sx={{ mb: 2 }} onClose={() => setMessage({ type: '', text: '' })}>
            {message.text}
          </Alert>
        )}

        <Paper sx={{ p: 3, mb: 3 }}>
          <Typography variant="h6" gutterBottom>Step 1: Select Source</Typography>
          <FormControl fullWidth sx={{ mb: 2 }}>
            <InputLabel>Source Data Type</InputLabel>
            <Select
              value={sourceType}
              label="Source Data Type"
              onChange={handleSourceTypeChange}
            >
              <MenuItem value="clickhouse">ClickHouse</MenuItem>
              <MenuItem value="file">Flat File (CSV)</MenuItem>
            </Select>
          </FormControl>
          <Typography variant="body1">Target will be: <strong>{targetType === 'clickhouse' ? 'ClickHouse' : 'Flat File'}</strong></Typography>
        </Paper>

        <Paper sx={{ p: 3, mb: 3 }}>
           <Typography variant="h6" gutterBottom>Step 2: Configure {sourceType === 'clickhouse' ? 'ClickHouse Connection' : 'Flat File'}</Typography>
          {sourceType === 'clickhouse' && (
            <>
              <TextField fullWidth name="host" label="Host" value={connection.host} onChange={handleConnectionChange} sx={{ mb: 2 }}/>
              <TextField fullWidth name="port" label="Port" value={connection.port} onChange={handleConnectionChange} sx={{ mb: 2 }}/>
              <TextField fullWidth name="database" label="Database" value={connection.database} onChange={handleConnectionChange} sx={{ mb: 2 }}/>
              <TextField fullWidth name="user" label="User" value={connection.user} onChange={handleConnectionChange} sx={{ mb: 2 }}/>
              <TextField fullWidth name="jwt_token" label="Password/JWT Token" type="password" value={connection.jwt_token} onChange={handleConnectionChange} sx={{ mb: 2 }}/>
              <Button variant="contained" onClick={connectToClickHouse} disabled={loading} sx={{ mb: 2 }}>
                Connect & List Tables
              </Button>

              {tables.length > 0 && (
                <FormControl fullWidth sx={{ mb: 2 }}>
                  <InputLabel>Select Table</InputLabel>
                  <Select value={selectedTable} label="Select Table" onChange={handleTableChange}>
                    {tables.map((table) => (<MenuItem key={table} value={table}>{table}</MenuItem>))}
                  </Select>
                </FormControl>
              )}
            </>
          )}

          {sourceType === 'file' && (
            <>
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <Button
                    component="label"
                    variant="outlined"
                    startIcon={<UploadFileIcon />}
                    disabled={loading}
                  >
                    Select File
                    <input
                      type="file"
                      hidden
                      accept=".csv,.tsv"
                      ref={fileInputRef}
                      onChange={handleFileSelect}
                    />
                  </Button>
                  {selectedFile && <Typography sx={{ ml: 2 }}>{selectedFile.name}</Typography>}
                </Box>
                <TextField
                  name="delimiter"
                  label="Delimiter"
                  value={fileConfig.delimiter}
                  onChange={handleFileConfigChange}
                  size="small"
                  sx={{ width: '100px' }}
                />
              </Box>
            </>
          )}
        </Paper>

        {columns.length > 0 && (
          <Paper sx={{ p: 3, mb: 3 }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
              <Typography variant="h6">Step 3: Select Columns</Typography>
              <Button
                variant="outlined"
                size="small"
                startIcon={<PreviewIcon />}
                onClick={handlePreview}
                disabled={isPreviewDisabled}
              >Preview Data</Button>
            </Box>
            <List dense sx={{ maxHeight: 300, overflow: 'auto', border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
              {columns.map((column) => (
                <ListItem key={column.name} dense button onClick={handleColumnToggle(column)}>
                  <Checkbox
                    edge="start"
                    checked={selectedColumns.some(c => c.name === column.name)}
                    tabIndex={-1}
                    disableRipple
                    size="small"
                  />
                  <ListItemText 
                    primary={column.name}
                    secondary={`Type: ${column.type}${column.nullable ? ' (Nullable)' : ''}`}
                  />
                </ListItem>
              ))}
            </List>
          </Paper>
        )}
        
        <Paper sx={{ p: 3, mb: 3 }}>
          <Typography variant="h6" gutterBottom>Step 4: Configure Target & Start Ingestion</Typography>
          {targetType === 'file' && (
             <TextField fullWidth name="file_path" label="Output File Path (e.g., output.csv)" value={fileConfig.file_path} onChange={handleFileConfigChange} sx={{ mb: 2 }} required={targetType === 'file'} />
          )}
           {targetType === 'clickhouse' && (
             <Box sx={{ border: 1, borderColor: 'divider', p: 2, borderRadius: 1, mb: 2, bgcolor: 'action.hover' }}>
               <Typography variant="body2" gutterBottom> Target ClickHouse Details:</Typography>
               <Typography variant="body2"> Host: {connection.host}</Typography>
               <Typography variant="body2"> Port: {connection.port}</Typography>
               <Typography variant="body2"> Database: {connection.database}</Typography>
               <Typography variant="body2" sx={{mb: 1}}> User: {connection.user}</Typography>
               <Typography variant="body1"> 
                   Table Name: <strong>{targetTableName}</strong> (auto-generated from filename, will be created if needed)
               </Typography>
             </Box>
           )}

          <Button
            variant="contained"
            color="primary"
            onClick={handleStartIngestion}
            disabled={isIngestDisabled}
            fullWidth
            size="large"
          >
            {`Start Ingestion (${sourceType} -> ${targetType})`}
          </Button>
        </Paper>

      </Box>

      <PreviewDialog
        open={isPreviewOpen}
        onClose={() => setIsPreviewOpen(false)}
        columns={previewColumns}
        data={previewData}
      />
    </Container>
  );
}

export default App; 