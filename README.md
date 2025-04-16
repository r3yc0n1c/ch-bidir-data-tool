# ClickHouse Data Ingestion Tool

A web-based application for bidirectional data ingestion between ClickHouse and Flat Files.

## Features

- Bidirectional data flow (ClickHouse ↔ Flat File)
- JWT token-based authentication for ClickHouse
- Column selection for data ingestion
- Progress tracking and error handling
- Modern, responsive UI

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Node.js 14+
- npm or yarn

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd clickhouse-data-ingestion
```

2. Start ClickHouse using Docker:
```bash
docker-compose up -d
```

3. Set up the backend:
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

4. Set up the frontend:
```bash
cd frontend
npm install
npm start
```

## Usage

1. Open your browser and navigate to `http://localhost:3000`
2. Select the source type (ClickHouse or Flat File)
3. Configure the connection parameters:
   - For ClickHouse: Host, Port, Database, User, and JWT Token
   - For Flat File: File path and delimiter
4. Connect to the source and select tables/columns
5. Start the ingestion process
6. Monitor the progress and view results

## API Endpoints

- `POST /connect/clickhouse`: Connect to ClickHouse and list tables
- `POST /tables/{table_name}/columns`: Get columns for a specific table
- `POST /ingest/clickhouse-to-file`: Export data from ClickHouse to file
- `POST /ingest/file-to-clickhouse`: Import data from file to ClickHouse

## Error Handling

The application provides clear error messages for:
- Connection failures
- Authentication issues
- Invalid configurations
- Data ingestion errors

## Security Considerations

- JWT tokens are used for ClickHouse authentication
- All sensitive data is handled securely
- CORS is properly configured for development

## Testing

To test the application:
1. Use the ClickHouse example datasets (uk_price_paid, ontime)
2. Test both directions of data flow
3. Verify column selection functionality
4. Test error handling scenarios

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License 