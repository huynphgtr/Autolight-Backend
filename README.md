# AUTOLIGHT BACKEND

A FastAPI-based backend system for intelligent automatic lighting control with priority-based decision logic, MQTT device communication, and smart area management.

## Overview

AUTOLIGHT Backend is a modern REST API that controls and monitors intelligent lighting systems in areas/zones. It implements a priority-based lighting decision engine that considers manual overrides, scheduled operations, and auto AI decisions based on occupancy (person count) and ambient light (lux) sensors.

## Features

- **Intelligent Lighting Control** - Priority-based decision making (Manual Override > Schedule > Auto AI)
- **Person Detection & Lux Sensing** - Automatic lights based on occupancy and brightness
- **Schedule Management** - Time-based automated lighting automation
- **Manual Override Control** - Temporary override of automatic decisions
- **Area Management** - Organize and control lights by areas/zones
- **Device Management** - Manage IoT lighting devices by IP address
- **MQTT Service** - Real-time IoT device communication
- **Configuration & Thresholds** - Customizable min_person, lux_threshold, and off_delay per area
- **CORS Support** - Cross-origin requests enabled for frontend integration

## Tech Stack

- **Framework**: FastAPI (Python web framework)
- **Database**: SQLite with SQLAlchemy ORM
- **Communication**: MQTT Protocol for IoT devices
- **API Documentation**: Swagger UI / ReDoc (auto-generated)
- **Async Support**: Async/await for non-blocking operations

## Project Structure

```
BACKEND/
├── app/
│   ├── main.py                      # FastAPI app initialization & routes
│   ├── api/
│   │   ├── api.py                   # API router configuration
│   │   ├── deps.py                  # Dependency injection
│   │   └── endpoints/
│   │       └── area.py              # Area management endpoints
│   ├── core/
│   │   ├── config.py                # Configuration settings
│   │   ├── device_controller.py     # Device control logic
│   │   └── lighting_controller.py   # Priority-based lighting decision engine
│   ├── database/
│   │   ├── db.py                    # Database initialization
│   │   └── repositories/
│   │       ├── area_repository.py   # Area data access layer
│   │       └── device_repository.py # Device data access layer
│   ├── models/                      # SQLAlchemy ORM models
│   └── services/
│       ├── mqtt_service.py          # MQTT device communication
│       └── simulator.py             # Device simulator for testing
├── scripts/
│   ├── create_schema_sqlite.py      # Database schema creation
│   └── seed_data.py                 # Sample data seeding
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

## Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- MQTT broker (Mosquitto recommended)
- SQLite3 (usually included with Python)

### Setup

1. **Clone/Navigate to the project**
   ```bash
   cd BACKEND
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

   - Database will use SQLite (auto-created as `app.db` in project root)
   - Set up database connection in `app/core/config.py`
   - Configure MQTT broker address
   - Set up JWT secret keys for authentication

## Running the Server

### Development

```bash
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

The API will be available at `http://localhost:8000`

**Interactive API Documentation:**
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Running the Device Simulator

For testing without real MQTT devices:

```bash
python ./app/services/simulator.py
```

This simulates device inputs (person count, lux values) and tests the lighting decision logic.

## API Endpoints

### Area Management
- `GET /api/areas` - List all areas
- `GET /api/areas/status` - List all areas with their current status, mode, and config
- `POST /api/areas` - Create area with base configuration
- `GET /api/areas/{area_id}` - Get area details
- `PUT /api/areas/{area_id}` - Update area definition
- `PUT /api/areas/{area_id}/config` - Update area AI thresholds (min_person, lux_threshold, off_delay, override_timeout)
- `DELETE /api/areas/{area_id}` - Delete area
- `GET /api/areas/{area_id}/history` - Get lighting decision history logs for the area

### Lighting Decision & Control
- `POST /api/areas/{area_id}/decide` - Get lighting decision for area (person_count, lux input)
- `POST /api/areas/{area_id}/control` - Send control command to devices in area
- `POST /api/areas/{area_id}/manual` - Set temporary manual override (P1) for an area (ON/OFF)

## Key Services

### LightingController
The core decision engine that implements the priority-based lighting logic:

**Priority Levels (Highest to Lowest):**
1. **P1 - Manual Override**: If override is active, preserve current state (NOOP)
2. **P2 - Schedule**: If an active schedule exists, follow its action_state
3. **P3 - Auto AI**: Use person_count and lux compared to area config:
   - `min_person`: Minimum occupancy required for lights on
   - `lux_threshold`: Maximum brightness threshold (lights on if lux < threshold)
   - `off_delay`: Seconds to wait before turning off (when conditions not met)

**Decision Actions:**
- `ON` - Turn lights on
- `OFF` - Turn lights off immediately
- `OFF_DELAYED` - Turn off after off_delay seconds
- `NOOP` - Keep current state (override or not applicable)

### MQTT Service
Handles IoT device communication for real-time light control.
- Connects to configured MQTT broker
- Publishes control commands to devices
- Receives status updates from devices

### Device Simulator
Test device that simulates person detection and lux sensor inputs without real hardware.

## Database Models

- **Area** - Lighting zones with configuration (min_person, lux_threshold, off_delay, override_timeout)
- **Device** - IoT lighting devices identified by IP address
- **Schedule** - Time-based lighting automation rules per area
- **Override** - Manual control overrides with expiration time
- **HistoryLog** - Records of lighting decisions and system events for auditing

## CORS Configuration

The API allows requests from:
- `http://localhost:5173` (Frontend dev server)
- `http://127.0.0.1:5173`
- `http://localhost:3000`

Update the CORS configuration in [app/main.py](app/main.py) if additional origins are needed.

## Environment Variables

Configure these in your environment or `.env` file:
- `DATABASE_URL` - Database connection string (default: `sqlite:///./app.db`)
- `MQTT_BROKER` - MQTT broker address (e.g., `localhost`)
- `MQTT_PORT` - MQTT broker port (e.g., `1883`)
- `CLIENT_ID` - Client ID broker (e.g, `autolight_backend_test`
- `KEEPALIVE` - Time to keep alive (e.g., `60`)

**Example .env file:**
```
DATABASE_URL=sqlite:///./app.db
MQTT_BROKER=localhost
MQTT_PORT=1883
CLIENT_ID=autolight_backend_test
KEEPALIVE=60
```

## Utilities

### Database Setup Scripts

**Create Schema:**
```bash
python scripts/create_schema_sqlite.py
```

**Seed Sample Data:**
```bash
python scripts/seed_data.py
```

## Development

### Code Style
- Follow PEP 8 conventions
- Use type hints for better code documentation
- Write descriptive docstrings

### Adding New Endpoints
1. Create route handler in `app/api/endpoints/`
2. Add route to main API router in `app/api/api.py`
3. Implement business logic in services or controllers

### Database Setup
The SQLite database file (`app.db`) will be automatically created on first run. To reset:
```bash
rm app.db  # Remove existing database
# Restart the server to create a fresh database
```

## Troubleshooting

### MQTT Connection Issues
- Verify MQTT broker is running and accessible
- Check `MQTT_BROKER` and `MQTT_PORT` configuration
- Test with: `mosquitto_sub -h localhost -t "test"` (if mosquitto-clients installed)

### Database Connection Errors
- Verify `DATABASE_URL` is correctly configured
- Ensure `app.db` file exists or can be created in project root
- Check file permissions on database directory

### Lighting Decision Issues
- Verify area configuration exists (min_person, lux_threshold, off_delay)
- Check device IP address is correctly registered
- Review logs for priority decision flow (override > schedule > auto)

### CORS Errors
- Add frontend URL to allowed origins in [app/main.py](app/main.py)
- Verify frontend sends requests with correct headers
- Check browser console for specific CORS error details

## Performance Considerations

- MQTT messages are processed asynchronously
- Database queries use SQLAlchemy ORM with efficient relationships
- Area lookups by device IP are optimized
- Priority decision logic runs in memory without I/O delays

## Security

- Input validation using Pydantic schemas
- SQL injection prevention through SQLAlchemy ORM
- CORS protection enabled for cross-origin requests
- Secure configuration management via environment variables

## Contributing

When contributing to the backend:
1. Follow the existing code structure and conventions
2. Test changes with both the API and simulator
3. Update documentation for new features
4. Ensure MQTT device communication is properly tested

## Support

For issues or questions:
- Check API documentation at `/docs` (Swagger UI)
- Review application logs for error details
- Test lighting decisions using the `/api/areas/{area_id}/decide` endpoint

## License

MIT License - See LICENSE file for details

## Changelog

### Version 1.0.0
- Initial release
- Priority-based lighting decision engine
- Area and device management
- MQTT integration for device control
- Schedule and override support
- Device simulator for testing
