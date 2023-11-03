#!/bin/bash

# Start the pgAdmin service
/entrypoint.sh

# Run your Python script
python /app/create_pgadmin_server.py

# Keep the container running
tail -f /dev/null
