#!/bin/bash

SESSION_ID="restreamer"

# Resolve paths relative to this script's location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_ACTIVATE="${SCRIPT_DIR}/../venv/bin/activate"
APP_DIR="${SCRIPT_DIR}"

# Create a new tmux session
tmux new-session -d -s "$SESSION_ID"

# Create a new window and run htop in it
tmux new-window -t "$SESSION_ID" -n "htop"
tmux send-keys -t "$SESSION_ID" "htop" ENTER

# Create a new window and run the runserver command
tmux new-window -t "$SESSION_ID" -n "runserver"
tmux send-keys -t "$SESSION_ID" "/bin/bash" ENTER
tmux send-keys -t "$SESSION_ID" "source ${VENV_ACTIVATE}" ENTER
tmux send-keys -t "$SESSION_ID" "cd ${APP_DIR}" ENTER
tmux send-keys -t "$SESSION_ID" "python3 manage.py runserver 0.0.0.0:8000 --insecure" ENTER

# Create a new window and run the inpoint command
tmux new-window -t "$SESSION_ID" -n "inpoint"
tmux send-keys -t "$SESSION_ID" "/bin/bash" ENTER
tmux send-keys -t "$SESSION_ID" "source ${VENV_ACTIVATE}" ENTER
tmux send-keys -t "$SESSION_ID" "cd ${APP_DIR}" ENTER
tmux send-keys -t "$SESSION_ID" "python3 manage.py inpoint_service" ENTER

# Create a new window and run the endpoints command
tmux new-window -t "$SESSION_ID" -n "endpoint"
tmux send-keys -t "$SESSION_ID" "/bin/bash" ENTER
tmux send-keys -t "$SESSION_ID" "source ${VENV_ACTIVATE}" ENTER
tmux send-keys -t "$SESSION_ID" "cd ${APP_DIR}" ENTER
tmux send-keys -t "$SESSION_ID" "python3 manage.py endpoints_service" ENTER

# Attach to the tmux session
tmux attach-session -t "$SESSION_ID"
