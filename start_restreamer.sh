#!/bin/bash

SESSION_ID="restreamer"  # Update with your desired session ID

# Create a new tmux session
tmux new-session -d -s "$SESSION_ID"

#z Create a new window and run htop in it
tmux new-window -t "$SESSION_ID" -n "htop"
tmux send-keys -t "$SESSION_ID" "htop" ENTER

# Create a new window and run the first runserver command in it
tmux new-window -t "$SESSION_ID" -n "runserver"
tmux send-keys -t "$SESSION_ID" "/bin/bash" ENTER
tmux send-keys -t "$SESSION_ID" "source .virtualenvs/nl_restreamer/bin/activate" ENTER
tmux send-keys -t "$SESSION_ID" "cd kristian/nl_restreamer" ENTER
tmux send-keys -t "$SESSION_ID" "python3 manage.py runserver 0.0.0.0:8000 --insecure" ENTER

# Create a new window and run the inpoint command in it
tmux new-window -t "$SESSION_ID" -n "inpoint"
tmux send-keys -t "$SESSION_ID" "/bin/bash" ENTER
tmux send-keys -t "$SESSION_ID"  "source .virtualenvs/nl_restreamer/bin/activate" ENTER
tmux send-keys -t "$SESSION_ID" " cd kristian/nl_restreamer" ENTER 
tmux send-keys -t "$SESSION_ID"  "python3 manage.py inpoint_service" ENTER

# Create a new window and run the endpoints command in it
tmux new-window -t "$SESSION_ID" -n "endpoint"
tmux send-keys -t "$SESSION_ID" "/bin/bash" ENTER
tmux send-keys -t "$SESSION_ID" "source .virtualenvs/nl_restreamer/bin/activate" ENTER
tmux send-keys -t "$SESSION_ID" "cd kristian/nl_restreamer" ENTER
tmux send-keys -t "$SESSION_ID" "python3 manage.py endpoints_service" ENTER

# Attach to the tmux session
tmux attach-session -t "$SESSION_ID"




