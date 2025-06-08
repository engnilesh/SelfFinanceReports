#!/bin/bash

# Check for jq dependency
if ! command -v jq >/dev/null 2>&1; then
    echo "Error: jq is not installed. Please install jq to use this script."
    exit 1
fi

kill_port_5000() {
    docker exec "$1" bash -c '
PORT=5000
if command -v lsof >/dev/null 2>&1; then
    PID=$(lsof -t -i:$PORT)
elif command -v netstat >/dev/null 2>&1; then
    PID=$(netstat -tlnp 2>/dev/null | grep ":$PORT " | awk "{print \$7}" | cut -d"/" -f1)
else
    echo "Neither lsof nor netstat found in container. Cannot check port $PORT."
    exit 1
fi
if [ -n "$PID" ]; then
    echo "Killing process on port $PORT"
    kill -9 $PID
fi
' || echo "Error: Failed to kill process on port 5000 in container $1"
}

update_hosts_in_container() {
    local container="$1"
    local ip="$2"
    local hostname="$3"
    local use_sudo="$4"
    local tee_cmd="tee"
    local cp_cmd="cp"
    if [ "$use_sudo" = "yes" ]; then
        tee_cmd="sudo tee"
        cp_cmd="sudo cp"
    fi

    docker exec -e IP="$ip" -u 0 "$container" bash -c "
HOSTNAME=\"$hostname\"
LINE=\"\$IP \$HOSTNAME\"

grep -v '$hostname' /etc/hosts > /tmp/hosts && $cp_cmd /tmp/hosts /etc/hosts

if ! grep -Fxq \"\$LINE\" /etc/hosts; then
  echo \"\$LINE\" | $tee_cmd -a /etc/hosts > /dev/null
  echo \"Added: \$LINE\"
else
  echo \"Entry already exists: \$LINE\"
fi
" || echo "Error: Failed to update /etc/hosts in container $container"
}

connect_containers_to_network() {
    local containers=("$@")
    for c in "${containers[@]}"; do
        if ! docker inspect -f '{{json .NetworkSettings.Networks.hadoop-main_default}}' "$c" 2>/dev/null | grep -q '"IPAddress"'; then
            docker network connect hadoop-main_default "$c" || echo "Error: Failed to connect $c to hadoop-main_default"
        fi
    done
}

update_common_hosts() {
    local SERVICE_LIST="$1"
    local NODE_FINAL NODE_IP PG_FINAL PG_IP

    NODE_FINAL=$(echo "$SERVICE_LIST" | jq -r '.[] | select(has("datanode")) | ."datanode"')
    NODE_IP=$(echo "$NODE_FINAL" | cut -d'/' -f1)
    update_hosts_in_container airflow-airflow-worker-1 "$NODE_IP" "datanode" yes

    PG_FINAL=$(echo "$SERVICE_LIST" | jq -r '.[] | select(has("my-pg-container")) | ."my-pg-container"')
    PG_IP=$(echo "$PG_FINAL" | cut -d'/' -f1)
    update_hosts_in_container my-spark-container "$PG_IP" "my-pg-container" no
}

start_common_services() {
    docker exec my-spark-container bash -c "python3 /home/sparkuser/app/my_API/Flask_API/app.py" &
    if [ $? -ne 0 ]; then echo "Error: Failed to start Flask API in my-spark-container"; fi
    docker exec my-python-container bash -c "python3 /home/commonid/code/my_API/Flask_API/app.py" &
    if [ $? -ne 0 ]; then echo "Error: Failed to start Flask API in my-python-container"; fi
    docker exec datanode bash -c 'mkdir -p /var/run/sshd
/usr/sbin/sshd -D &' || echo "Error: Failed to start sshd in datanode"
}

update_hue_hosts() {
    local SERVICE_LIST="$1"
    local PG_FINAL PG_IP HUE_FINAL HUE_IP

    PG_FINAL=$(echo "$SERVICE_LIST" | jq -r '.[] | select(has("my-pg-container")) | ."my-pg-container"')
    PG_IP=$(echo "$PG_FINAL" | cut -d'/' -f1)
    update_hosts_in_container my-hue-container "$PG_IP" "my-pg-container" yes

    HUE_FINAL=$(echo "$SERVICE_LIST" | jq -r '.[] | select(has("my-hue-container")) | ."my-hue-container"')
    HUE_IP=$(echo "$HUE_FINAL" | cut -d'/' -f1)
    update_hosts_in_container namenode "$HUE_IP" "my-hue-container" no
    update_hosts_in_container resourcemanager "$HUE_IP" "my-hue-container" no
    update_hosts_in_container historyserver "$HUE_IP" "my-hue-container" no
    update_hosts_in_container hive-server "$HUE_IP" "my-hue-container" no
}

if [ "$1" = "with_hue" ]; then

    docker ps --filter "name=my-hue-container" --filter "status=running" | grep my-hue-container || docker start my-hue-container || { echo "Error: Failed to start my-hue-container"; exit 1; }

    sleep 2

    connect_containers_to_network my-hue-container my-spark-container airflow-airflow-worker-1 my-python-container ftp_server

    kill_port_5000 my-spark-container
    kill_port_5000 my-python-container

    JSON_DATA=$(docker inspect hadoop-main_default)
    SERVICE_LIST=$(echo "$JSON_DATA" | jq -c '
      .[] | .Containers | to_entries[] |
      select(.value.Name and .value.IPv4Address) |
      { (.value.Name): .value.IPv4Address }
    ' | jq -s '.')

    update_common_hosts "$SERVICE_LIST"
    update_hue_hosts "$SERVICE_LIST"

    start_common_services

else

    connect_containers_to_network my-spark-container airflow-airflow-worker-1 my-python-container ftp_server

    kill_port_5000 my-spark-container
    kill_port_5000 my-python-container

    JSON_DATA=$(docker inspect hadoop-main_default)
    SERVICE_LIST=$(echo "$JSON_DATA" | jq -c '
      .[] | .Containers | to_entries[] |
      select(.value.Name and .value.IPv4Address) |
      { (.value.Name): .value.IPv4Address }
    ' | jq -s '.')

    update_common_hosts "$SERVICE_LIST"

    start_common_services

fi
