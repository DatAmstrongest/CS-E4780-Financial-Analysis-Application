#!/bin/sh
# usage: setup.sh elasticsearch | kibana
# elasticsearch: retention policies and index templates for the log data streams.
# kibana: data views, so the logs are browsable in Discover right away.
set -eu

ES=http://elasticsearch:9200
KIBANA=http://kibana:5601

es() {
  curl -fsS -X "$1" "$ES/$2" -H 'Content-Type: application/json' -d "$3"
  echo
}

kibana() {
  curl -fsS -X POST "$KIBANA/$1" -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d "$2"
  echo
}

setup_elasticsearch() {
  until curl -fs "$ES/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null; do sleep 2; done

  # Check retention every minute instead of every 10, so the rejected-rows cap is enforced promptly.
  es PUT _cluster/settings '{"persistent": {"indices.lifecycle.poll_interval": "1m"}}'

  es PUT _ilm/policy/container-logs '{
    "policy": {"phases": {
      "hot": {"actions": {"rollover": {"max_age": "1d", "max_primary_shard_size": "1gb"}}},
      "delete": {"min_age": "3d", "actions": {"delete": {}}}
    }}}'

  # ~11M rejected rows per replayed trading day: keep only the most recent ones so they cannot fill the disk.
  es PUT _ilm/policy/rejected-rows '{
    "policy": {"phases": {
      "hot": {"actions": {"rollover": {"max_primary_shard_size": "200mb"}}},
      "delete": {"min_age": "10m", "actions": {"delete": {}}}
    }}}'

  es PUT _index_template/container-logs '{
    "index_patterns": ["logs-containers-*"],
    "data_stream": {},
    "priority": 500,
    "template": {
      "settings": {
        "index.number_of_shards": 1,
        "index.number_of_replicas": 0,
        "index.lifecycle.name": "container-logs",
        "index.codec": "best_compression"
      },
      "mappings": {
        "dynamic": false,
        "properties": {
          "@timestamp": {"type": "date"},
          "message": {"type": "match_only_text"},
          "level": {"type": "keyword"},
          "component": {"type": "keyword"},
          "service": {"type": "keyword"},
          "container": {"type": "keyword"},
          "container_id": {"type": "keyword"},
          "image": {"type": "keyword"},
          "stream": {"type": "keyword"},
          "producer": {"type": "object", "dynamic": true}
        }
      }
    }}'

  es PUT _index_template/rejected-rows '{
    "index_patterns": ["logs-rejected_rows-*"],
    "data_stream": {},
    "priority": 500,
    "template": {
      "settings": {
        "index.number_of_shards": 1,
        "index.number_of_replicas": 0,
        "index.lifecycle.name": "rejected-rows",
        "index.codec": "best_compression"
      },
      "mappings": {
        "dynamic": false,
        "properties": {
          "@timestamp": {"type": "date"},
          "reason": {"type": "keyword"},
          "message": {"type": "keyword"},
          "file": {"type": "keyword"},
          "line": {"type": "long"},
          "symbol": {"type": "keyword"},
          "sec_type": {"type": "keyword"},
          "date": {"type": "keyword"},
          "time": {"type": "keyword"},
          "last": {"type": "double", "ignore_malformed": true},
          "trading_time": {"type": "keyword"},
          "trading_date": {"type": "keyword"},
          "component": {"type": "keyword"},
          "service": {"type": "keyword"}
        }
      }
    }}'

  # Vector starts before this script and Elasticsearch creates a plain index for writes that arrive
  # before the templates exist. Move those documents into a proper data stream.
  for stream in logs-containers-default logs-rejected_rows-default; do
    if curl -fs "$ES/_data_stream/$stream" | grep -q '"name"'; then
      continue
    fi
    if curl -fs -o /dev/null -I "$ES/$stream"; then
      es POST "_reindex?refresh=true" '{"source": {"index": "'"$stream"'"}, "dest": {"index": "startup-'"$stream"'"}}'
      es DELETE "$stream" ''
      # Vector may already have recreated it as a data stream from the template; either way is fine.
      curl -sS -X PUT "$ES/_data_stream/$stream"
      echo
      es POST _reindex '{"source": {"index": "startup-'"$stream"'"}, "dest": {"index": "'"$stream"'", "op_type": "create"}}'
      es DELETE "startup-$stream" ''
    fi
  done
}

setup_kibana() {
  until curl -fs "$KIBANA/api/status" | grep -q '"level":"available"'; do sleep 5; done

  kibana api/data_views/data_view '{"override": true, "data_view": {
    "id": "container-logs", "name": "Container logs", "title": "logs-containers-*", "timeFieldName": "@timestamp"}}'
  kibana api/data_views/data_view '{"override": true, "data_view": {
    "id": "rejected-rows", "name": "Rejected rows", "title": "logs-rejected_rows-*", "timeFieldName": "@timestamp"}}'
  kibana api/data_views/default '{"data_view_id": "container-logs", "force": true}'
}

case "${1:-}" in
  elasticsearch) setup_elasticsearch ;;
  kibana) setup_kibana ;;
  *) echo "usage: $0 elasticsearch|kibana" >&2; exit 1 ;;
esac
