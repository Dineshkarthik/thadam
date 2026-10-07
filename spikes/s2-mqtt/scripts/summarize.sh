#!/usr/bin/env bash
# Print compact TSV tables from results/<broker>/*.jsonl (requires jq).
#   scripts/summarize.sh mosquitto > results/mosquitto/SUMMARY.txt
set -euo pipefail
B="${1:?broker}"
D="$(cd "$(dirname "$0")/.." && pwd)/results/$B"

echo "## q1 throughput (inflight latency auto recv/s wire_ack/s nominal_ceiling ratio ack_delay_mean_us max_unacked min_backlog supply_limited connacks errors)"
jq -r 'select(.test=="throughput") | [.broker_inflight, .latency_ms, (.auto_ack|tostring), (.recv_per_s|round), (.wire_ack_per_s|round), (.predicted_ceiling_nominal // "-"), ((.ratio_to_nominal // 0) * 100 | round / 100), (.ack_delay_mean_us|round), .max_unacked_observed, .min_backlog_in_window, .supply_limited, .connacks, .poll_errors] | @tsv' "$D/q1.jsonl"
jq -r 'select(.test=="rtt_publish_to_puback") | "rtt p50_us=\(.p50_us) p90_us=\(.p90_us) p99_us=\(.p99_us) min_us=\(.min_us)"' "$D/q1.jsonl"

echo; echo "## q2 keepalive"
jq -r '[.mode, "inflight=\(.broker_inflight)", "qcap=\(.queue_cap)", "ka=\(.keepalive_s)", "hold=\(.hold_s)", "tcp_conns=\(.tcp_connections_seen_by_proxy)", "connacks=\(.client_connacks)", "errs=\(.poll_errors)", "closes=\(.broker_side_closes|tostring)", "close_after_last_client_pkt_ms=\(.broker_close_ms_after_last_client_packet)", "pingreq_in_hold=\(.pingreq_on_wire_during_hold)", "pingresp_in_hold=\(.pingresp_during_hold)", "acks_issued=\(.acks_issued_by_consumer)", "liveness_blocked_at_end=\(.liveness_task_blocked_in_queue_send_at_end)", "consumer_blocked_in_ack_at_end=\(.consumer_blocked_in_client_ack_at_end)", "max_unacked=\(.max_unacked_observed)", "rx_at_hold_end=\(.received_at_hold_end)", "broker_pub_pkts_in_hold=\(.broker_publish_packets_during_hold)", "published=\(.published_total)", "unique=\(.unique_delivered)", "dups=\(.duplicate_deliveries)", "missing=\(.missing)"] | @tsv' "$D/q2.jsonl"
echo "-- q2 events"
jq -r '"[\(.mode) inflight=\(.broker_inflight) qcap=\(.queue_cap)] " + ([.events[] | "\(.[0])ms:\(.[1])"] | join(" | "))' "$D/q2.jsonl"
echo "-- q2 timeline (s,received,unacked) for first run per inflight"
jq -r 'select(.mode=="withhold" and .queue_cap==0) | "inflight=\(.broker_inflight): " + ([.timeline_s_received_unacked[] | "\(.[0])s:\(.[1])/\(.[2])"] | join(" "))' "$D/q2.jsonl"

echo; echo "## q3 cancel-safety"
jq -r '[.style, "max_us=\(.max_us)", "from_start=\(.from_start)", "published=\(.published)", "unique=\(.unique_delivered)", "missing=\(.missing_count)", "nondup_dups=\(.non_dup_duplicates)", "dupflag=\(.dup_flagged_deliveries)", "cancelled=\(.poll_cancelled)", "completed=\(.poll_completed)", "connacks=\(.connacks)", "errs=\(.poll_errors)", "pingresp=\(.pingresp)", "wire_acks=\(.wire_acks_emitted)", "redelivered_on_verify=\(.verify_redelivered_after_reconnect)", "elapsed=\(.elapsed_s|round)s"] | @tsv' "$D/q3.jsonl"

if [ -f "$D/q3-stall.jsonl" ]; then
  echo; echo "## q3-stall cancel while write side blocked"
  jq -r '[.style, "cancelled_polls=\(.poll_cancelled)", "errors=\(.poll_errors)", "ack_events=\(.outgoing_puback_events_emitted_by_rumqttc)", "acks_on_wire=\(.puback_packets_on_wire_at_proxy)", "acks_lost=\(.acks_lost_between_event_and_wire)", "filler_events=\(.outgoing_publish_events_emitted)", "filler_on_wire=\(.qos0_filler_packets_on_wire)", "fillers_lost=\(.fillers_lost_between_event_and_wire)", "unique_rx=\(.unique_received)", "dups=\(.duplicate_deliveries)", "redelivered_to_fresh_session=\(.redelivered_to_fresh_session_after_run)", "connacks=\(.connacks)"] | @tsv' "$D/q3-stall.jsonl"
fi

echo; echo "## q4 strip"
jq -r '[.mode, "qos=\(.qos)", "payload_kib=\(.payload_kib)", "count=\(.count)", "held_peak=\(.held_unacked_at_peak)", "rss_delta_mib=\(.rss_delta_mib * 10 | round / 10)", "payload_held_if_retained_mib=\(.payload_bytes_held_if_retained_mib)", "unique=\(.unique_delivered)", "dups=\(.duplicate_deliveries)", "puback=\(.wire_puback_c2s)", "pubrec=\(.wire_pubrec_c2s)", "pubcomp=\(.wire_pubcomp_c2s)", "in_order=\(.order.acks_in_receive_order)", "unmatched=\(.order.unmatched_acks)", "redelivered_on_verify=\(.verify_redelivered_after_reconnect)", "session_present=\(.verify_session_present)"] | @tsv' "$D/q4.jsonl"

echo; echo "## q6 order"
jq -r '[.mode, "count=\(.count)", "received=\(.received)", "acks_issued=\(.acks_issued)", "wire_acks=\(.wire.acks_seen)", "in_receive_order=\(.wire.acks_in_receive_order)", "descents=\(.wire.descents)", "unmatched=\(.wire.unmatched_acks)", "broker_closed=\(.broker_closed_connection_during_run)"] | @tsv' "$D/q6.jsonl"

if [ -f "$D/q5.jsonl" ]; then
  echo; echo "## q5 persistent session"
  jq -c 'if .test=="sess_report" then {label,expected,unique_delivered_overall,missing_count,missing_ranges_first10,logs:[.logs[]|{log,session_present_in_connack,deliveries,dup_flagged_deliveries,dup_flagged_seq_range,unique_seqs,seq_range,acks_issued}]} else . end' "$D/q5.jsonl"
fi
if [ -f "$D/e1.jsonl" ]; then
  echo; echo "## e1 stale ack after session loss"
  jq -c '.' "$D/e1.jsonl"
fi

if [ -f "$D/v5.jsonl" ]; then
  echo; echo "## v5"
  jq -c '.' "$D/v5.jsonl"
fi
