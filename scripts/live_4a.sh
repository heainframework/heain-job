#!/usr/bin/env bash
# heain-job live test 4a: heain-job rebuilt on heain-sdk v1, against a real
# heain-core node, with two instances of the example module heain-textmod.
#  - an orchestration job is split by the AI planner (reasoning record),
#    the module's split runs, the sub-units go through core (P1-P4) to both
#    module instances, a failed sub-unit is retried by core, the module's
#    merge returns the result;
#  - the planner learns from finished sub-units and keeps what it learned
#    across a restart;
#  - a transactional job is refused (core still retries sub-units).
# Every app is a plain process configured through HEAIN_* variables.
# Needs ~/heain-core and ~/heain-sdk next to this repo. ~2 min.
# Run from ~/heain-job:  bash scripts/live_4a.sh
set -uo pipefail
JOB=$(cd "$(dirname "$0")/.." && pwd)
cd ~/heain-core || { echo "needs ~/heain-core"; exit 1; }
H=./test_1_2_live.sh
T=$HOME/heain-core/.test-1.2
C=$T/certs; L=$T/logs; P=$T/pids; BIN=$T/node; W=$T/job-4a
URL=https://127.0.0.1:18000
PASS=0; FAIL=0
ok()  { echo "  PASS: $*"; PASS=$((PASS+1)); }
bad() { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }
as() { local who=$1; shift; curl -sk --cert "$C/$who.pem" --key "$C/$who.key" --cacert "$C/ca.pem" "$@"; }
code() { local who=$1; shift; as "$who" -o /dev/null -w "%{http_code}" "$@"; }
j() { python3 -c "import json,sys;d=json.load(sys.stdin);print($1)" 2>/dev/null; }
mkcert() { [ -f "$C/$1.pem" ] && return; openssl genrsa -out "$C/$1.key" 2048 >/dev/null 2>&1
  openssl req -new -key "$C/$1.key" -subj "/CN=$1" -out "$C/$1.csr" >/dev/null 2>&1
  openssl x509 -req -in "$C/$1.csr" -CA "$C/ca.pem" -CAkey "$C/ca.key" -CAcreateserial -out "$C/$1.pem" \
    -days 825 -sha256 -extfile <(printf "subjectAltName=DNS:%s" "$1") >/dev/null 2>&1; }
aud() { as admin "$URL/v1/admin/audit?limit=5000" | python3 -c "
import json,sys
E=[r['event'] for r in json.load(sys.stdin)['records']]
R=[e['Detail']['record'] for e in E if e['Action']=='ai.reasoning_record' and e['Actor']=='heain-job.j1']
def fac(r,n): return next((f['value'] for f in r['reasoning']['factors'] if f['name']==n),None)
print($1)" 2>/dev/null; }
# run <name> <instance> <manifest> <port> <binary> [args...]: an app as a plain process, HEAIN_* contract
run() { local name=$1 inst=$2 man=$3 port=$4; shift 4
  as admin -X POST -H 'Content-Type: application/json' -d "{\"label\":\"$name.$inst\"}" $URL/provision/token > "$W/$inst.tok" 2>/dev/null
  mkdir -p "$W/state-$inst"
  HEAIN_MANIFEST=$man HEAIN_INSTANCE=$inst HEAIN_CORE_URL=$URL HEAIN_CORE_ID=G HEAIN_CA=$C/ca.pem HEAIN_CHAIN=$W/prov.pem \
  HEAIN_STATE_DIR=$W/state-$inst HEAIN_ENROLL_TOKEN=$W/$inst.tok HEAIN_ENDPOINT_BASE=https://127.0.0.1:$port HEAIN_LISTEN=127.0.0.1:$port \
    nohup "$@" >> "$W/$inst.log" 2>&1 &
  echo $! > "$P/$inst.pid"; }
approve() { for i in $(seq 1 30); do
    for a in $(as approver-1 $URL/v1/admin/policy/pending | j "' '.join(x['ID'] for x in d['actions'] if x['Type']=='app.register')"); do
      code approver-1 -X POST $URL/v1/admin/policy/$a/approve >/dev/null; done
    [ "$(as admin $URL/v1/admin/apps | python3 -c "
import json,sys
n=[0]
def walk(x):
    if isinstance(x,dict):
        if x.get('status')=='active' and x.get('instance_id') in '$1'.split(): n[0]+=1
        for v in x.values(): walk(v)
    elif isinstance(x,list):
        for v in x: walk(v)
walk(json.load(sys.stdin)); print(n[0])" 2>/dev/null)" = "$(echo $1 | wc -w)" ] && return 0; sleep 1; done; return 1; }
submit() { HEAIN_MANIFEST=$JOB/cmd/heain-job-submit/heain-app.yaml HEAIN_INSTANCE=c1 HEAIN_CORE_URL=$URL HEAIN_CORE_ID=G HEAIN_CA=$C/ca.pem \
  HEAIN_CHAIN=$W/prov.pem HEAIN_STATE_DIR=$W/state-c1 HEAIN_ENROLL_TOKEN=$W/c1.tok timeout 300 "$W/submit" "$@" 2>>"$W/c1.log" | grep -a '^JOB\|^ERROR\|^REFUSED'; }

echo "== 0. core node G (P1-P5, provisioning); build heain-job, heain-textmod, heain-job-submit"
$H clean >/dev/null; $H build >/dev/null || { echo "core build failed"; exit 1; }; $H certs >/dev/null
mkdir -p "$L" "$P" "$T/data-G" "$W"; for c in admin approver-1; do mkcert $c; done
openssl genrsa -out "$W/prov.key" 2048 >/dev/null 2>&1
openssl req -new -key "$W/prov.key" -subj "/CN=heain-test-provisioning-ca" -out "$W/prov.csr" >/dev/null 2>&1
openssl x509 -req -in "$W/prov.csr" -CA "$C/ca.pem" -CAkey "$C/ca.key" -CAcreateserial -out "$W/prov.pem" -days 30 -sha256 \
  -extfile <(printf "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign") >/dev/null 2>&1
: > "$L/G.log"
nohup "$BIN" -node-id=G -tier=ZONE -raft-addr=127.0.0.1:19000 -data-dir="$T/data-G" -http-addr=127.0.0.1:18000 \
  -cert="$C/G.pem" -key="$C/G.key" -ca="$C/ca.pem" -admin-node-id=admin -approver-ids=approver-1 -bootstrap=true \
  -ingest-queue-path="$T/data-G/queue.db" -staging-path="$T/data-G/staging.db" -approval-store-path="$T/data-G/approvals.db" \
  -provision-ca-cert="$W/prov.pem" -provision-ca-key="$W/prov.key" >> "$L/G.log" 2>&1 &
echo $! > "$P/G.pid"; sleep 6
( cd "$JOB" && GOFLAGS= GOWORK=${SDK_GOWORK:-} go build -o "$W/heain-job" ./cmd/heain-job && GOFLAGS= GOWORK=${SDK_GOWORK:-} go build -o "$W/textmod" ./examples/textmod \
  && GOFLAGS= GOWORK=${SDK_GOWORK:-} go build -o "$W/submit" ./cmd/heain-job-submit ) && ok "heain-job, heain-textmod and heain-job-submit build" || { bad "build"; exit 1; }
for k in $(seq 1 15); do [ "$(as admin -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{"label":"probe.x"}' $URL/provision/token)" = 200 ] && break; sleep 1; done

echo "== 1. the apps start (plain processes) and are admitted"
run heain-textmod t1 "$JOB/examples/textmod/heain-app.yaml" 19453 "$W/textmod" -delay 2s
run heain-textmod t2 "$JOB/examples/textmod/heain-app.yaml" 19454 "$W/textmod" -delay 2s -fail-first 1
run heain-job j1 "$JOB/heain-app.yaml" 19455 "$W/heain-job" -target-unit-seconds 0.0001
as admin -X POST -H 'Content-Type: application/json' -d '{"label":"heain-job-client.c1"}' $URL/provision/token > "$W/c1.tok"
approve "t1 t2 j1" && ok "heain-textmod t1, t2 and heain-job j1 admitted (P5 app.register)" || bad "admission: $(tail -2 "$W/j1.log")"
printf 'line one\nline two\nline three\nline four\nline five\nline six\nline seven\nline eight\n' > "$W/in1.txt"
( sleep 3; approve "c1" ) >/dev/null 2>&1 &

echo "== 2. one job, fanned out across both module instances"
r=$(submit -cap text.upper -file "$W/in1.txt")
echo "$r" | grep -q "state=delivered" && [ "$(echo "$r" | sed 's/.*out=//')" = "LINE ONE" ] && ok "job delivered (merged output starts with LINE ONE)" || bad "job 1: $r"
OUT=$(echo "$r" | sed 's/.*out=//')
TK=$(echo "$r" | awk '{print $2}')
[ "$(aud "R[0]['output']['value']['parts'] if R else 0")" = 2 ] && [ "$(aud "int(fac(R[0],'live_workers'))")" = 2 ] && ok "AI planner: no history, 2 live workers -> 2 sub-units (signed reasoning record)" || bad "record 1: $(aud "R[0]['output'] if R else R")"
grep -q "UNIT" "$W/t1.log" && grep -q "UNIT" "$W/t2.log" && ok "sub-units ran on both module instances (t1 and t2)" || bad "spread: t1=$(grep -c UNIT "$W/t1.log") t2=$(grep -c UNIT "$W/t2.log")"
grep -q "failing sub-unit" "$W/t2.log" && [ "$(as admin "$URL/v1/admin/audit?limit=5000" | j "sum(1 for x in d['records'] if x['event']['Action']=='app.job_failed')")" -ge 1 ] && ok "a failed sub-unit was retried by core and the job still completed" || bad "retry"
[ "$(as admin "$URL/v1/admin/audit?limit=5000" | j "sorted(set(x['event']['Detail']['capability'] for x in d['records'] if x['event']['Action']=='app.event' and x['event']['Actor']!='heain-job.j1' and x['event']['Detail'].get('app_actor')=='heain-job.j1'))")" = "['text.upper.merge', 'text.upper.split']" ] \
  && ok "the module audited split and merge as calls from heain-job.j1 (uses[] pattern *.split/*.merge)" || bad "split/merge audit: $(as admin "$URL/v1/admin/audit?limit=5000" | j "[(x['event']['Actor'],x['event']['Detail'].get('capability'),x['event']['Detail'].get('app_actor')) for x in d['records'] if x['event']['Action']=='app.event'][:8]")"

echo "== 3. the planner learns, and keeps it across a restart"
for i in $(seq 1 40); do echo "more line $i"; done > "$W/in2.txt"
r2=$(submit -cap text.upper -file "$W/in2.txt")
echo "$r2" | grep -q "state=delivered" && [ "$(aud "fac(R[-1],'observations')")" != 0.0 ] && ok "second job: the decision uses $(aud "int(fac(R[-1],'observations'))") observed sub-units ($(aud "R[-1]['reasoning']['summary'][:60]"))" || bad "learning: $r2 / $(aud "R[-1]['reasoning'] if R else R")"
N2=$(aud "int(fac(R[-1],'observations'))")
kill -TERM "$(cat "$P/j1.pid")"; sleep 2
grep -q "deregistered" "$W/j1.log" && [ -s "$W/state-j1/planner-stats.json" ] && ok "heain-job stopped gracefully; learned state on disk (numbers only)" || bad "stop/state"
run heain-job j1 "$JOB/heain-app.yaml" 19455 "$W/heain-job" -target-unit-seconds 0.0001
sleep 6
r3=$(submit -cap text.upper -file "$W/in1.txt")
[ "$(aud "int(fac(R[-1],'observations'))")" -ge "$N2" ] && echo "$r3" | grep -q "state=delivered" && ok "after a restart the planner still knows $(aud "int(fac(R[-1],'observations'))") observations" || bad "after restart: $r3"
[ "$(aud "R[0]['model']['artifact_sha256'] != R[-1]['model']['artifact_sha256']")" = True ] && ok "the record's model hash changes as the planner learns" || bad "model hash"
as admin "$URL/v1/admin/audit?limit=5000" | grep -q "line one" && bad "raw job input reached core's audit" || ok "no raw input in core's audit (records hash the input)"

echo "== 4. refusals"
r4=$(submit -cap text.upper -file "$W/in1.txt" -resume transactional)
echo "$r4" | grep -q "state=retries_exhausted" && [ "$(as admin "$URL/v1/admin/audit?limit=5000" | j "sum(1 for x in d['records'] if x['event']['Action']=='app.event' and 'transactional' in str(x['event']['Detail']))")" -ge 1 ] \
  && ok "a transactional job is refused, not fanned out (core still retries sub-units)" || bad "transactional: $r4"
r5=$(submit -cap no.such.module -file "$W/in1.txt")
echo "$r5" | grep -q "state=retries_exhausted" && ok "a capability no module offers is refused" || bad "unknown module: $r5"
[ "$(as admin "$URL/v1/admin/audit/verify" | j "d['ok']")" = True ] && ok "core audit chain verifies" || bad "audit verify"

echo "== cleanup"
for f in t1 t2 j1; do kill -TERM "$(cat "$P/$f.pid")" 2>/dev/null; done; sleep 2
$H stop-all >/dev/null 2>&1
echo
echo "RESULT: $PASS passed, $FAIL failed"
