"""Local monitor for the AgentCore multi-agent POC.

    python tools/monitor.py            # then open http://localhost:8000

Uses your saved AWS login (no extra setup, standard library + boto3 only) and listens on
127.0.0.1 only. Lets you invoke the agent, see the per-agent latency/token breakdown and
reviewer score, follow the runtime's CloudWatch logs, and see runtime/endpoint status.
"""
import json
import os
import sys
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "deploy"))
from common import REGION, control, data  # noqa: E402

PORT = int(os.getenv("MONITOR_PORT", "8000"))
ALL_RUNTIMES = {"dev": "research_orchestrator_dev", "prod": "research_orchestrator"}
# MONITOR_ENV=prod locks the page (and the API) to the prod endpoint only.
LOCK = os.getenv("MONITOR_ENV")
RUNTIMES = {LOCK: ALL_RUNTIMES[LOCK]} if LOCK else ALL_RUNTIMES
logs = boto3.client("logs", region_name=REGION, config=Config(read_timeout=30))


def runtime(env: str) -> dict:
    name = RUNTIMES[env]
    for r in control().list_agent_runtimes().get("agentRuntimes", []):
        if r["agentRuntimeName"] == name:
            return r
    raise LookupError(f"runtime {name} not found")


def api_status() -> dict:
    out = {}
    for env in RUNTIMES:
        try:
            r = runtime(env)
            eps = control().list_agent_runtime_endpoints(agentRuntimeId=r["agentRuntimeId"]).get("runtimeEndpoints", [])
            out[env] = {"name": r["agentRuntimeName"], "id": r["agentRuntimeId"], "status": r["status"],
                        "version": r.get("agentRuntimeVersion"),
                        "endpoints": [{"name": e["name"], "status": e["status"],
                                       "live": e.get("liveVersion")} for e in eps]}
        except Exception as exc:  # show the problem in the page instead of failing
            out[env] = {"error": str(exc)}
    return out


def api_logs(env: str, minutes: int) -> dict:
    group = f"/aws/bedrock-agentcore/runtimes/{runtime(env)['agentRuntimeId']}-{'prod' if env == 'prod' else 'DEFAULT'}"
    since = int((time.time() - minutes * 60) * 1000)
    try:
        streams = logs.describe_log_streams(logGroupName=group, orderBy="LastEventTime", descending=True, limit=5)
        events = []
        for s in streams.get("logStreams", []):
            events += logs.get_log_events(logGroupName=group, logStreamName=s["logStreamName"],
                                          startTime=since, limit=100, startFromHead=False)["events"]
    except logs.exceptions.ResourceNotFoundException:
        return {"group": group, "lines": []}
    events.sort(key=lambda e: e["timestamp"])
    return {"group": group, "lines": [
        {"t": time.strftime("%H:%M:%S", time.localtime(e["timestamp"] / 1000)), "m": e["message"].rstrip()[:600]}
        for e in events[-200:]]}


def api_invoke(body: dict) -> dict:
    env = body.get("env") or LOCK or "dev"
    if env not in RUNTIMES:
        raise LookupError(f"environment {env} is not enabled on this monitor")
    r = runtime(env)
    session = body.get("session") or f"session-{uuid.uuid4()}"
    started = time.time()
    resp = data().invoke_agent_runtime(
        agentRuntimeArn=r["agentRuntimeArn"],
        qualifier="prod" if env == "prod" else "DEFAULT",
        runtimeSessionId=session,
        payload=json.dumps({"prompt": body["prompt"], "actor_id": body.get("actor") or "monitor-user"}).encode(),
        contentType="application/json", accept="application/json")
    result = json.loads(resp["response"].read())
    result["session_id"] = session
    result["env"] = env
    result["wall_s"] = round(time.time() - started, 1)
    return result


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, payload, ctype="application/json"):
        raw = payload if isinstance(payload, bytes) else (
            payload.encode() if isinstance(payload, str) else json.dumps(payload, default=str).encode())
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        try:
            if u.path == "/":
                self._send(200, render_page(), "text/html")
            elif u.path == "/api/status":
                self._send(200, api_status())
            elif u.path == "/api/logs":
                self._send(200, api_logs(q.get("env", "dev"), int(q.get("minutes", "15"))))
            else:
                self._send(404, {"error": "not found"})
        except Exception as exc:
            self._send(500, {"error": f"{type(exc).__name__}: {exc}"})

    def do_POST(self):
        try:
            if self.path != "/api/invoke":
                return self._send(404, {"error": "not found"})
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            if not body.get("prompt"):
                return self._send(400, {"error": "prompt is required"})
            self._send(200, api_invoke(body))
        except Exception as exc:
            self._send(500, {"error": f"{type(exc).__name__}: {exc}"})

    def log_message(self, *a):  # quiet
        pass


PAGE = r"""<!doctype html><html><head><meta charset="utf-8"><title>AgentCore Monitor @@TAG@@</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{--bg:#f6f7f9;--card:#fff;--ink:#1c2430;--mute:#667085;--line:#e3e6eb;--acc:#2f5fd0;--ok:#1a7f4b;--bad:#b42318}
@media(prefers-color-scheme:dark){:root{--bg:#12161c;--card:#1b212b;--ink:#e8ebf0;--mute:#98a2b3;--line:#2b3340;--acc:#7aa2ff;--ok:#4cc38a;--bad:#ff7b72}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 system-ui,sans-serif;padding:16px}
h1{font-size:18px;margin:0 0 12px}h2{font-size:14px;margin:0 0 8px;color:var(--mute);text-transform:uppercase;letter-spacing:.04em}
.grid{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(340px,1fr))}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:14px}
textarea,input,select,button{font:inherit;color:var(--ink);background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:6px 8px}
textarea{width:100%;min-height:70px}button{background:var(--acc);color:#fff;border:0;cursor:pointer}button:disabled{opacity:.5}
.row{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0;align-items:center}.pill{padding:1px 8px;border-radius:99px;border:1px solid var(--line);font-size:12px}
.ok{color:var(--ok)}.bad{color:var(--bad)}.mute{color:var(--mute)}
.bar{height:14px;background:var(--acc);border-radius:3px;min-width:2px}.bw{background:var(--line);border-radius:3px}
table{width:100%;border-collapse:collapse}td,th{padding:4px 6px;text-align:left;border-bottom:1px solid var(--line);font-size:13px}
pre{white-space:pre-wrap;word-break:break-word;margin:0;font:12px/1.4 ui-monospace,Consolas,monospace}
#log{max-height:320px;overflow:auto;background:var(--bg);padding:8px;border-radius:6px}#report{max-height:420px;overflow:auto}
.wide{grid-column:1/-1}
</style></head><body>
<h1>AgentCore multi-agent monitor @@TAG@@ <span class="mute" id="clock"></span></h1>
<div class="grid">
 <div class="card wide"><h2>Invoke</h2>
  <textarea id="prompt">Write a short report on AgentCore Memory for backend engineers</textarea>
  <div class="row"><select id="env">@@ENV_OPTIONS@@</select>
   <input id="actor" value="monitor-user" size="14" title="actor id (memory)">
   <button id="go">Run</button><span id="busy" class="mute"></span></div>
  <div id="err" class="bad"></div></div>
 <div class="card"><h2>Last run</h2><div id="summary" class="mute">No run yet.</div>
  <table id="calls"></table></div>
 <div class="card"><h2>Runtimes</h2><div id="status" class="mute">loading...</div></div>
 <div class="card wide"><h2>Report</h2><div id="report" class="mute">-</div></div>
 <div class="card wide"><h2>Run history (this page)</h2><table id="hist"><tr><th>time</th><th>env</th><th>score</th><th>rev</th><th>agent s</th><th>wall s</th><th>tokens in/out</th></tr></table></div>
 <div class="card wide"><h2>Runtime logs <span class="mute" id="lg"></span></h2>
  <div class="row"><select id="lenv">@@LOG_OPTIONS@@</select><label><input type="checkbox" id="follow" checked> auto-refresh (5s)</label></div>
  <div id="log"><pre id="logtxt"></pre></div></div>
</div>
<script>
const $=id=>document.getElementById(id), esc=s=>String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
async function j(u,o){const r=await fetch(u,o);const b=await r.json();if(!r.ok)throw new Error(b.error||r.status);return b}
async function status(){try{const s=await j('/api/status');$('status').innerHTML=Object.entries(s).map(([k,v])=>v.error?`<div class=bad>${k}: ${esc(v.error)}</div>`:
 `<div><b>${k}</b> ${esc(v.name)} <span class="pill ${v.status=='READY'?'ok':'bad'}">${v.status}</span> v${v.version}<br><span class=mute>`+
 v.endpoints.map(e=>`${esc(e.name)}: ${e.status} (v${e.live})`).join(' · ')+`</span></div>`).join('<hr>')}catch(e){$('status').textContent=e.message}}
async function logs(){try{const l=await j('/api/logs?env='+$('lenv').value);$('lg').textContent=l.group;
 $('logtxt').textContent=l.lines.map(x=>x.t+'  '+x.m).join('\n')||'(no events in the last 15 min)';const d=$('log');d.scrollTop=d.scrollHeight}catch(e){$('logtxt').textContent=e.message}}
$('go').onclick=async()=>{$('go').disabled=true;$('err').textContent='';const t0=Date.now();
 const tick=setInterval(()=>$('busy').textContent='working... '+Math.round((Date.now()-t0)/1000)+'s (a full run takes 40-90s)',500);
 try{const r=await j('/api/invoke',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({prompt:$('prompt').value,actor:$('actor').value,env:$('env').value})});
  const calls=r.agent_calls||[],max=Math.max(1,...calls.map(c=>c.latency_ms));
  const tin=calls.filter(c=>c.agent!='orchestrator').reduce((a,c)=>a+c.input_tokens,0),tout=calls.filter(c=>c.agent!='orchestrator').reduce((a,c)=>a+c.output_tokens,0);
  $('summary').innerHTML=`review score <b>${(r.review_scores||[]).join(', ')||'-'}</b> · revisions <b>${r.revisions??'-'}</b><br>agent-side ${(r.latency_ms/1000).toFixed(1)}s · wall ${r.wall_s}s<br><span class=mute>session ${esc(r.session_id)}</span>`;
  $('calls').innerHTML='<tr><th>agent</th><th>latency</th><th></th><th>in</th><th>out</th></tr>'+calls.map(c=>`<tr><td>${esc(c.agent)}</td><td>${(c.latency_ms/1000).toFixed(1)}s</td><td style="width:40%"><div class=bw><div class=bar style="width:${100*c.latency_ms/max}%"></div></div></td><td>${c.input_tokens}</td><td>${c.output_tokens}</td></tr>`).join('');
  $('report').innerHTML='<pre>'+esc(r.result||JSON.stringify(r,null,2))+'</pre>';
  const row=document.createElement('tr');row.innerHTML=`<td>${new Date().toLocaleTimeString()}</td><td>${r.env}</td><td>${(r.review_scores||[]).join(', ')}</td><td>${r.revisions}</td><td>${(r.latency_ms/1000).toFixed(1)}</td><td>${r.wall_s}</td><td>${tin}/${tout}</td>`;
  $('hist').insertBefore(row,$('hist').rows[1]||null);logs()
 }catch(e){$('err').textContent=e.message}finally{clearInterval(tick);$('busy').textContent='';$('go').disabled=false}};
$('lenv').onchange=logs;setInterval(()=>{$('clock').textContent=new Date().toLocaleTimeString();if($('follow').checked)logs()},5000);status();logs();setInterval(status,30000);
</script></body></html>"""

def render_page() -> str:
    label = {"dev": "dev (research_orchestrator_dev)", "prod": "prod endpoint (research_orchestrator)"}
    return (PAGE.replace("@@ENV_OPTIONS@@", "".join(f'<option value="{k}">{label[k]}</option>' for k in RUNTIMES))
            .replace("@@LOG_OPTIONS@@", "".join(f"<option>{k}</option>" for k in RUNTIMES))
            .replace("@@TAG@@", f"[{LOCK.upper()}]" if LOCK else ""))


if __name__ == "__main__":
    print(f"AgentCore monitor on http://localhost:{PORT}  (Ctrl+C to stop)")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
