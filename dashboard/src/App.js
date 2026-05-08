/**
 * Self-Healing Network AIOps — React Dashboard
 * Real-time operator console with WebSocket telemetry, device grid,
 * event log, telemetry sparklines, and fault injection controls.
 */
import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  LineChart, Line, AreaChart, Area, ResponsiveContainer,
  XAxis, YAxis, Tooltip, CartesianGrid, Legend
} from 'recharts';
import { io } from 'socket.io-client';

const WS_URL = process.env.REACT_APP_WS_URL || 'http://localhost:5000';
const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:5000';
const HISTORY_LEN = 60;
const DEVICES = Array.from({ length: 15 }, (_, i) => `dev${i + 1}`);
const DEVICE_ROLES = {
  ...Object.fromEntries([1,2,3].map(i => [`dev${i}`, 'core'])),
  ...Object.fromEntries([4,5,6,7].map(i => [`dev${i}`, 'edge'])),
  ...Object.fromEntries([8,9,10,11,12,13,14,15].map(i => [`dev${i}`, 'access'])),
};
const FAULT_TYPES = ['high_loss', 'high_latency', 'high_cpu', 'link_flap'];

// ── Colour helpers ──────────────────────────────────────────────────────────
const STATUS_COLOR = {
  ok:          { bg: '#0d2318', border: '#16a34a', text: '#4ade80', dot: '#22c55e' },
  anomaly:     { bg: '#2d1a00', border: '#d97706', text: '#fbbf24', dot: '#f59e0b' },
  remediating: { bg: '#0d1a2d', border: '#3b82f6', text: '#60a5fa', dot: '#3b82f6' },
  verifying:   { bg: '#1a0d2d', border: '#a78bfa', text: '#c4b5fd', dot: '#a78bfa' },
  resolved:    { bg: '#0d2318', border: '#16a34a', text: '#4ade80', dot: '#22c55e' },
  escalated:   { bg: '#2d0f0f', border: '#ef4444', text: '#f87171', dot: '#ef4444' },
};
const ROLE_COLOR = { core: '#818cf8', edge: '#34d399', access: '#94a3b8' };
const METRIC_COLOR = {
  packet_loss_pct: '#f87171',
  latency_ms:      '#fbbf24',
  cpu_pct:         '#818cf8',
  memory_pct:      '#34d399',
};
const EVENT_ICON = {
  alert:         { icon: '⚠',  color: '#fbbf24', label: 'Anomaly detected'   },
  remediation:   { icon: '⚡',  color: '#3b82f6', label: 'NETCONF pushed'     },
  blocked:       { icon: '✕',  color: '#f97316', label: 'Remediation blocked' },
  verifying:     { icon: '◎',  color: '#a78bfa', label: 'Verifying fix'       },
  resolved:      { icon: '✓',  color: '#4ade80', label: 'Healed'              },
  escalated:     { icon: '!!', color: '#f87171', label: 'Escalated'           },
  fault_injected:{ icon: '▶',  color: '#64748b', label: 'Fault injected'      },
  default:       { icon: '·',  color: '#475569', label: ''                    },
};

// ── Styled primitives ────────────────────────────────────────────────────────
const S = {
  topbar: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    padding: '12px 20px', background: '#13151f',
    borderBottom: '1px solid #1e293b', flexWrap: 'wrap', gap: 8,
  },
  logo: { fontWeight: 700, fontSize: 15, color: '#e2e8f0', letterSpacing: '-0.5px' },
  badge: (color) => ({
    fontSize: 11, padding: '3px 10px', borderRadius: 20, fontWeight: 500,
    background: color.bg, border: `1px solid ${color.border}`, color: color.text,
  }),
  dot: (color) => ({
    width: 8, height: 8, borderRadius: '50%', background: color, flexShrink: 0,
    boxShadow: `0 0 6px ${color}`,
  }),
  card: (extra = {}) => ({
    background: '#13151f', border: '1px solid #1e293b',
    borderRadius: 10, padding: '14px 16px', ...extra,
  }),
  section: { fontSize: 11, fontWeight: 600, color: '#64748b',
             textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 10 },
  metricCard: (color) => ({
    background: '#0d111a', border: `1px solid ${color}22`,
    borderRadius: 8, padding: '10px 14px', minWidth: 120,
  }),
  metricVal: { fontSize: 22, fontWeight: 700, lineHeight: 1 },
  metricLbl: { fontSize: 11, color: '#64748b', marginTop: 3 },
  devCard: (status) => {
    const c = STATUS_COLOR[status] || STATUS_COLOR.ok;
    return {
      background: c.bg, border: `1px solid ${c.border}`,
      borderRadius: 8, padding: '8px 10px', cursor: 'pointer',
      transition: 'all 0.2s',
    };
  },
  devId:   { fontSize: 11, fontWeight: 600, color: '#e2e8f0' },
  devRole: (role) => ({
    fontSize: 10, color: ROLE_COLOR[role] || '#94a3b8', marginTop: 1,
  }),
  evRow: { display: 'flex', gap: 8, padding: '7px 0',
           borderBottom: '1px solid #1e293b', alignItems: 'flex-start' },
  evText: { fontSize: 12, color: '#cbd5e1', lineHeight: 1.5, flex: 1 },
  evTime: { fontSize: 11, color: '#475569', whiteSpace: 'nowrap', flexShrink: 0 },
  btn: (variant = 'default') => {
    const variants = {
      default: { background: '#1e293b', border: '1px solid #334155', color: '#e2e8f0' },
      danger:  { background: '#450a0a', border: '1px solid #dc2626', color: '#f87171' },
      info:    { background: '#0c1a2d', border: '1px solid #3b82f6', color: '#60a5fa' },
      success: { background: '#052e16', border: '1px solid #16a34a', color: '#4ade80' },
    };
    return {
      ...variants[variant], borderRadius: 6, padding: '6px 14px',
      fontSize: 12, cursor: 'pointer', fontWeight: 500, transition: 'opacity 0.15s',
    };
  },
  select: {
    background: '#1e293b', border: '1px solid #334155', color: '#e2e8f0',
    borderRadius: 6, padding: '5px 10px', fontSize: 12, cursor: 'pointer',
  },
};

// ── Sparkline ────────────────────────────────────────────────────────────────
function Sparkline({ data, metric, color }) {
  return (
    <ResponsiveContainer width="100%" height={36}>
      <AreaChart data={data} margin={{ top: 2, right: 0, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id={`sg-${metric}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%"  stopColor={color} stopOpacity={0.3} />
            <stop offset="95%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        <Area type="monotone" dataKey={metric} stroke={color} strokeWidth={1.5}
              fill={`url(#sg-${metric})`} dot={false} isAnimationActive={false} />
      </AreaChart>
    </ResponsiveContainer>
  );
}

// ── Device card ──────────────────────────────────────────────────────────────
function DeviceCard({ id, metrics, status, onClick }) {
  const c   = STATUS_COLOR[status] || STATUS_COLOR.ok;
  const loss = metrics?.packet_loss_pct?.toFixed(1) ?? '—';
  const lat  = metrics?.latency_ms?.toFixed(0) ?? '—';
  const cpu  = metrics?.cpu_pct?.toFixed(0) ?? '—';
  return (
    <div style={S.devCard(status)} onClick={() => onClick(id)}
         title={`Click to inspect ${id}`}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={S.devId}>{id}</span>
        <span style={S.dot(c.dot)} />
      </div>
      <div style={S.devRole(DEVICE_ROLES[id])}>{DEVICE_ROLES[id]}</div>
      <div style={{ display: 'flex', gap: 6, marginTop: 6, fontSize: 10, color: '#94a3b8' }}>
        <span>loss {loss}%</span>
        <span>·</span>
        <span>{lat}ms</span>
        <span>·</span>
        <span>cpu {cpu}%</span>
      </div>
    </div>
  );
}

// ── Event row ─────────────────────────────────────────────────────────────────
function EventRow({ event }) {
  const meta = EVENT_ICON[event.type] || EVENT_ICON.default;
  const t  = new Date(event.ts);
  const ts = `${t.getHours().toString().padStart(2,'0')}:${t.getMinutes().toString().padStart(2,'0')}:${t.getSeconds().toString().padStart(2,'0')}`;

  let text = event.message || '';
  if (!text) {
    if (event.type === 'alert')
      text = `Anomaly: ${event.anomaly_type} on ${event.device_id} (${(event.confidence*100).toFixed(0)}% conf, model: ${event.model})`;
    else if (event.type === 'remediation')
      text = `NETCONF pushed '${event.playbook}' to ${event.device_id} in ${event.duration_ms}ms — now verifying...`;
    else if (event.type === 'blocked')
      text = `Blocked: ${event.playbook} on ${event.device_id} — ${event.reason}`;
    else if (event.type === 'resolved')
      text = `HEALED: ${event.device_id} cleared in ${event.duration_s}s (${event.readings} readings)`;
    else if (event.type === 'escalated')
      text = `ESCALATED: ${event.device_id} — manual intervention needed`;
    else if (event.type === 'fault_injected')
      text = `Fault injected: ${event.fault_type} on ${event.device_id}`;
    else
      text = event.type + (event.device_id ? ' on ' + event.device_id : '');
  }

  return (
    <div style={S.evRow}>
      <span style={{ fontSize: 11, color: meta.color, flexShrink: 0, marginTop: 1,
                     fontWeight: 600, minWidth: 18, textAlign: 'center' }}>{meta.icon}</span>
      <span style={S.evText}>{text}</span>
      <span style={S.evTime}>{ts}</span>
    </div>
  );
}

// ── Detail drawer ─────────────────────────────────────────────────────────────
function DeviceDrawer({ id, history, status, onClose, onInject }) {
  const [fault, setFault] = useState('high_loss');
  if (!id) return null;
  const latest = history[history.length - 1] || {};
  return (
    <div style={{
      position: 'fixed', right: 0, top: 0, bottom: 0, width: 340,
      background: '#13151f', borderLeft: '1px solid #1e293b',
      zIndex: 100, padding: 20, overflowY: 'auto',
      display: 'flex', flexDirection: 'column', gap: 16,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <div style={{ fontWeight: 700, fontSize: 16 }}>{id}</div>
          <div style={{ fontSize: 12, color: ROLE_COLOR[DEVICE_ROLES[id]] }}>
            {DEVICE_ROLES[id]} node
          </div>
        </div>
        <button style={S.btn()} onClick={onClose}>✕ close</button>
      </div>

      {/* Live metrics */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
        {[
          ['packet_loss_pct', 'Packet loss', '%',  '#f87171'],
          ['latency_ms',      'Latency',     'ms', '#fbbf24'],
          ['cpu_pct',         'CPU',         '%',  '#818cf8'],
          ['memory_pct',      'Memory',      '%',  '#34d399'],
        ].map(([key, lbl, unit, color]) => (
          <div key={key} style={S.metricCard(color)}>
            <div style={{ ...S.metricVal, color }}>{latest[key]?.toFixed(1) ?? '—'}<span style={{ fontSize: 12 }}>{unit}</span></div>
            <div style={S.metricLbl}>{lbl}</div>
          </div>
        ))}
      </div>

      {/* Sparklines */}
      {[
        ['packet_loss_pct', 'Packet loss %',  '#f87171'],
        ['latency_ms',      'Latency ms',     '#fbbf24'],
        ['cpu_pct',         'CPU %',          '#818cf8'],
      ].map(([metric, label, color]) => (
        <div key={metric}>
          <div style={{ fontSize: 11, color: '#64748b', marginBottom: 2 }}>{label}</div>
          <Sparkline data={history} metric={metric} color={color} />
        </div>
      ))}

      {/* Fault injection */}
      <div style={{ borderTop: '1px solid #1e293b', paddingTop: 16 }}>
        <div style={{ ...S.section, marginBottom: 8 }}>Inject fault</div>
        <div style={{ display: 'flex', gap: 8 }}>
          <select style={S.select} value={fault} onChange={e => setFault(e.target.value)}>
            {FAULT_TYPES.map(f => <option key={f} value={f}>{f}</option>)}
          </select>
          <button style={S.btn('danger')} onClick={() => onInject(id, fault)}>
            Inject ↗
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Main App ──────────────────────────────────────────────────────────────────
export default function App() {
  const [connected,   setConnected]   = useState(false);
  const [devices,     setDevices]     = useState({});
  const [deviceStatus,setDeviceStatus]= useState({});
  const [events,      setEvents]      = useState([]);
  const [stats,       setStats]       = useState({ alerts:0, resolved:0, escalated:0, avg_mttr:0 });
  const [history,     setHistory]     = useState({}); // device_id → [{ts, ...metrics}]
  const [selected,    setSelected]    = useState(null);
  const [chartMetric, setChartMetric] = useState('packet_loss_pct');
  const [globalChart, setGlobalChart] = useState([]); // aggregated
  const socketRef = useRef(null);

  // ── WebSocket ──────────────────────────────────────────────────────────────
  useEffect(() => {
    const socket = io(WS_URL, { transports: ['websocket'] });
    socketRef.current = socket;

    socket.on('connect',    () => setConnected(true));
    socket.on('disconnect', () => setConnected(false));

    socket.on('telemetry', (rec) => {
      const dev = rec.device_id;
      setDevices(prev => ({ ...prev, [dev]: rec }));
      setHistory(prev => {
        const h = prev[dev] || [];
        const next = [...h, { ts: rec.timestamp, ...rec }].slice(-HISTORY_LEN);
        return { ...prev, [dev]: next };
      });
      // Global chart (avg packet_loss across all devices)
      setGlobalChart(prev => {
        const t   = new Date(rec.timestamp).toLocaleTimeString();
        const last = prev[prev.length - 1] || {};
        const entry = { ...last, t, [dev]: rec.packet_loss_pct };
        return [...prev.slice(-HISTORY_LEN), entry];
      });
    });

    socket.on('event', (ev) => {
      setEvents(prev => [ev, ...prev].slice(0, 100));
      // Update device card colour for every pipeline stage
      if (ev.device_id) {
        const statusMap = {
          fault_injected: 'anomaly',
          alert:          'anomaly',
          remediation:    'remediating',
          blocked:        'escalated',
          verifying:      'verifying',
          resolved:       'resolved',
          escalated:      'escalated',
        };
        const newStatus = statusMap[ev.type];
        if (newStatus) {
          setDeviceStatus(prev => ({ ...prev, [ev.device_id]: newStatus }));
          if (newStatus === 'resolved') {
            setTimeout(() => {
              setDeviceStatus(prev => ({ ...prev, [ev.device_id]: 'ok' }));
            }, 6000);
          }
        }
      }
    });

    // Also handle direct device_status events from backend
    socket.on('device_status', ({ device_id, status }) => {
      setDeviceStatus(prev => ({ ...prev, [device_id]: status }));
      if (status === 'resolved') {
        setTimeout(() => {
          setDeviceStatus(prev => ({ ...prev, [device_id]: 'ok' }));
        }, 6000);
      }
    });

    socket.on('state', (s) => {
      if (s.devices) setDevices(s.devices);
      setStats({
        alerts:    s.alerts_total    || 0,
        resolved:  s.resolved_total  || 0,
        escalated: s.escalated_total || 0,
        avg_mttr:  s.avg_mttr?.toFixed(1) || '—',
      });
    });

    return () => socket.disconnect();
  }, []);

  // ── Inject fault via REST ──────────────────────────────────────────────────
  const handleInject = useCallback(async (deviceId, faultType) => {
    try {
      await fetch(`${API_URL}/api/inject/${deviceId}/${faultType}`, { method: 'POST' });
    } catch (e) {
      console.error('Inject failed:', e);
    }
  }, []);

  // ── Layout ─────────────────────────────────────────────────────────────────
  const globalStatus = events.some(e => e.type === 'alert' &&
    (Date.now() - new Date(e.ts).getTime()) < 15000) ? 'anomaly' : 'ok';

  const activeChart = DEVICES.filter(d => history[d]?.length > 0)
    .map(d => ({ id: d, data: history[d] || [] }));

  return (
    <div style={{ minHeight: '100vh', background: '#0f1117' }}>
      {/* Top bar */}
      <div style={S.topbar}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={S.logo}>◈ AIOps Console</span>
          <span style={{ fontSize: 11, color: '#475569' }}>Self-Healing Network</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={S.dot(connected ? '#22c55e' : '#ef4444')} />
          <span style={{ fontSize: 11, color: '#64748b' }}>
            {connected ? 'Live' : 'Disconnected'}
          </span>
          <span style={S.badge(STATUS_COLOR[globalStatus])}>
            {globalStatus === 'ok' ? 'All systems normal' : 'Anomaly detected'}
          </span>
        </div>
      </div>

      <div style={{ padding: '16px 20px', paddingRight: selected ? 360 : 20 }}>

        {/* KPI row */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12, marginBottom: 16 }}>
          {[
            { label: 'Devices monitored', value: '15',         sub: '6 core · 4 edge · 5 access', color: '#818cf8' },
            { label: 'Anomalies detected', value: stats.alerts, sub: 'since session start',        color: '#fbbf24' },
            { label: 'Auto-resolved',       value: stats.resolved,  sub: `${stats.escalated} escalated`, color: '#4ade80' },
            { label: 'Avg MTTR',            value: `${stats.avg_mttr}s`, sub: 'vs 2.4h manual baseline', color: '#38bdf8' },
          ].map(({ label, value, sub, color }) => (
            <div key={label} style={S.card()}>
              <div style={{ fontSize: 11, color: '#64748b', marginBottom: 4 }}>{label}</div>
              <div style={{ fontSize: 26, fontWeight: 700, color, lineHeight: 1 }}>{value}</div>
              <div style={{ fontSize: 11, color: '#475569', marginTop: 4 }}>{sub}</div>
            </div>
          ))}
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14, marginBottom: 14 }}>

          {/* Device grid */}
          <div style={S.card()}>
            <div style={S.section}>Device grid — click to inspect</div>
            {['core', 'edge', 'access'].map(role => (
              <div key={role}>
                <div style={{ fontSize: 10, color: ROLE_COLOR[role], marginBottom: 6, marginTop: role === 'core' ? 0 : 10 }}>
                  {role.toUpperCase()}
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(100px, 1fr))', gap: 6 }}>
                  {DEVICES.filter(d => DEVICE_ROLES[d] === role).map(d => (
                    <DeviceCard
                      key={d} id={d}
                      metrics={devices[d]}
                      status={deviceStatus[d] || 'ok'}
                      onClick={() => setSelected(selected === d ? null : d)}
                    />
                  ))}
                </div>
              </div>
            ))}
          </div>

          {/* Telemetry chart */}
          <div style={S.card()}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
              <div style={S.section}>Live telemetry</div>
              <select style={S.select} value={chartMetric} onChange={e => setChartMetric(e.target.value)}>
                <option value="packet_loss_pct">Packet loss %</option>
                <option value="latency_ms">Latency ms</option>
                <option value="cpu_pct">CPU %</option>
                <option value="memory_pct">Memory %</option>
              </select>
            </div>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="ts" tick={{ fontSize: 10, fill: '#475569' }}
                       tickFormatter={v => v ? new Date(v).toLocaleTimeString().slice(0,5) : ''}
                       type="category" allowDuplicatedCategory={false} />
                <YAxis tick={{ fontSize: 10, fill: '#475569' }} />
                <Tooltip
                  contentStyle={{ background: '#1e293b', border: '1px solid #334155',
                                  borderRadius: 6, fontSize: 11 }}
                  labelStyle={{ color: '#94a3b8' }}
                />
                {activeChart.slice(0, 5).map(({ id, data }, i) => (
                  <Line key={id} data={data} type="monotone" dataKey={chartMetric}
                        stroke={Object.values(METRIC_COLOR)[i % 4]}
                        strokeWidth={1.5} dot={false} isAnimationActive={false}
                        name={id} />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Event log */}
        <div style={S.card({ maxHeight: 280, overflowY: 'auto' })}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
            <div style={S.section}>Event log</div>
            <span style={{ fontSize: 11, color: '#475569' }}>{events.length} events</span>
          </div>
          {events.length === 0 && (
            <div style={{ fontSize: 12, color: '#475569', padding: '12px 0' }}>
              Waiting for telemetry… start the backend with <code style={{ color: '#818cf8' }}>python src/main.py --demo</code>
            </div>
          )}
          {events.map((ev, i) => <EventRow key={i} event={ev} />)}
        </div>

        {/* Quick fault inject (global) */}
        <div style={{ ...S.card({ marginTop: 14 }) }}>
          <div style={S.section}>Quick fault injection — demo controls</div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {['dev1','dev4','dev8'].map(dev =>
              FAULT_TYPES.map(ft => (
                <button key={`${dev}-${ft}`} style={S.btn('danger')}
                        onClick={() => handleInject(dev, ft)}>
                  {dev} · {ft}
                </button>
              ))
            )}
          </div>
        </div>

      </div>

      {/* Device detail drawer */}
      <DeviceDrawer
        id={selected}
        history={selected ? (history[selected] || []) : []}
        status={selected ? (deviceStatus[selected] || 'ok') : 'ok'}
        onClose={() => setSelected(null)}
        onInject={handleInject}
      />
    </div>
  );
}
