import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import Plot from "react-plotly.js";
import "./styles.css";

const ORGANIZATION_PALETTE = {
  "Motilal Oswal": "#00D4AA",
  "ICICI Securities": "#7B9FFF",
  "Prabhudas Lilladher": "#C77DFF",
  "Emkay Global Financial Services": "#FF9E40",
  Nuvama: "#56B4E9",
  "JM Financial": "#E69F00",
  "HDFC Securities": "#009E73",
  "Kotak Securities": "#D55E00",
  "Axis Securities": "#CC79A7",
  "Geojit Financial Services": "#F0E442",
  "Anand Rathi": "#EE6677",
};
const FALLBACK_ORGANIZATION_COLORS = [
  "#88CCEE",
  "#CC6677",
  "#DDCC77",
  "#117733",
  "#332288",
  "#AA4499",
  "#44AA99",
  "#999933",
  "#882255",
  "#661100",
  "#6699CC",
  "#888888",
];
const SYMBOL_CYCLE = ["diamond", "square", "x", "cross", "triangle-up", "triangle-down", "star", "pentagon"];
const DEFAULT_SYMBOL = "circle";
const COLORWAY = ["#00d4aa", "#ff5577", "#ffb400", "#7b9fff", "#c77dff", "#ff9e40"];
const TABS = ["Scorecard", "All Calls", "Target Analysis", "Stock Lookup"];

const PLOTLY_THEME = {
  paper_bgcolor: "#0a0a0f",
  plot_bgcolor: "#0a0a0f",
  font: { family: "DM Mono, monospace", color: "#e8e8f0", size: 12 },
  colorway: COLORWAY,
};
const AXIS_STYLE = { gridcolor: "#1e1e2e", linecolor: "#2a2a3e", tickcolor: "#2a2a3e" };
const PLOT_CONFIG = { displayModeBar: false, responsive: true };

function formatPct(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "";
  const num = Number(value);
  return `${num > 0 ? "+" : ""}${num.toFixed(digits)}%`;
}

function formatPlainPct(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "";
  return `${Number(value).toFixed(digits)}%`;
}

function formatNum(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "";
  return Number(value).toLocaleString("en-IN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatMoney(value) {
  const formatted = formatNum(value, 2);
  return formatted ? `₹${formatted}` : "";
}

function formatDate(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });
}

async function fetchJson(path) {
  const response = await fetch(path);
  if (!response.ok) throw new Error(`Failed to load ${path}: ${response.status}`);
  return response.json();
}

function finiteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function sortStrings(values) {
  return [...new Set(values.filter(Boolean))].sort((a, b) => String(a).localeCompare(String(b)));
}

function buildFirmColorMap(rows) {
  const firms = sortStrings(rows.map((row) => row.organization));
  const colorMap = { ...ORGANIZATION_PALETTE };
  const missing = firms.filter((firm) => !colorMap[firm]);
  missing.forEach((firm, idx) => {
    colorMap[firm] = FALLBACK_ORGANIZATION_COLORS[idx % FALLBACK_ORGANIZATION_COLORS.length];
  });
  return { firms, colorMap };
}

function buildSymbolMap(firms) {
  return Object.fromEntries(
    firms.map((firm, idx) => [firm, idx === 0 ? DEFAULT_SYMBOL : SYMBOL_CYCLE[(idx - 1) % SYMBOL_CYCLE.length]])
  );
}

function plotLayout(layout) {
  return {
    ...PLOTLY_THEME,
    autosize: true,
    ...layout,
  };
}

function useDashboardData() {
  const [state, setState] = useState({ status: "loading", error: null, data: null });

  useEffect(() => {
    let active = true;
    Promise.all([
      fetchJson("/data/scorecard.json"),
      fetchJson("/data/returns.json"),
      fetchJson("/data/target-hit.json"),
      fetchJson("/data/stocks.json"),
      fetchJson("/data/manifest.json"),
    ])
      .then(([scorecard, returns, targetHit, stocks, manifest]) => {
        if (!active) return;
        setState({ status: "ready", error: null, data: { scorecard, returns, targetHit, stocks, manifest } });
      })
      .catch((error) => {
        if (!active) return;
        setState({ status: "error", error, data: null });
      });
    return () => {
      active = false;
    };
  }, []);

  return state;
}

function SectionTitle({ children }) {
  return <div className="section-title">{children}</div>;
}

function MetricCard({ label, value, tone, subtext }) {
  return (
    <div className="metric-card">
      <div className="metric-label">{label}</div>
      <div className={`metric-value ${tone || ""}`}>{value}</div>
      {subtext ? <div className="metric-subtext">{subtext}</div> : null}
    </div>
  );
}

function VirtualTable({ columns, rows, height = 460, rowHeight = 38 }) {
  const [scrollTop, setScrollTop] = useState(0);
  const visibleCount = Math.ceil(height / rowHeight) + 8;
  const start = Math.max(0, Math.floor(scrollTop / rowHeight) - 4);
  const visibleRows = rows.slice(start, start + visibleCount);
  const topPad = start * rowHeight;
  const totalHeight = rows.length * rowHeight;
  const gridCols = columns.map((c) => c.width || "1fr").join(" ");

  return (
    <div className="table-shell">
      <div className="table-scroll" style={{ height }} onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}>
        <div className="table-head" style={{ gridTemplateColumns: gridCols }}>
          {columns.map((column) => (
            <div key={column.key}>{column.label}</div>
          ))}
        </div>
        <div style={{ height: totalHeight, position: "relative" }}>
          <div style={{ transform: `translateY(${topPad}px)` }}>
            {visibleRows.map((row, idx) => (
              <div
                className="table-row"
                key={`${start + idx}-${row.rec_id || row.stock_name || row.organization || idx}`}
                style={{
                  height: rowHeight,
                  gridTemplateColumns: gridCols,
                }}
              >
                {columns.map((column) => (
                  <div key={column.key} className={column.className ? column.className(row[column.key], row) : ""}>
                    {column.render ? column.render(row[column.key], row) : row[column.key]}
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function PlotFrame({ data, layout }) {
  return (
    <div className="chart-frame">
      <Plot data={data} layout={layout} config={PLOT_CONFIG} useResizeHandler className="plotly-chart" />
    </div>
  );
}

function DashboardMetrics({ returns, targetHit }) {
  const totalRecs = returns.length;
  const totalFirms = new Set(returns.map((row) => row.organization).filter(Boolean)).size;
  const avgReturn = returns.reduce((sum, row) => sum + (Number(row.return_current) || 0), 0) / Math.max(totalRecs, 1);
  const correct = returns.filter((row) => Number(row.direction_correct) === 1).length;
  const targetsHit = targetHit.filter((row) => Number(row.target_hit) === 1).length;
  return (
    <div className="metric-grid">
      <MetricCard label="Total Calls" value={totalRecs} />
      <MetricCard label="Firms Tracked" value={totalFirms} />
      <MetricCard label="Avg Return Current" value={formatPct(avgReturn)} tone={avgReturn >= 0 ? "positive" : "negative"} />
      <MetricCard label="Direction Accuracy" value={formatPct((correct / Math.max(totalRecs, 1)) * 100)} />
      <MetricCard label="Targets Hit" value={targetsHit} subtext={`/ ${targetHit.length}`} />
    </div>
  );
}

function HitRateRankingChart({ scorecard }) {
  const rows = [...scorecard]
    .filter((row) => Number(row.total_calls) >= 3)
    .sort((a, b) => Number(a.hit_rate_pct) - Number(b.hit_rate_pct));

  return (
    <PlotFrame
      data={[
        {
          type: "bar",
          orientation: "h",
          x: rows.map((row) => finiteNumber(row.hit_rate_pct)),
          y: rows.map((row) => row.organization),
          marker: {
            color: rows.map((row) => finiteNumber(row.hit_rate_pct)),
            colorscale: [
              [0, "#ff5577"],
              [0.5, "#ffb400"],
              [1, "#00d4aa"],
            ],
            cmin: 0,
            cmax: 100,
          },
          text: rows.map((row) => formatPlainPct(row.hit_rate_pct)),
          textposition: "outside",
          textfont: { size: 11 },
          hovertemplate: "%{y}<br>Dir. Accuracy: %{x:.1f}%<extra></extra>",
        },
      ]}
      layout={plotLayout({
        height: 380,
        margin: { l: 160, r: 60, t: 10, b: 10 },
        xaxis: { ...AXIS_STYLE, range: [0, 110], showgrid: false, showticklabels: false },
        yaxis: { ...AXIS_STYLE, showgrid: false, automargin: true, tickfont: { size: 11 } },
        showlegend: false,
      })}
    />
  );
}

function ReturnDistributionChart({ returns, scorecard, firmContext, minCalls }) {
  const included = new Set(scorecard.filter((row) => Number(row.total_calls) >= minCalls).map((row) => row.organization));
  const rows = returns.filter((row) => included.has(row.organization) && finiteNumber(row.return_current) !== null);
  const firms = firmContext.firms.filter((firm) => included.has(firm));
  const data = firms
    .map((firm) => {
      const firmRows = rows.filter((row) => row.organization === firm);
      if (!firmRows.length) return null;
      return {
        type: "box",
        name: firm,
        x: firmRows.map(() => firm),
        y: firmRows.map((row) => finiteNumber(row.return_current)),
        text: firmRows.map((row) => row.stock_name),
        marker: { color: firmContext.colorMap[firm] },
        line: { color: firmContext.colorMap[firm] },
        hovertemplate: "Firm: %{x}<br>Return: %{y:.1f}%<br>Stock: %{text}<extra></extra>",
      };
    })
    .filter(Boolean);

  return (
    <PlotFrame
      data={data}
      layout={plotLayout({
        height: 600,
        margin: { l: 80, r: 10, t: 10, b: 60 },
        showlegend: false,
        xaxis: { ...AXIS_STYLE, tickangle: -30, categoryorder: "array", categoryarray: firmContext.firms, automargin: true },
        yaxis: { ...AXIS_STYLE, title: { text: "% Returns", standoff: 14 }, automargin: true },
        shapes: [{ type: "line", xref: "paper", x0: 0, x1: 1, y0: 0, y1: 0, line: { color: "#3a3a5e", width: 1, dash: "dot" } }],
      })}
    />
  );
}

function PromisedVsActualChart({ rows, firmContext }) {
  const plotRows = rows
    .map((row) => {
      const potential = finiteNumber(row.potential_returns);
      const actual = finiteNumber(row.return_current);
      if (potential === null || actual === null) return null;
      const promiseDelta = actual - potential;
      return {
        ...row,
        potential,
        actual,
        promiseDelta,
        promiseResult: promiseDelta > 0 ? "Outperformed" : promiseDelta < 0 ? "Underperformed" : "Met",
      };
    })
    .filter(Boolean);
  const firms = sortStrings(plotRows.map((row) => row.organization));
  const symbolMap = buildSymbolMap(firms);
  const traces = firms.map((firm) => {
    const firmRows = plotRows.filter((row) => row.organization === firm);
    return {
      type: "scatter",
      mode: "markers",
      name: firm,
      x: firmRows.map((row) => row.potential),
      y: firmRows.map((row) => row.actual),
      text: firmRows.map((row) => row.stock_name),
      customdata: firmRows.map((row) => [
        formatDate(row.recommend_date),
        formatMoney(row.recommended_price),
        formatMoney(row.target_price),
        formatPct(row.promiseDelta),
        row.promiseResult,
      ]),
      marker: {
        color: firmContext.colorMap[firm],
        symbol: symbolMap[firm],
        opacity: 0.88,
        size: 8,
        line: { width: 0.5, color: "#0a0a0f" },
      },
      hovertemplate:
        "Firm: %{fullData.name}<br>Stock: %{text}<br>Potential Return: %{x:.1f}%<br>Actual Return: %{y:.1f}%<br>Date: %{customdata[0]}<br>Entry: %{customdata[1]}<br>Target: %{customdata[2]}<br>Actual - Potential: %{customdata[3]}<br>Promise Outcome: %{customdata[4]}<extra></extra>",
    };
  });

  const xValues = plotRows.map((row) => row.potential);
  const yValues = plotRows.map((row) => row.actual);
  let xLo = -20;
  let xHi = 30;
  let yLo = -20;
  let yHi = 20;
  if (xValues.length && yValues.length) {
    const xMin = Math.min(...xValues, 0);
    const xMax = Math.max(...xValues, 15);
    const yMin = Math.min(...yValues);
    const yMax = Math.max(...yValues);
    const xSpan = Math.max(xMax - xMin, 1);
    const ySpan = Math.max(yMax - yMin, 1);
    xLo = xMin - Math.max(xSpan * 0.08, 5);
    xHi = xMax + Math.max(xSpan * 0.08, 5);
    yLo = yMin - Math.max(ySpan * 0.08, 3);
    yHi = yMax + Math.max(ySpan * 0.08, 3);
  }

  const d0 = Math.max(xLo, yLo);
  const d1 = Math.min(xHi, yHi);
  const diagVisible = d1 > d0;
  const shapes = [
    { type: "line", xref: "paper", x0: 0, x1: 1, y0: 0, y1: 0, line: { color: "#3a3a5e", width: 1, dash: "dot" } },
    { type: "line", x0: 0, x1: 0, y0: yLo, y1: yHi, line: { color: "#6c728f", width: 1, dash: "dash" } },
    { type: "line", x0: 15, x1: 15, y0: yLo, y1: yHi, line: { color: "#6c728f", width: 1, dash: "dash" } },
    { type: "rect", x0: xLo, x1: Math.min(0, xHi), y0: yLo, y1: yHi, fillcolor: "rgba(255, 99, 132, 0.07)", line: { width: 0 }, layer: "below" },
    { type: "rect", x0: Math.max(0, xLo), x1: Math.min(15, xHi), y0: yLo, y1: yHi, fillcolor: "rgba(255, 206, 86, 0.08)", line: { width: 0 }, layer: "below" },
    { type: "rect", x0: Math.max(15, xLo), x1: xHi, y0: yLo, y1: yHi, fillcolor: "rgba(75, 192, 192, 0.08)", line: { width: 0 }, layer: "below" },
  ];
  if (diagVisible) {
    shapes.push({ type: "line", x0: d0, y0: d0, x1: d1, y1: d1, line: { color: "rgba(220, 220, 230, 0.8)", width: 2, dash: "dash" } });
  }

  const annotationStyle = {
    showarrow: false,
    align: "left",
    bgcolor: "rgba(18,18,26,0.75)",
    bordercolor: "#2a2a3e",
    borderwidth: 1,
    font: { size: 10, color: "#cfd3e6" },
  };
  const annotations = [
    zoneAnnotation("Sell (<0%)", xLo, Math.min(0, xHi)),
    zoneAnnotation("Hold (0-15%)", Math.max(0, xLo), Math.min(15, xHi)),
    zoneAnnotation("Buy (>=15%)", Math.max(15, xLo), xHi),
    { x: 0.99, y: 0.01, xref: "paper", yref: "paper", xanchor: "right", yanchor: "bottom", text: "y = x (met promise)", ...annotationStyle },
    {
      x: 0.01,
      y: 0.01,
      xref: "paper",
      yref: "paper",
      xanchor: "left",
      yanchor: "bottom",
      text: "SELL zone:<br>Above line = weaker SELL call<br>(fell less than predicted)",
      ...annotationStyle,
    },
  ].filter(Boolean);
  if (diagVisible) {
    const span = d1 - d0;
    const offset = Math.max(span * 0.03, 0.6);
    annotations.push(
      {
        x: d0 + span * 0.78 - offset,
        y: clamp(d0 + span * 0.78 + offset, yLo + (yHi - yLo) * 0.03, yHi - (yHi - yLo) * 0.03),
        xref: "x",
        yref: "y",
        xanchor: "left",
        yanchor: "bottom",
        text: "Outperformed promise<br>(actual > potential)",
        showarrow: false,
        align: "left",
        opacity: 0.9,
        bgcolor: "rgba(0, 212, 170, 0.18)",
        bordercolor: "rgba(0, 212, 170, 0.45)",
        borderwidth: 1,
        font: { size: 10, color: "#9EF5C6" },
        xshift: 6,
        yshift: 4,
      },
      {
        x: d0 + span * 0.22 + offset,
        y: clamp(d0 + span * 0.22 - offset, yLo + (yHi - yLo) * 0.03, yHi - (yHi - yLo) * 0.03),
        xref: "x",
        yref: "y",
        xanchor: "right",
        yanchor: "top",
        text: "Underperformed promise<br>(actual < potential)",
        showarrow: false,
        align: "left",
        opacity: 0.9,
        bgcolor: "rgba(230, 57, 70, 0.18)",
        bordercolor: "rgba(230, 57, 70, 0.45)",
        borderwidth: 1,
        font: { size: 10, color: "#FFB3C1" },
        xshift: -6,
        yshift: -4,
      }
    );
  }

  function zoneAnnotation(text, left, right) {
    if (right <= left || (text.startsWith("Hold") && right - left < 2)) return null;
    return {
      x: (left + right) / 2,
      y: text.startsWith("Hold") && right - left < 4 ? 1.01 : 0.98,
      xref: "x",
      yref: "paper",
      xanchor: "center",
      yanchor: text.startsWith("Hold") && right - left < 4 ? "bottom" : "top",
      text,
      ...annotationStyle,
      font: { size: text.startsWith("Hold") && right - left < 4 ? 9 : 10, color: "#cfd3e6" },
    };
  }

  return (
    <PlotFrame
      data={traces}
      layout={plotLayout({
        title: "Promised vs Actual Return",
        height: 600,
        margin: { l: 80, r: 10, t: 56, b: 70 },
        xaxis: { ...AXIS_STYLE, range: [xLo, xHi], title: { text: "% Target Returns", standoff: 14 }, automargin: true },
        yaxis: { ...AXIS_STYLE, range: [yLo, yHi], title: { text: "% Actual Returns", standoff: 14 }, automargin: true },
        shapes,
        annotations,
        legend: { title: { text: "Firm" }, orientation: "v", yanchor: "top", y: 1, xanchor: "left", x: 1.02, traceorder: "normal" },
      })}
    />
  );
}

function TargetHitRateByUpsideChart({ targetHit }) {
  const brackets = [
    { label: "<0%", min: -Infinity, max: 0 },
    { label: "0-10%", min: 0, max: 10 },
    { label: "10-20%", min: 10, max: 20 },
    { label: "20-30%", min: 20, max: 30 },
    { label: "30%+", min: 30, max: Infinity },
  ];

  const stats = brackets.map((b) => ({ ...b, total: 0, hits: 0 }));

  targetHit.forEach((row) => {
    const upside = finiteNumber(row.target_upside_pct);
    if (upside === null) return;
    const isHit = Number(row.target_hit) === 1;
    for (const b of stats) {
      if (upside > b.min && upside <= b.max) {
        b.total++;
        if (isHit) b.hits++;
        break;
      } else if (b.label === "<0%" && upside <= 0) {
        b.total++;
        if (isHit) b.hits++;
        break;
      }
    }
  });

  const validStats = stats.filter((b) => b.total > 0);
  const xValues = validStats.map((b) => b.label);
  const yValues = validStats.map((b) => (b.hits / b.total) * 100);
  const textValues = validStats.map((b) => {
    const pct = (b.hits / b.total) * 100;
    return `${b.hits} / ${b.total} (${pct.toFixed(1)}%)`;
  });

  return (
    <PlotFrame
      data={[
        {
          type: "bar",
          x: xValues,
          y: yValues,
          text: textValues,
          textposition: "outside",
          cliponaxis: false,
          textfont: { size: 10, color: "#e8e8f0" },
          marker: {
            color: yValues,
            colorscale: [
              [0, "#ff5577"],
              [0.5, "#ffb400"],
              [1, "#00d4aa"],
            ],
            cmin: 0,
            cmax: 100,
          },
          hovertemplate: "Upside: %{x}<br>Hit Rate: %{y:.1f}%<br>%{text}<extra></extra>",
        },
      ]}
      layout={plotLayout({
        height: 360,
        margin: { l: 60, r: 18, t: 40, b: 58 },
        xaxis: { ...AXIS_STYLE, title: { text: "Promised Upside Bracket", standoff: 12 }, automargin: true },
        yaxis: { ...AXIS_STYLE, title: { text: "Hit Rate (%)", standoff: 14 }, range: [0, 115], automargin: true },
        showlegend: false,
      })}
    />
  );
}

function TargetHitStackedChart({ targetHit }) {
  const rows = Object.values(
    targetHit.reduce((acc, row) => {
      const firm = row.organization || "Unknown";
      acc[firm] ||= { organization: firm, total: 0, hits: 0 };
      acc[firm].total += 1;
      acc[firm].hits += Number(row.target_hit) === 1 ? 1 : 0;
      return acc;
    }, {})
  )
    .filter((row) => row.total >= 2)
    .map((row) => ({ ...row, hit_rate: (row.hits / row.total) * 100 }))
    .sort((a, b) => b.hit_rate - a.hit_rate);
  return (
    <PlotFrame
      data={[
        { type: "bar", name: "Hits", x: rows.map((row) => row.organization), y: rows.map((row) => row.hits), marker: { color: "#00d4aa" } },
        { type: "bar", name: "Misses", x: rows.map((row) => row.organization), y: rows.map((row) => row.total - row.hits), marker: { color: "#ff5577" } },
      ]}
      layout={plotLayout({
        barmode: "stack",
        height: 360,
        margin: { l: 40, r: 10, t: 40, b: 80 },
        xaxis: { ...AXIS_STYLE, tickangle: -30, automargin: true },
        yaxis: { ...AXIS_STYLE, automargin: true },
        legend: { orientation: "h", y: 1.15, x: 1, xanchor: "right", yanchor: "bottom" },
      })}
    />
  );
}

function DaysToTargetHistogram({ targetHit, firmContext }) {
  const rows = targetHit.filter((row) => Number(row.target_hit) === 1 && finiteNumber(row.days_to_target) !== null);
  if (!rows.length) return <div className="empty-state">No targets hit yet - check back as data matures</div>;
  const firms = firmContext.firms.filter((firm) => rows.some((row) => row.organization === firm));
  return (
    <PlotFrame
      data={firms.map((firm) => {
        const firmRows = rows.filter((row) => row.organization === firm);
        return {
          type: "histogram",
          name: firm,
          x: firmRows.map((row) => finiteNumber(row.days_to_target)),
          nbinsx: 20,
          marker: { color: firmContext.colorMap[firm] },
          opacity: 1,
          hovertemplate: "Days to Target: %{x}<br># Calls: %{y}<extra></extra>",
        };
      })}
      layout={plotLayout({
        height: 300,
        margin: { l: 64, r: 16, t: 10, b: 58 },
        xaxis: { ...AXIS_STYLE, title: { text: "Days to Target", standoff: 12 }, automargin: true },
        yaxis: { ...AXIS_STYLE, title: { text: "# Calls", standoff: 12 }, automargin: true },
        barmode: "stack",
        bargap: 0.1,
      })}
    />
  );
}

function StockPriceChart({ priceRows, stockReturns, firmContext }) {
  const validPrices = priceRows.filter((row) => row?.[0] && finiteNumber(row?.[1]) !== null);
  if (!validPrices.length) return <div className="empty-state">No price history found for this stock.</div>;

  const firms = sortStrings(stockReturns.map((row) => row.organization));
  const symbolMap = buildSymbolMap(firms);
  const priceTrace = {
    type: "scatter",
    mode: "lines",
    name: "Close",
    x: validPrices.map((row) => row[0]),
    y: validPrices.map((row) => finiteNumber(row[1])),
    line: { color: "#AEB7D8", width: 2.5 },
    hovertemplate: "Date: %{x|%d %b %Y}<br>Close: ₹%{y:,.2f}<extra></extra>",
  };
  const latestPriceDate = validPrices[validPrices.length - 1]?.[0];
  const recommendationTraces = firms
    .map((firm) => {
      const firmRows = stockReturns.filter((row) => row.organization === firm && row.recommend_date && finiteNumber(row.recommended_price) !== null);
      if (!firmRows.length) return null;
      return {
        type: "scatter",
        mode: "markers",
        name: firm,
        legendgroup: firm,
        x: firmRows.map((row) => row.recommend_date),
        y: firmRows.map((row) => finiteNumber(row.recommended_price)),
        text: firmRows.map((row) => row.analyst_recommendation),
        customdata: firmRows.map((row) => [formatMoney(row.target_price), formatPct(row.potential_returns), formatPct(row.return_current), directionLabel(row.direction_correct)]),
        marker: {
          color: firmContext.colorMap[firm],
          symbol: symbolMap[firm],
          size: 10,
          opacity: 0.9,
          line: { width: 1, color: "#0a0a0f" },
        },
        hovertemplate:
          "Firm: %{fullData.name}<br>Call: %{text}<br>Date: %{x|%d %b %Y}<br>Entry: ₹%{y:,.2f}<br>Target: %{customdata[0]}<br>Upside: %{customdata[1]}<br>Current Return: %{customdata[2]}<br>Direction: %{customdata[3]}<extra></extra>",
      };
    })
    .filter(Boolean);
  const targetLineTraces = latestPriceDate
    ? stockReturns
        .filter((row) => row.recommend_date && finiteNumber(row.target_price) !== null)
        .map((row) => {
          const firm = row.organization || "Unknown";
          const color = firmContext.colorMap[firm] || "#8d93b8";
          return {
            type: "scatter",
            mode: "lines",
            name: `${firm} target`,
            legendgroup: firm,
            showlegend: false,
            x: [row.recommend_date, latestPriceDate],
            y: [finiteNumber(row.target_price), finiteNumber(row.target_price)],
            text: [row.analyst_recommendation, row.analyst_recommendation],
            customdata: [
              [firm, formatDate(row.recommend_date), formatMoney(row.target_price), formatMoney(row.recommended_price), formatPct(row.potential_returns)],
              [firm, formatDate(row.recommend_date), formatMoney(row.target_price), formatMoney(row.recommended_price), formatPct(row.potential_returns)],
            ],
            line: { color, width: 1.5, dash: "dash" },
            opacity: 0.75,
            hovertemplate:
              "Firm: %{customdata[0]}<br>Call: %{text}<br>Reco Date: %{customdata[1]}<br>Entry: %{customdata[3]}<br>Target: %{customdata[2]}<br>Upside: %{customdata[4]}<extra></extra>",
          };
        })
    : [];

  return (
    <PlotFrame
      data={[priceTrace, ...targetLineTraces, ...recommendationTraces]}
      layout={plotLayout({
        height: 380,
        margin: { l: 72, r: 18, t: 36, b: 58 },
        xaxis: { ...AXIS_STYLE, title: { text: "Date", standoff: 12 }, automargin: true },
        yaxis: { ...AXIS_STYLE, title: { text: "Close Price ₹", standoff: 14 }, automargin: true },
        annotations: [
          {
            x: 0.99,
            y: 1.08,
            xref: "paper",
            yref: "paper",
            xanchor: "right",
            yanchor: "bottom",
            text: "Dashed lines = target price (colored by firm)",
            showarrow: false,
            bgcolor: "rgba(18, 18, 26, 0.75)",
            bordercolor: "#2a2a3e",
            borderwidth: 1,
            font: { size: 10, color: "#c8cbe0" },
          },
        ],
        legend: { title: { text: "Firm" }, orientation: "h", y: -0.25 },
      })}
    />
  );
}

function ScorecardPage({ scorecard, returns, firmContext }) {
  const columns = [
    { key: "organization", label: "Firm", width: "1.7fr" },
    { key: "total_calls", label: "Calls", width: "0.55fr" },
    { key: "earliest_call", label: "Since", render: formatDate },
    { key: "hit_rate_pct", label: "Dir. Accuracy", width: "0.8fr", render: formatPct, className: toneClass },
    { key: "avg_return_current", label: "Avg Current", width: "0.9fr", render: formatPct, className: toneClass },
    { key: "best_call_current", label: "Best", render: formatPct, className: toneClass },
    { key: "worst_call_current", label: "Worst", render: formatPct, className: toneClass },
    { key: "stdev_return_current", label: "Stdev", render: formatPct },
  ];
  return (
    <>
      <div className="two-col">
        <div>
          <SectionTitle>Performance by Firm</SectionTitle>
          <VirtualTable columns={columns} rows={scorecard} height={420} />
        </div>
        <div>
          <SectionTitle>Directional Accuracy Ranking</SectionTitle>
          <div className="metric-subtext" style={{ marginBottom: "14px", marginTop: "-4px", lineHeight: "1.4" }}>
            Ranks firms by the percentage of their recommendations that are currently tracking in a profitable direction.
          </div>
          <HitRateRankingChart scorecard={scorecard} />
        </div>
      </div>
      <SectionTitle>Return Distribution by Firm</SectionTitle>
      <div className="metric-subtext" style={{ marginBottom: "14px", marginTop: "-4px", lineHeight: "1.4" }}>
        Visualizes the spread of current returns for each firm's active recommendations. Highlights best and worst calls, along with the overall variance in performance.
      </div>
      <ReturnDistributionChart returns={returns} scorecard={scorecard} firmContext={firmContext} minCalls={3} />
    </>
  );
}

function AllCallsPage({ returns, firmContext }) {
  const [firm, setFirm] = useState("All");
  const [recommendation, setRecommendation] = useState("All");
  const [direction, setDirection] = useState("All");
  const firms = useMemo(() => ["All", ...sortStrings(returns.map((row) => row.organization))], [returns]);
  const recommendations = useMemo(() => ["All", ...sortStrings(returns.map((row) => row.analyst_recommendation))], [returns]);
  const filtered = returns.filter((row) => {
    if (firm !== "All" && row.organization !== firm) return false;
    if (recommendation !== "All" && row.analyst_recommendation !== recommendation) return false;
    if (direction === "Correct" && Number(row.direction_correct) !== 1) return false;
    if (direction === "Incorrect" && Number(row.direction_correct) !== 0) return false;
    return true;
  });
  return (
    <>
      <div className="filters">
        <Select label="Firm" value={firm} options={firms} onChange={setFirm} />
        <Select label="Recommendation" value={recommendation} options={recommendations} onChange={setRecommendation} />
        <Select label="Direction" value={direction} options={["All", "Correct", "Incorrect"]} onChange={setDirection} />
      </div>
      <SectionTitle>{filtered.length} Calls</SectionTitle>
      <div className="metric-subtext" style={{ marginBottom: "14px", marginTop: "-4px", lineHeight: "1.4" }}>
        Scatter plot comparing the promised target return against the actual current return. Calls above the diagonal line have exceeded their target, while calls in the red zone are currently tracking at a loss.
      </div>
      <PromisedVsActualChart rows={filtered} firmContext={firmContext} />
      <VirtualTable columns={allCallsColumns()} rows={filtered} height={500} />
    </>
  );
}

function TargetAnalysisPage({ targetHit, firmContext }) {
  return (
    <>
      <div className="two-col">
        <div>
          <SectionTitle>Target Hit Rate by Upside Bracket</SectionTitle>
          <div className="metric-subtext" style={{ marginBottom: "14px", marginTop: "-4px", lineHeight: "1.4" }}>
            Analyzes the reliability of analyst targets by grouping them into promised upside buckets. Helps answer whether high-return predictions are hit less frequently than modest ones.
          </div>
          <TargetHitRateByUpsideChart targetHit={targetHit} />
        </div>
        <div>
          <SectionTitle>Target Hit Rate by Firm</SectionTitle>
          <div className="metric-subtext" style={{ marginBottom: "14px", marginTop: "-4px", lineHeight: "1.4" }}>
            Compares the absolute number of successful hits versus misses across different analyst firms.
          </div>
          <TargetHitStackedChart targetHit={targetHit} />
        </div>
      </div>
      <SectionTitle>Days to Target (Hits Only)</SectionTitle>
      <div className="metric-subtext" style={{ marginBottom: "14px", marginTop: "-4px", lineHeight: "1.4" }}>
        Shows the distribution of time taken for successful recommendations to reach their promised target price. Helps identify typical realization timeframes for profitable calls.
      </div>
      <DaysToTargetHistogram targetHit={targetHit} firmContext={firmContext} />
      <SectionTitle>All Calls</SectionTitle>
      <VirtualTable columns={targetColumns()} rows={targetHit} height={460} />
    </>
  );
}

function StockLookupPage({ returns, stocks, firmContext }) {
  const [selectedStock, setSelectedStock] = useState(stocks[0]?.stock_name || "");
  const [priceRows, setPriceRows] = useState([]);
  const [priceState, setPriceState] = useState("idle");
  const stock = stocks.find((item) => item.stock_name === selectedStock);
  const stockReturns = returns.filter((row) => row.stock_name === selectedStock);
  const latest = stockReturns[0] || {};
  const avgReturn = stockReturns.reduce((sum, row) => sum + (Number(row.return_current) || 0), 0) / Math.max(stockReturns.length, 1);
  const avgTarget = stockReturns.reduce((sum, row) => sum + (Number(row.target_price) || 0), 0) / Math.max(stockReturns.length, 1);

  useEffect(() => {
    if (!stock?.symbol) {
      setPriceRows([]);
      setPriceState("missing");
      return;
    }
    let active = true;
    setPriceState("loading");
    fetchJson(`/data/price-history/${stock.symbol}.json`)
      .then((rows) => {
        if (!active) return;
        setPriceRows(rows);
        setPriceState("ready");
      })
      .catch(() => {
        if (!active) return;
        setPriceRows([]);
        setPriceState("missing");
      });
    return () => {
      active = false;
    };
  }, [stock?.symbol]);

  return (
    <>
      <div className="filters single-filter">
        <Select label="Select Stock" value={selectedStock} options={stocks.map((item) => item.stock_name)} onChange={setSelectedStock} />
      </div>
      <SectionTitle>
        {selectedStock || "Stock Lookup"} {stockReturns.length ? `- ${stockReturns.length} Recommendation(s)` : ""}
      </SectionTitle>
      {stockReturns.length ? (
        <>
          <div className="metric-grid four">
            <MetricCard label="Latest Call" value={latest.analyst_recommendation || ""} subtext={latest.organization || ""} />
            <MetricCard label="Avg Current Return" value={formatPct(avgReturn)} tone={avgReturn >= 0 ? "positive" : "negative"} />
            <MetricCard label="Avg Target Price" value={`₹${formatNum(avgTarget, 0)}`} />
            <MetricCard label="Firms Covering" value={new Set(stockReturns.map((row) => row.organization)).size} />
          </div>
          <SectionTitle>Price Chart with Analyst Overlays</SectionTitle>
          {priceState === "loading" ? (
            <div className="empty-state">Loading price history...</div>
          ) : (
            <StockPriceChart priceRows={priceRows} stockReturns={stockReturns} firmContext={firmContext} />
          )}
          <SectionTitle>Recommendations</SectionTitle>
          <VirtualTable columns={stockColumns()} rows={stockReturns} height={360} />
        </>
      ) : (
        <div className="empty-state">No data found for this stock.</div>
      )}
    </>
  );
}

function Select({ label, value, options, onChange }) {
  return (
    <label className="select-field">
      <span>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {options.map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
    </label>
  );
}

function RangeField({ label, min, max, value, onChange }) {
  return (
    <label className="range-field">
      <span>{label}</span>
      <div>
        <input type="range" min={min} max={max} value={value} onChange={(event) => onChange(Number(event.target.value))} />
        <strong>{value}</strong>
      </div>
    </label>
  );
}

function allCallsColumns() {
  return [
    { key: "organization", label: "Firm", width: "1.5fr" },
    { key: "stock_name", label: "Stock", width: "1.5fr" },
    { key: "analyst_recommendation", label: "Call", width: "0.7fr" },
    { key: "recommend_date", label: "Date", render: formatDate },
    { key: "recommended_price", label: "Entry ₹", render: formatMoney },
    { key: "target_price", label: "Target ₹", render: formatMoney },
    { key: "return_current", label: "Current %", render: formatPct, className: toneClass },
    { key: "potential_returns", label: "Upside %", render: formatPct, className: toneClass },
    { key: "days_alive", label: "Days" },
    { key: "direction_correct", label: "Direction", render: directionLabel, className: directionClass },
  ];
}

function targetColumns() {
  return [
    { key: "organization", label: "Firm", width: "1.5fr" },
    { key: "stock_name", label: "Stock", width: "1.6fr" },
    { key: "analyst_recommendation", label: "Call", width: "0.7fr" },
    { key: "recommend_date", label: "Date", render: formatDate },
    { key: "recommended_price", label: "Entry ₹", render: formatMoney },
    { key: "target_price", label: "Target ₹", render: formatMoney },
    { key: "target_upside_pct", label: "Upside %", render: formatPct, className: toneClass },
    { key: "target_hit", label: "Hit?", render: (value) => (Number(value) === 1 ? "Hit" : "Miss"), className: (value) => (Number(value) === 1 ? "positive" : "negative") },
    { key: "days_to_target", label: "Days", render: (value) => (finiteNumber(value) === null ? "" : String(Math.trunc(Number(value)))) },
    { key: "max_return_achieved", label: "Max Return %", render: formatPct, className: toneClass },
  ];
}

function stockColumns() {
  return [
    { key: "organization", label: "Firm", width: "1.5fr" },
    { key: "analyst_recommendation", label: "Call", width: "0.7fr" },
    { key: "recommend_date", label: "Date", render: formatDate },
    { key: "recommended_price", label: "Entry ₹", render: formatMoney },
    { key: "target_price", label: "Target ₹", render: formatMoney },
    { key: "potential_returns", label: "Upside %", render: formatPct, className: toneClass },
    { key: "return_30d", label: "30d %", render: formatPct, className: toneClass },
    { key: "return_current", label: "Current %", render: formatPct, className: toneClass },
    { key: "direction_correct", label: "Direction", render: (value) => (Number(value) === 1 ? "✓" : "✗"), className: directionClass },
  ];
}

function directionLabel(value) {
  return Number(value) === 1 ? "Correct" : "Incorrect";
}

function directionClass(value) {
  return Number(value) === 1 ? "positive" : "negative";
}

function toneClass(value) {
  if (value === null || value === undefined || value === "") return "";
  return Number(value) >= 0 ? "positive" : "negative";
}

function clamp(value, low, high) {
  return Math.max(low, Math.min(high, value));
}

function App() {
  const { status, error, data } = useDashboardData();
  const [tab, setTab] = useState(TABS[0]);

  if (status === "loading") {
    return <div className="app loading">Loading dashboard data...</div>;
  }
  if (status === "error") {
    return (
      <div className="app loading">
        <h1>Analyst Tracker</h1>
        <p>Static data files were not found. Run the export script before starting the dashboard.</p>
        <pre>{error.message}</pre>
      </div>
    );
  }

  const { scorecard, returns, targetHit, stocks, manifest } = data;
  const firmContext = buildFirmColorMap([...returns, ...targetHit, ...scorecard]);
  return (
    <main className="app">
      <header className="topbar">
        <div>
          <div className="eyebrow">NSE Equity Research</div>
          <h1>Analyst Tracker</h1>
        </div>
        <div className="refresh-note">Data generated {manifest.generated_at ? formatDate(manifest.generated_at) : ""}</div>
      </header>
      <DashboardMetrics returns={returns} targetHit={targetHit} />
      <div className="warning-note">Data refreshes from static JSON generated after the market-close pipeline.</div>
      <nav className="tabs">
        {TABS.map((item) => (
          <button key={item} className={tab === item ? "active" : ""} onClick={() => setTab(item)}>
            {item}
          </button>
        ))}
      </nav>
      {tab === "Scorecard" ? <ScorecardPage scorecard={scorecard} returns={returns} firmContext={firmContext} /> : null}
      {tab === "All Calls" ? <AllCallsPage returns={returns} firmContext={firmContext} /> : null}
      {tab === "Target Analysis" ? <TargetAnalysisPage targetHit={targetHit} firmContext={firmContext} /> : null}
      {tab === "Stock Lookup" ? <StockLookupPage returns={returns} stocks={stocks} firmContext={firmContext} /> : null}
      <footer>DATA REFRESHED DAILY - NSE EQUITY - FOR INFORMATIONAL USE ONLY</footer>
    </main>
  );
}

createRoot(document.getElementById("root")).render(<App />);
