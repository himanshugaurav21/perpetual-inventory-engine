import { useState } from "react";
import { useApi } from "../hooks/useApi";
import RiskBadge from "../components/RiskBadge";
import SignalPanel from "../components/SignalPanel";
import { Bot, Loader2, MessageSquare, Search, Zap, Sparkles } from "lucide-react";

export default function AIAnalysis() {
  // Tab state
  const [activeTab, setActiveTab] = useState<"agent" | "genie">("agent");

  // Agent state
  const [skuId, setSkuId] = useState("");
  const [storeId, setStoreId] = useState("");
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  // Dropdown data
  const { data: topCritical } = useApi<any[]>("/api/anomalies/top-critical");
  const [selectedAnomaly, setSelectedAnomaly] = useState("");

  // Genie status
  const { data: genieStatus } = useApi<any>("/api/genie/status");

  // Genie state
  const [question, setQuestion] = useState("");
  const [genieResult, setGenieResult] = useState<any>(null);
  const [genieLoading, setGenieLoading] = useState(false);
  const [genieError, setGenieError] = useState("");
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [genieHistory, setGenieHistory] = useState<any[]>([]);

  const handleDropdownSelect = (val: string) => {
    setSelectedAnomaly(val);
    if (val) {
      const [sku, store] = val.split("|");
      setSkuId(sku);
      setStoreId(store);
    }
  };

  const runAnalysis = async () => {
    if (!skuId || !storeId) return;
    setLoading(true); setError(""); setResult(null);
    try {
      const r = await fetch(`/api/anomalies/${encodeURIComponent(skuId)}/${encodeURIComponent(storeId)}/analyze`, { method: "POST" });
      const data = await r.json();
      if (!r.ok) { setError(data.detail || `Error: ${r.status}`); return; }
      setResult(data);
    } catch (e: any) { setError(e.message); }
    finally { setLoading(false); }
  };

  const askGenie = async () => {
    if (!question.trim()) return;
    setGenieLoading(true); setGenieError(""); setGenieResult(null);
    try {
      const r = await fetch("/api/genie/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question: question.trim(),
          conversation_id: conversationId,
        }),
      });
      const data = await r.json();
      if (data.error) { setGenieError(data.error); setGenieResult(data); }
      else {
        setGenieResult(data);
        if (data.conversation_id) setConversationId(data.conversation_id);
        setGenieHistory(prev => [...prev, { question: question.trim(), result: data }]);
      }
      setQuestion("");
    } catch (e: any) { setGenieError(e.message); }
    finally { setGenieLoading(false); }
  };

  const startNewConversation = () => {
    setConversationId(null);
    setGenieHistory([]);
    setGenieResult(null);
    setGenieError("");
  };

  const sampleQuestions = [
    "Which SKUs are likely out-of-stock but show inventory?",
    "Which stores have the lowest PI accuracy?",
    "Show me critical anomalies in Electronics",
    "What is the total financial exposure by department?",
    "Which stores have the most ghost inventory value?",
  ];

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-bold text-gray-900">AI Analysis</h2>

      {/* Tab Switcher */}
      <div className="flex gap-1 bg-gray-100 rounded-lg p-1 w-fit">
        <button
          onClick={() => setActiveTab("agent")}
          className={`flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors ${
            activeTab === "agent" ? "bg-white text-primary-700 shadow-sm" : "text-gray-500 hover:text-gray-700"
          }`}
        >
          <Bot className="w-4 h-4" /> SKU Anomaly Agent
        </button>
        <button
          onClick={() => setActiveTab("genie")}
          className={`flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors ${
            activeTab === "genie" ? "bg-white text-primary-700 shadow-sm" : "text-gray-500 hover:text-gray-700"
          }`}
        >
          <Sparkles className="w-4 h-4" /> Genie — Ask a Question
        </button>
      </div>

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 1: SKU Anomaly Agent */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {activeTab === "agent" && (
        <>
          <div className="bg-white rounded-xl border border-gray-100 p-6">
            <h3 className="font-semibold text-gray-900 mb-4">Run AI Anomaly Analysis</h3>

            {/* Dropdown selector */}
            <div className="mb-4">
              <label className="text-xs text-gray-500 mb-1 block">Quick Select — Top Critical Anomalies</label>
              <select
                value={selectedAnomaly}
                onChange={(e) => handleDropdownSelect(e.target.value)}
                className="px-3 py-2 border rounded-lg text-sm w-full max-w-xl bg-white"
              >
                <option value="">Select a flagged SKU-store pair...</option>
                {(topCritical || []).map((a: any) => (
                  <option key={a.anomaly_id} value={`${a.sku_id}|${a.store_id}`}>
                    {a.sku_id} @ {a.store_name} — {a.category} — Score: {parseFloat(a.composite_risk_score).toFixed(3)} — ${parseFloat(a.financial_impact || "0").toLocaleString(undefined, { maximumFractionDigits: 0 })}
                  </option>
                ))}
              </select>
            </div>

            <div className="flex items-center gap-2 mb-4 text-xs text-gray-400">
              <div className="h-px bg-gray-200 flex-1" />
              <span>or enter manually</span>
              <div className="h-px bg-gray-200 flex-1" />
            </div>

            {/* Manual input */}
            <div className="flex gap-3 items-end">
              <div>
                <label className="text-xs text-gray-500 mb-1 block">SKU ID</label>
                <input value={skuId} onChange={e => setSkuId(e.target.value)}
                  placeholder="e.g. SKU-13062" className="px-3 py-2 border rounded-lg text-sm w-40" />
              </div>
              <div>
                <label className="text-xs text-gray-500 mb-1 block">Store ID</label>
                <input value={storeId} onChange={e => setStoreId(e.target.value)}
                  placeholder="e.g. STR-022" className="px-3 py-2 border rounded-lg text-sm w-40" />
              </div>
              <button onClick={runAnalysis} disabled={loading || !skuId || !storeId}
                className="flex items-center gap-2 px-4 py-2 bg-primary-600 text-white rounded-lg text-sm font-medium hover:bg-primary-700 disabled:opacity-50">
                {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Zap className="w-4 h-4" />}
                {loading ? "Analyzing..." : "Run AI Analysis"}
              </button>
            </div>
            {error && <div className="mt-3 text-red-500 text-sm">{error}</div>}
          </div>

          {/* Agent Results */}
          {result && (
            <div className="space-y-4">
              <div className="bg-white rounded-xl border border-gray-100 p-6">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="font-semibold text-gray-900">Risk Assessment</h3>
                  <RiskBadge tier={result.risk?.risk_tier || "LOW"} />
                </div>
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div><span className="text-gray-400">Score:</span> <span className="font-bold text-lg">{result.risk?.risk_score}</span></div>
                  <div><span className="text-gray-400">Confidence:</span> <span className="font-bold text-lg">{result.risk?.confidence}</span></div>
                  <div><span className="text-gray-400">Recommendation:</span> <span className="font-bold text-lg">{result.risk?.recommendation}</span></div>
                </div>
              </div>

              <div className="bg-white rounded-xl border border-gray-100 p-6">
                <h3 className="font-semibold text-gray-900 mb-4">Signal Breakdown</h3>
                <SignalPanel scores={result.signals || {}} />
              </div>

              <div className="bg-white rounded-xl border border-gray-100 p-6">
                <h3 className="font-semibold text-gray-900 mb-4">AI Explanation</h3>
                <p className="text-gray-700">{result.llm_analysis?.explanation}</p>
                {result.llm_analysis?.root_cause_hypothesis && (
                  <div className="mt-3 p-3 bg-primary-50 rounded-lg">
                    <div className="text-xs font-semibold text-primary-700 uppercase">Root Cause Hypothesis</div>
                    <p className="text-sm text-primary-900 mt-1">{result.llm_analysis.root_cause_hypothesis}</p>
                  </div>
                )}
                {result.llm_analysis?.suggested_action && (
                  <div className="mt-3 p-3 bg-amber-50 rounded-lg">
                    <div className="text-xs font-semibold text-amber-700 uppercase">Suggested Action</div>
                    <p className="text-sm text-amber-900 mt-1">{result.llm_analysis.suggested_action}</p>
                  </div>
                )}
                {result.llm_analysis?.llm_stats && (
                  <div className="mt-3 text-xs text-gray-400">
                    Model: {result.llm_analysis.llm_stats.model} ·
                    Latency: {result.llm_analysis.llm_stats.latency_ms}ms ·
                    Tokens: {result.llm_analysis.llm_stats.total_tokens} ·
                    Fallback: {result.llm_analysis.llm_stats.fallback ? "Yes" : "No"}
                  </div>
                )}
              </div>

              {(result.similar_patterns || []).length > 0 && (
                <div className="bg-white rounded-xl border border-gray-100 p-6">
                  <h3 className="font-semibold text-gray-900 mb-4">Similar Historical Patterns</h3>
                  <div className="space-y-2">
                    {result.similar_patterns.map((p: any, i: number) => (
                      <div key={i} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg text-sm">
                        <div>
                          <span className="font-medium">{p.sku_name || p.anomaly_id}</span>
                          <span className="text-gray-400 ml-2">{p.category} · {p.primary_anomaly_type}</span>
                        </div>
                        <RiskBadge tier={p.risk_tier || "MEDIUM"} />
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <div className="text-xs text-gray-400 flex gap-4">
                <span>Total: {result.performance?.total_ms}ms</span>
                <span>Data: {result.performance?.data_lookup_ms}ms</span>
                <span>Signals: {result.performance?.step2_ms}ms</span>
                <span>LLM: {result.performance?.step4_ms}ms</span>
                <span>VS: {result.performance?.vs_ms}ms</span>
              </div>
            </div>
          )}
        </>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 2: Ask a Question (Genie-style NL Query) */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {activeTab === "genie" && (
        <>
          <div className="bg-white rounded-xl border border-gray-100 p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="font-semibold text-gray-900">Ask About Inventory</h3>
              <span className={`text-xs px-2 py-1 rounded-full ${
                genieStatus?.genie_enabled
                  ? "bg-purple-50 text-purple-700"
                  : "bg-primary-50 text-primary-700"
              }`}>
                {genieStatus?.genie_enabled ? "Powered by Databricks Genie" : "Powered by AI SQL Generation"}
              </span>
            </div>
            <p className="text-sm text-gray-400 mb-4">
              {genieStatus?.genie_enabled
                ? "Ask any question in natural language. Databricks Genie will interpret your question and query your inventory data."
                : "Ask any question in natural language. AI will generate a SQL query and return results from your inventory data. To enable Genie, set the GENIE_SPACE_ID environment variable."}
            </p>

            <div className="flex gap-3">
              <input
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && askGenie()}
                placeholder={conversationId ? "Ask a follow-up question..." : "e.g. Which stores have the most ghost inventory?"}
                className="flex-1 px-4 py-2.5 border rounded-lg text-sm"
              />
              <button onClick={askGenie} disabled={genieLoading || !question.trim()}
                className="flex items-center gap-2 px-5 py-2.5 bg-primary-600 text-white rounded-lg text-sm font-medium hover:bg-primary-700 disabled:opacity-50">
                {genieLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
                {genieLoading ? "Querying..." : "Ask"}
              </button>
              {conversationId && (
                <button onClick={startNewConversation}
                  className="px-3 py-2.5 border border-gray-200 rounded-lg text-sm text-gray-500 hover:bg-gray-50">
                  New Chat
                </button>
              )}
            </div>

            {/* Sample questions */}
            <div className="mt-4">
              <div className="text-xs text-gray-400 mb-2">Try these:</div>
              <div className="flex flex-wrap gap-2">
                {sampleQuestions.map((q) => (
                  <button
                    key={q}
                    onClick={() => { setQuestion(q); }}
                    className="px-3 py-1.5 bg-primary-50 text-primary-700 rounded-full text-xs hover:bg-primary-100 transition-colors"
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          </div>

          {genieError && !genieResult && (
            <div className="bg-red-50 border border-red-100 rounded-xl p-4 text-red-700 text-sm">{genieError}</div>
          )}

          {/* Genie Results */}
          {genieResult && (
            <div className="space-y-4">
              {/* Genie Description */}
              {genieResult.description && (
                <div className="bg-purple-50 border border-purple-100 rounded-xl p-4">
                  <div className="text-xs font-semibold text-purple-700 uppercase mb-1">Genie Interpretation</div>
                  <p className="text-sm text-purple-900">{genieResult.description}</p>
                </div>
              )}

              {/* Generated SQL */}
              <div className="bg-white rounded-xl border border-gray-100 p-6">
                <div className="flex items-center justify-between mb-2">
                  <h3 className="font-semibold text-gray-900">Generated SQL</h3>
                  {genieResult.source && (
                    <span className={`text-xs px-2 py-0.5 rounded-full ${
                      genieResult.source === "genie" ? "bg-purple-50 text-purple-600" : "bg-primary-50 text-primary-600"
                    }`}>
                      {genieResult.source === "genie" ? "via Genie" : "via AI SQL"}
                    </span>
                  )}
                </div>
                <pre className="bg-gray-900 text-green-400 p-4 rounded-lg text-xs overflow-x-auto whitespace-pre-wrap">
                  {genieResult.sql}
                </pre>
                {genieResult.query_ms && (
                  <div className="mt-2 text-xs text-gray-400">
                    {genieResult.row_count} rows returned in {genieResult.query_ms}ms
                  </div>
                )}
              </div>

              {genieResult.error && (
                <div className="bg-red-50 border border-red-100 rounded-xl p-4 text-red-700 text-sm">
                  {genieResult.error}
                </div>
              )}

              {/* Results Table */}
              {genieResult.results && genieResult.results.length > 0 && (
                <div className="bg-white rounded-xl border border-gray-100 overflow-hidden">
                  <div className="p-4 border-b border-gray-100">
                    <h3 className="font-semibold text-gray-900">Results</h3>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50">
                        <tr>
                          {Object.keys(genieResult.results[0]).map((col) => (
                            <th key={col} className="px-4 py-2 text-left text-gray-500 font-medium text-xs whitespace-nowrap">
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {genieResult.results.map((row: any, i: number) => (
                          <tr key={i} className="border-t border-gray-50 hover:bg-gray-50">
                            {Object.values(row).map((val: any, j: number) => (
                              <td key={j} className="px-4 py-2 text-gray-700 whitespace-nowrap">
                                {val === null ? <span className="text-gray-300">null</span> : String(val)}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {genieResult.results && genieResult.results.length === 0 && !genieResult.error && (
                <div className="bg-gray-50 rounded-xl p-6 text-center text-gray-400">No results returned.</div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
