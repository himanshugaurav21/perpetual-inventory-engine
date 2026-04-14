import { useState } from "react";
import { useApi } from "../hooks/useApi";
import RiskBadge from "../components/RiskBadge";
import SignalPanel from "../components/SignalPanel";
import { ChevronDown, ChevronUp, Search } from "lucide-react";

export default function AtRiskInventory() {
  const [tier, setTier] = useState("");
  const [category, setCategory] = useState("");
  const [expanded, setExpanded] = useState<string | null>(null);

  const params = new URLSearchParams();
  if (tier) params.set("risk_tier", tier);
  if (category) params.set("category", category);
  params.set("limit", "100");

  const { data, loading } = useApi<any>(`/api/anomalies?${params}`);

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-bold text-gray-900">At-Risk Inventory</h2>

      {/* Filters */}
      <div className="flex gap-3 flex-wrap">
        <select value={tier} onChange={e => setTier(e.target.value)}
          className="px-3 py-2 border rounded-lg text-sm bg-white">
          <option value="">All Tiers</option>
          <option value="CRITICAL">Critical</option>
          <option value="HIGH">High</option>
        </select>
        <select value={category} onChange={e => setCategory(e.target.value)}
          className="px-3 py-2 border rounded-lg text-sm bg-white">
          <option value="">All Categories</option>
          {["Grocery","Electronics","Apparel","Home","Health & Beauty","Toys","Sports"].map(c =>
            <option key={c} value={c}>{c}</option>
          )}
        </select>
      </div>

      {loading ? <div className="text-gray-400">Loading...</div> : (
        <div className="space-y-2">
          {(data?.anomalies || []).map((a: any) => (
            <div key={a.anomaly_id} className="bg-white rounded-xl border border-gray-100 overflow-hidden">
              <button
                onClick={() => setExpanded(expanded === a.anomaly_id ? null : a.anomaly_id)}
                className="w-full flex items-center justify-between p-4 hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center gap-4 text-left">
                  <RiskBadge tier={a.risk_tier} />
                  <div>
                    <div className="font-medium text-gray-900">{a.sku_name || a.sku_id}</div>
                    <div className="text-xs text-gray-400">{a.store_name} · {a.category} · {a.primary_anomaly_type}</div>
                  </div>
                </div>
                <div className="flex items-center gap-4">
                  <div className="text-right">
                    <div className="text-sm font-semibold text-gray-900">${parseFloat(a.financial_impact || "0").toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
                    <div className="text-xs text-gray-400">Score: {parseFloat(a.composite_risk_score || "0").toFixed(3)}</div>
                  </div>
                  {expanded === a.anomaly_id ? <ChevronUp className="w-4 h-4 text-gray-400" /> : <ChevronDown className="w-4 h-4 text-gray-400" />}
                </div>
              </button>
              {expanded === a.anomaly_id && (
                <div className="px-4 pb-4 border-t border-gray-50">
                  <div className="grid grid-cols-3 gap-4 mt-3 text-sm">
                    <div><span className="text-gray-400">System Qty:</span> <span className="font-medium">{a.system_quantity}</span></div>
                    <div><span className="text-gray-400">Discrepancy:</span> <span className="font-medium">{a.stock_discrepancy}</span></div>
                    <div><span className="text-gray-400">Rank:</span> <span className="font-medium">#{a.priority_rank}</span></div>
                  </div>
                  <div className="mt-3 text-sm text-gray-600">{a.explanation_text}</div>
                  <div className="mt-2 text-sm text-primary-700 font-medium">{a.recommended_action}</div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
