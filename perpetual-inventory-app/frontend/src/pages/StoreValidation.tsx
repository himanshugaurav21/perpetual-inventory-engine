import { useState } from "react";
import { useApi } from "../hooks/useApi";
import RiskBadge from "../components/RiskBadge";
import { CheckCircle, XCircle, Search as SearchIcon } from "lucide-react";

export default function StoreValidation() {
  const [storeId, setStoreId] = useState("STR-001");
  const { data, loading, refetch } = useApi<any>(`/api/validations/queue/${storeId}`);
  const [submitting, setSubmitting] = useState<string | null>(null);
  const [notes, setNotes] = useState("");
  const [physCount, setPhysCount] = useState("");

  const submitValidation = async (skuId: string, type: string) => {
    setSubmitting(skuId);
    try {
      await fetch("/api/validations", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sku_id: skuId, store_id: storeId,
          validation_type: type,
          physical_count: physCount ? parseInt(physCount) : null,
          notes, validated_by: "store_team",
        }),
      });
      setNotes(""); setPhysCount("");
      refetch();
    } finally { setSubmitting(null); }
  };

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-bold text-gray-900">Store Validation</h2>

      <div className="flex gap-3 items-center">
        <select value={storeId} onChange={e => setStoreId(e.target.value)}
          className="px-3 py-2 border rounded-lg text-sm bg-white">
          {Array.from({ length: 50 }, (_, i) => {
            const id = `STR-${String(i + 1).padStart(3, "0")}`;
            return <option key={id} value={id}>{id}</option>;
          })}
        </select>
        <span className="text-sm text-gray-400">{data?.count || 0} pending items</span>
      </div>

      {loading ? <div className="text-gray-400">Loading...</div> : (
        <div className="space-y-3">
          {(data?.items || []).map((item: any) => (
            <div key={item.anomaly_id} className="bg-white rounded-xl border border-gray-100 p-5">
              <div className="flex items-start justify-between">
                <div>
                  <div className="flex items-center gap-2">
                    <RiskBadge tier={item.risk_tier} />
                    <span className="font-medium text-gray-900">{item.sku_name || item.sku_id}</span>
                  </div>
                  <div className="text-sm text-gray-500 mt-1">{item.category} · System Qty: {item.system_quantity}</div>
                  <div className="text-sm text-gray-600 mt-2">{item.explanation_text}</div>
                </div>
                <div className="text-right">
                  <div className="text-sm font-semibold">${parseFloat(item.financial_impact || "0").toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
                  <div className="text-xs text-gray-400">Score: {parseFloat(item.composite_risk_score || "0").toFixed(3)}</div>
                </div>
              </div>

              {/* Validation Controls */}
              <div className="mt-4 pt-3 border-t border-gray-50 space-y-3">
                <div className="flex gap-3">
                  <input type="number" placeholder="Physical count"
                    value={physCount} onChange={e => setPhysCount(e.target.value)}
                    className="px-3 py-1.5 border rounded-lg text-sm w-32" />
                  <input type="text" placeholder="Notes..."
                    value={notes} onChange={e => setNotes(e.target.value)}
                    className="px-3 py-1.5 border rounded-lg text-sm flex-1" />
                </div>
                <div className="flex gap-2">
                  <button onClick={() => submitValidation(item.sku_id, "confirmed")}
                    disabled={submitting === item.sku_id}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-red-50 text-red-700 rounded-lg text-sm font-medium hover:bg-red-100">
                    <CheckCircle className="w-4 h-4" /> Confirm Ghost
                  </button>
                  <button onClick={() => submitValidation(item.sku_id, "dismissed")}
                    disabled={submitting === item.sku_id}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-green-50 text-green-700 rounded-lg text-sm font-medium hover:bg-green-100">
                    <XCircle className="w-4 h-4" /> Dismiss
                  </button>
                  <button onClick={() => submitValidation(item.sku_id, "investigated")}
                    disabled={submitting === item.sku_id}
                    className="px-3 py-1.5 bg-amber-50 text-amber-700 rounded-lg text-sm font-medium hover:bg-amber-100">
                    Investigate
                  </button>
                </div>
              </div>
            </div>
          ))}
          {(data?.items || []).length === 0 && (
            <div className="text-center text-gray-400 py-12">No pending validations for this store</div>
          )}
        </div>
      )}
    </div>
  );
}
