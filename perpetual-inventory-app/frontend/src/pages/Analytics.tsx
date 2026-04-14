import { useApi } from "../hooks/useApi";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

export default function Analytics() {
  const { data: trends, loading } = useApi<any>("/api/analytics/trends");
  const { data: stores } = useApi<any>("/api/stores/health");

  if (loading) return <div className="text-gray-400 p-8">Loading analytics...</div>;

  const anomalyTypes = trends?.anomaly_types || [];
  const deptAccuracy = trends?.department_accuracy || [];
  const storeList = stores?.stores || [];

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-bold text-gray-900">Analytics</h2>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Anomaly Types */}
        <div className="bg-white rounded-xl border border-gray-100 p-6">
          <h3 className="font-semibold text-gray-900 mb-4">Anomaly Types</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={anomalyTypes} layout="vertical">
              <XAxis type="number" />
              <YAxis type="category" dataKey="primary_anomaly_type" width={140} tick={{ fontSize: 11 }} />
              <Tooltip />
              <Bar dataKey="count" fill="#0d9488" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Department Accuracy */}
        <div className="bg-white rounded-xl border border-gray-100 p-6">
          <h3 className="font-semibold text-gray-900 mb-4">Department PI Accuracy</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={deptAccuracy}>
              <XAxis dataKey="department" tick={{ fontSize: 11 }} />
              <YAxis domain={[0, 100]} />
              <Tooltip />
              <Bar dataKey="accuracy_pct" fill="#14b8a6" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Store Health Heatmap */}
      <div className="bg-white rounded-xl border border-gray-100 p-6">
        <h3 className="font-semibold text-gray-900 mb-4">Store Health Overview</h3>
        <div className="grid grid-cols-5 md:grid-cols-10 gap-2">
          {storeList.slice(0, 50).map((s: any) => {
            const acc = parseFloat(s.pi_accuracy_pct || "100");
            const bg = acc >= 95 ? "bg-green-200" : acc >= 90 ? "bg-green-100" : acc >= 85 ? "bg-yellow-100" : acc >= 80 ? "bg-orange-100" : "bg-red-100";
            const text = acc >= 90 ? "text-green-800" : acc >= 85 ? "text-yellow-800" : "text-red-800";
            return (
              <div key={s.store_id} className={`${bg} rounded-lg p-2 text-center`} title={`${s.store_name}: ${acc}% accuracy`}>
                <div className={`text-xs font-bold ${text}`}>{acc}%</div>
                <div className="text-[10px] text-gray-500 truncate">{s.store_id}</div>
              </div>
            );
          })}
        </div>
        <div className="flex gap-4 mt-3 text-xs text-gray-400">
          <span className="flex items-center gap-1"><span className="w-3 h-3 bg-green-200 rounded" /> 95%+</span>
          <span className="flex items-center gap-1"><span className="w-3 h-3 bg-green-100 rounded" /> 90-95%</span>
          <span className="flex items-center gap-1"><span className="w-3 h-3 bg-yellow-100 rounded" /> 85-90%</span>
          <span className="flex items-center gap-1"><span className="w-3 h-3 bg-orange-100 rounded" /> 80-85%</span>
          <span className="flex items-center gap-1"><span className="w-3 h-3 bg-red-100 rounded" /> &lt;80%</span>
        </div>
      </div>

      {/* Store Table */}
      <div className="bg-white rounded-xl border border-gray-100 overflow-hidden">
        <div className="p-4 border-b border-gray-100">
          <h3 className="font-semibold text-gray-900">Store Rankings (by PI Accuracy)</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-2 text-left text-gray-500 font-medium">Store</th>
                <th className="px-4 py-2 text-left text-gray-500 font-medium">Region</th>
                <th className="px-4 py-2 text-right text-gray-500 font-medium">PI Accuracy</th>
                <th className="px-4 py-2 text-right text-gray-500 font-medium">Critical</th>
                <th className="px-4 py-2 text-right text-gray-500 font-medium">High</th>
                <th className="px-4 py-2 text-right text-gray-500 font-medium">Ghost $ Value</th>
                <th className="px-4 py-2 text-right text-gray-500 font-medium">Shrinkage $</th>
              </tr>
            </thead>
            <tbody>
              {storeList.slice(0, 20).map((s: any) => (
                <tr key={s.store_id} className="border-t border-gray-50 hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium">{s.store_name}</td>
                  <td className="px-4 py-2 text-gray-500">{s.region}</td>
                  <td className="px-4 py-2 text-right font-medium">{s.pi_accuracy_pct}%</td>
                  <td className="px-4 py-2 text-right text-red-600 font-medium">{s.critical_risk_skus}</td>
                  <td className="px-4 py-2 text-right text-orange-600">{s.high_risk_skus}</td>
                  <td className="px-4 py-2 text-right">${parseFloat(s.total_ghost_inventory_value || "0").toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                  <td className="px-4 py-2 text-right">${parseFloat(s.estimated_shrinkage_dollars || "0").toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
