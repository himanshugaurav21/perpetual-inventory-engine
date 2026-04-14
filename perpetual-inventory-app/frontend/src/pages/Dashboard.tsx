import { useApi } from "../hooks/useApi";
import StatCard from "../components/StatCard";
import {
  Package, AlertTriangle, ShieldAlert, DollarSign, Activity, Store,
} from "lucide-react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from "recharts";

const PIE_COLORS = ["#dc2626", "#f97316", "#eab308", "#22c55e"];

export default function Dashboard() {
  const { data, loading } = useApi<any>("/api/dashboard/summary");

  if (loading) return <div className="text-gray-400 p-8">Loading dashboard...</div>;
  if (!data) return <div className="text-red-500 p-8">Failed to load dashboard</div>;

  const m = data.metrics || {};
  const tierData = [
    { name: "CRITICAL", value: parseInt(m.critical_count || "0") },
    { name: "HIGH", value: parseInt(m.high_count || "0") },
    { name: "MEDIUM", value: parseInt(m.medium_count || "0") },
    { name: "LOW", value: parseInt(m.low_count || "0") },
  ];

  return (
    <div className="space-y-6">
      {/* Hero Card */}
      <div className="relative bg-gradient-to-r from-primary-900 to-primary-800 rounded-2xl p-8 text-white overflow-hidden shadow-xl">
        <div className="absolute top-0 right-0 w-64 h-64 bg-primary-500 rounded-full opacity-5 -translate-y-1/2 translate-x-1/4" />
        <div className="absolute bottom-0 left-1/2 w-48 h-48 bg-primary-400 rounded-full opacity-5 translate-y-1/2" />
        <div className="relative flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Inventory Accuracy Dashboard</h1>
            <p className="text-primary-200 mt-1">FreshMart Perpetual Inventory Intelligence</p>
          </div>
          <div className="text-right">
            <div className="text-4xl font-bold">{m.avg_pi_accuracy || "—"}%</div>
            <div className="text-primary-300 text-sm">Avg PI Accuracy</div>
          </div>
        </div>
        <div className="relative flex gap-8 mt-6 pt-4 border-t border-white/10">
          <div>
            <div className="text-2xl font-bold">{parseInt(m.total_monitored || "0").toLocaleString()}</div>
            <div className="text-primary-300 text-xs">SKU-Store Pairs</div>
          </div>
          <div>
            <div className="text-2xl font-bold text-amber-400">${parseFloat(m.total_financial_exposure || "0").toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
            <div className="text-primary-300 text-xs">Financial Exposure</div>
          </div>
          <div>
            <div className="text-2xl font-bold">{parseInt(m.at_risk_count || "0").toLocaleString()}</div>
            <div className="text-primary-300 text-xs">At-Risk Items</div>
          </div>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard label="Total Monitored" value={parseInt(m.total_monitored || "0").toLocaleString()} icon={Package} />
        <StatCard label="Critical" value={m.critical_count || "0"} icon={ShieldAlert} color="red" sub="Immediate action needed" />
        <StatCard label="High Risk" value={m.high_count || "0"} icon={AlertTriangle} color="orange" sub="Close monitoring" />
        <StatCard label="Financial Exposure" value={`$${parseFloat(m.total_financial_exposure || "0").toLocaleString(undefined, { maximumFractionDigits: 0 })}`} icon={DollarSign} color="amber" />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Risk Tier Distribution */}
        <div className="bg-white rounded-xl border border-gray-100 p-6">
          <h3 className="font-semibold text-gray-900 mb-4">Risk Tier Distribution</h3>
          <ResponsiveContainer width="100%" height={250}>
            <PieChart>
              <Pie data={tierData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={90} label={({ name, value }) => `${name}: ${value}`}>
                {tierData.map((_, i) => <Cell key={i} fill={PIE_COLORS[i]} />)}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Anomaly Types */}
        <div className="bg-white rounded-xl border border-gray-100 p-6">
          <h3 className="font-semibold text-gray-900 mb-4">Anomaly by Category</h3>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={data.categories || []} layout="vertical">
              <XAxis type="number" />
              <YAxis type="category" dataKey="category" width={100} tick={{ fontSize: 12 }} />
              <Tooltip />
              <Bar dataKey="count" fill="#0d9488" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
