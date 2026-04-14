const signals = [
  { key: "velocity_score", label: "Velocity", desc: "Sales velocity consistency" },
  { key: "stock_consistency_score", label: "Stock Consistency", desc: "Calculated vs reported PI" },
  { key: "adjustment_score", label: "Adjustments", desc: "Suspicious adjustment patterns" },
  { key: "shrinkage_score", label: "Shrinkage", desc: "Unexplained inventory loss" },
  { key: "shipment_gap_score", label: "Shipment Gap", desc: "Replenishment overdue" },
];

function barColor(score: number) {
  if (score >= 0.75) return "bg-red-500";
  if (score >= 0.5) return "bg-orange-500";
  if (score >= 0.3) return "bg-yellow-500";
  return "bg-green-500";
}

export default function SignalPanel({ scores }: { scores: Record<string, any> }) {
  return (
    <div className="space-y-3">
      {signals.map((s) => {
        const val = parseFloat(scores?.[s.key] || "0");
        return (
          <div key={s.key}>
            <div className="flex justify-between text-sm mb-1">
              <span className="font-medium text-gray-700">{s.label}</span>
              <span className="text-gray-500">{val.toFixed(2)}</span>
            </div>
            <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
              <div className={`h-full rounded-full ${barColor(val)}`} style={{ width: `${val * 100}%` }} />
            </div>
            <div className="text-xs text-gray-400 mt-0.5">{s.desc}</div>
          </div>
        );
      })}
    </div>
  );
}
