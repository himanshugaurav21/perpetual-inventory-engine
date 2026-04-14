import { type LucideIcon } from "lucide-react";

interface Props {
  label: string;
  value: string | number;
  sub?: string;
  icon: LucideIcon;
  color?: string;
}

export default function StatCard({ label, value, sub, icon: Icon, color = "primary" }: Props) {
  const bgMap: Record<string, string> = {
    primary: "bg-primary-50 text-primary-600",
    red: "bg-red-50 text-red-600",
    orange: "bg-orange-50 text-orange-600",
    amber: "bg-amber-50 text-amber-600",
    green: "bg-green-50 text-green-600",
  };

  return (
    <div className="bg-white rounded-xl border border-gray-100 p-5 hover:shadow-md transition-shadow">
      <div className="flex items-start justify-between">
        <div>
          <div className="text-xs font-semibold uppercase tracking-wider text-gray-400">{label}</div>
          <div className="text-2xl font-bold mt-1 text-gray-900">{value}</div>
          {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
        </div>
        <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${bgMap[color] || bgMap.primary}`}>
          <Icon className="w-5 h-5" />
        </div>
      </div>
    </div>
  );
}
