const tierColors: Record<string, { dot: string; bg: string; text: string }> = {
  CRITICAL: { dot: "bg-red-500", bg: "bg-red-50", text: "text-red-700" },
  HIGH: { dot: "bg-orange-500", bg: "bg-orange-50", text: "text-orange-700" },
  MEDIUM: { dot: "bg-yellow-500", bg: "bg-yellow-50", text: "text-yellow-700" },
  LOW: { dot: "bg-green-500", bg: "bg-green-50", text: "text-green-700" },
};

export default function RiskBadge({ tier }: { tier: string }) {
  const c = tierColors[tier] || tierColors.LOW;
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold ${c.bg} ${c.text}`}>
      <span className={`w-2 h-2 rounded-full ${c.dot}`} />
      {tier}
    </span>
  );
}
