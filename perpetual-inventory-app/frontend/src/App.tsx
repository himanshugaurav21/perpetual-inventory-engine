import { Routes, Route, NavLink } from "react-router-dom";
import {
  Store, LayoutDashboard, AlertTriangle, ClipboardCheck,
  Bot, BarChart3,
} from "lucide-react";
import Dashboard from "./pages/Dashboard";
import AtRiskInventory from "./pages/AtRiskInventory";
import StoreValidation from "./pages/StoreValidation";
import AIAnalysis from "./pages/AIAnalysis";
import Analytics from "./pages/Analytics";

const navItems = [
  { to: "/", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/at-risk", icon: AlertTriangle, label: "At-Risk Inventory" },
  { to: "/validation", icon: ClipboardCheck, label: "Store Validation" },
  { to: "/analysis", icon: Bot, label: "AI Analysis" },
  { to: "/analytics", icon: BarChart3, label: "Analytics" },
];

export default function App() {
  return (
    <div className="flex min-h-screen">
      {/* Sidebar */}
      <aside className="w-64 bg-primary-900 text-white flex flex-col fixed h-full">
        {/* Logo */}
        <div className="p-5 border-b border-white/10">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-primary-500 rounded-lg flex items-center justify-center">
              <Store className="w-6 h-6 text-white" />
            </div>
            <div>
              <div className="font-bold text-lg leading-tight">FreshMart</div>
              <div className="text-xs text-white/50">Inventory Intelligence</div>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav className="flex-1 p-3 space-y-1">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === "/"}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  isActive
                    ? "bg-primary-500/15 text-primary-300"
                    : "text-white/60 hover:text-white hover:bg-white/5"
                }`
              }
            >
              <item.icon className="w-5 h-5" />
              {item.label}
            </NavLink>
          ))}
        </nav>

        {/* Footer */}
        <div className="p-4 text-xs text-white/30 border-t border-white/10">
          Powered by Databricks
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 ml-64 p-6 bg-primary-50 min-h-screen">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/at-risk" element={<AtRiskInventory />} />
          <Route path="/validation" element={<StoreValidation />} />
          <Route path="/analysis" element={<AIAnalysis />} />
          <Route path="/analytics" element={<Analytics />} />
        </Routes>
      </main>
    </div>
  );
}
