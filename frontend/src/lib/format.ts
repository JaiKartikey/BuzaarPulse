const inr = new Intl.NumberFormat("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const whole = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 0 });

/** 1234567.5 -> "12,34,567.50" (Indian digit grouping). */
export function price(value: number | null | undefined): string {
  return value == null || Number.isNaN(value) ? "–" : inr.format(value);
}

/** Money with the rupee sign, without paise for large amounts where they are noise. */
export function money(value: number | null | undefined, { paise = false } = {}): string {
  if (value == null || Number.isNaN(value)) return "–";
  const sign = value < 0 ? "−" : "";
  return `${sign}₹${(paise ? inr : whole).format(Math.abs(value))}`;
}

/** A signed percentage: "+1.24%", "−0.80%", "0.00%". Uses a real minus sign so columns align. */
export function percent(value: number | null | undefined, digits = 2): string {
  if (value == null || Number.isNaN(value)) return "–";
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}${Math.abs(value).toFixed(digits)}%`;
}

/** 1,25,000 -> "1.25L", 3,40,00,000 -> "3.40Cr": how Indian markets quote volume. */
export function compact(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return "–";
  const abs = Math.abs(value);
  if (abs >= 1e7) return `${(value / 1e7).toFixed(2)}Cr`;
  if (abs >= 1e5) return `${(value / 1e5).toFixed(2)}L`;
  if (abs >= 1e3) return `${(value / 1e3).toFixed(1)}K`;
  return String(Math.round(value));
}

export function direction(value: number | null | undefined): "up" | "down" | "flat" {
  return !value ? "flat" : value > 0 ? "up" : "down";
}

export const toneClass = { up: "text-up", down: "text-down", flat: "text-muted" } as const;

export function clock(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}

export function ago(epochSeconds: number, now = Date.now() / 1000): string {
  const seconds = Math.max(0, Math.round(now - epochSeconds));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  return `${Math.floor(seconds / 3600)}h ago`;
}
