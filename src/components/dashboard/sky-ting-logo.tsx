import { Yoga } from "./icons";

/**
 * Yoga pose logo mark for the sidebar header.
 * Renders at the given size (defaults to 28px) using currentColor.
 */
export function SkyTingSwirl({ size = 28, className }: { size?: number; className?: string }) {
  return <Yoga className={className} size={size} />;
}

/**
 * "SKY TING" wordmark in Helvetica, spaced uppercase.
 */
export function SkyTingWordmark({ className }: { className?: string }) {
  return (
    <span
      className={className}
      style={{
        fontFamily: "Helvetica, Arial, sans-serif",
        fontSize: "1.1rem",
        fontWeight: 500,
        letterSpacing: "0.3em",
        textTransform: "uppercase" as const,
        color: "var(--st-text-primary)",
      }}
    >
      SKY TING
    </span>
  );
}

/**
 * Combined logo — yoga mark + "SKY TING" wordmark side by side.
 */
export function SkyTingLogo({ className }: { className?: string }) {
  return (
    <div className={className} style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
      <SkyTingSwirl size={24} />
      <SkyTingWordmark />
    </div>
  );
}
