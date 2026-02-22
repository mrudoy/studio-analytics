const FONT_BRAND = "'Cormorant Garamond', 'Times New Roman', serif";

/**
 * Swirl logo mark — the coiled spiral icon from the Sky Ting brand.
 * Renders at the given size (defaults to 28px) using currentColor.
 */
export function SkyTingSwirl({ size = 28, className }: { size?: number; className?: string }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 100 100"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      aria-label="Sky Ting logo"
    >
      <path
        d="M 20 72 C 10 64 4 46 12 30 C 20 14 40 6 58 10 C 76 14 90 30 88 50 C 86 70 70 84 50 84 C 30 84 18 70 22 52 C 26 34 40 24 56 28 C 72 32 78 48 72 60 C 66 72 52 76 42 70 C 32 64 30 52 36 44 C 42 36 50 34 54 40 C 58 46 56 52 50 52"
        stroke="currentColor"
        strokeWidth="7"
        strokeLinecap="round"
        fill="none"
      />
    </svg>
  );
}

/**
 * "SKY TING" wordmark in Cormorant Garamond, spaced uppercase.
 */
export function SkyTingWordmark({ className }: { className?: string }) {
  return (
    <span
      className={className}
      style={{
        fontFamily: FONT_BRAND,
        fontSize: "1.1rem",
        fontWeight: 400,
        letterSpacing: "0.35em",
        textTransform: "uppercase" as const,
        color: "var(--st-text-primary)",
      }}
    >
      SKY TING
    </span>
  );
}

/**
 * Combined logo — swirl mark + "SKY TING" wordmark side by side.
 */
export function SkyTingLogo({ className }: { className?: string }) {
  return (
    <div className={className} style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
      <SkyTingSwirl size={24} />
      <SkyTingWordmark />
    </div>
  );
}
