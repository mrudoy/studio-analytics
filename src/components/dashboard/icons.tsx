import type { SVGProps } from "react";

type IconProps = SVGProps<SVGSVGElement> & { size?: number };

function iconDefaults(props: IconProps): SVGProps<SVGSVGElement> {
  const { size = 24, width, height, className, ...rest } = props;
  return {
    xmlns: "http://www.w3.org/2000/svg",
    width: width ?? size,
    height: height ?? size,
    viewBox: "0 0 24 24",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: 2,
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
    className,
    ...rest,
  };
}

/** Clipboard with dollar sign — Revenue section */
export function ReportMoney(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2" />
      <path d="M9 5a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2a2 2 0 0 1-2 2h-2a2 2 0 0 1-2-2" />
      <path d="M14 11h-2.5a1.5 1.5 0 0 0 0 3h1a1.5 1.5 0 0 1 0 3H10" />
      <path d="M12 17v1" />
      <path d="M12 10v1" />
    </svg>
  );
}

/** Ascending bar chart — Growth section */
export function ChartBarPopular(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M3 13a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v6a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1z" />
      <path d="M9 9a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1h-4a1 1 0 0 1-1-1z" />
      <path d="M15 5a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v14a1 1 0 0 1-1 1h-4a1 1 0 0 1-1-1z" />
      <path d="M4 20h14" />
    </svg>
  );
}

/** Forking arrows — Conversion section */
export function ArrowFork(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M16 3h5v5" />
      <path d="M8 3H3v5" />
      <path d="M21 3l-7.536 7.536A5 5 0 0 0 12 14.07V21" />
      <path d="M3 3l7.536 7.536A5 5 0 0 1 12 14.07V15" />
    </svg>
  );
}

/** Hourglass low — Churn section */
export function HourglassLow(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M6.5 17h11" />
      <path d="M6 20v-2a6 6 0 1 1 12 0v2a1 1 0 0 1-1 1H7a1 1 0 0 1-1-1" />
      <path d="M6 4v2a6 6 0 1 0 12 0V4a1 1 0 0 0-1-1H7a1 1 0 0 0-1 1" />
    </svg>
  );
}

/** Eyeglasses — Overview section */
export function Eyeglass(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M8 4H6L3 14" />
      <path d="M16 4h2l3 10" />
      <path d="M10 16h4" />
      <path d="M21 16.5a3.5 3.5 0 0 1-7 0V14h7z" />
      <path d="M10 16.5a3.5 3.5 0 0 1-7 0V14h7z" />
    </svg>
  );
}
