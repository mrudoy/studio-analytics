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

/** Recycle arrows — Auto-Renews */
export function Recycle(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="m12 17l-2 2l2 2" />
      <path d="M10 19h9a2 2 0 0 0 1.75-2.75l-.55-1" />
      <path d="M8.536 11l-.732-2.732L5.072 9" />
      <path d="m7.804 8.268l-4.5 7.794a2 2 0 0 0 1.506 2.89l1.141.024" />
      <path d="M15.464 11l2.732.732L18.928 9" />
      <path d="m18.196 11.732l-4.5-7.794a2 2 0 0 0-3.256-.14l-.591.976" />
    </svg>
  );
}

/** Recycle with slash — Non-Auto-Renews */
export function RecycleOff(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="m12 17l-2 2l2 2" />
      <path d="M10 19h9" />
      <path d="M21.896 16.929a2 2 0 0 0-.146-.679l-.55-1" />
      <path d="M8.536 11l-.732-2.732L5.072 9" />
      <path d="m7.804 8.268l-4.5 7.794a2 2 0 0 0 1.506 2.89l1.141.024" />
      <path d="M15.464 11l2.732.732L18.928 9" />
      <path d="m18.196 11.732l-4.5-7.794a2 2 0 0 0-3.256-.14l-.591.976" />
      <path d="M3 3l18 18" />
    </svg>
  );
}

/** User with plus — New Customers */
export function UserPlus(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M8 7a4 4 0 1 0 8 0a4 4 0 0 0-8 0" />
      <path d="M16 19h6" />
      <path d="M19 16v6" />
      <path d="M6 21v-2a4 4 0 0 1 4-4h4" />
    </svg>
  );
}

/** Users group — All Non-Auto-Renew Customers */
export function UsersGroup(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M10 13a2 2 0 1 0 4 0a2 2 0 0 0-4 0" />
      <path d="M8 21v-1a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v1" />
      <path d="M15 5a2 2 0 1 0 4 0a2 2 0 0 0-4 0" />
      <path d="M17 10h2a2 2 0 0 1 2 2v1" />
      <path d="M5 5a2 2 0 1 0 4 0a2 2 0 0 0-4 0" />
      <path d="M3 13v-1a2 2 0 0 1 2-2h2" />
    </svg>
  );
}

/** Database cylinder — Data section */
export function Database(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M4 6a8 3 0 1 0 16 0A8 3 0 1 0 4 6" />
      <path d="M4 6v6a8 3 0 0 0 16 0V6" />
      <path d="M4 12v6a8 3 0 0 0 16 0v-6" />
    </svg>
  );
}

/** Yoga pose — sidebar logo */
export function Yoga(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M11 4a1 1 0 1 0 2 0a1 1 0 1 0-2 0" />
      <path d="M4 20h4l1.5-3m7.5 3l-1-5h-5l1-7" />
      <path d="m4 10l4-1l4-1l4 1.5l4 1.5" />
    </svg>
  );
}

/** Arrow badge down — Members subscription */
export function ArrowBadgeDown(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M17 13V7l-5 4l-5-4v6l5 4z" />
    </svg>
  );
}

/** Sky butterfly — SKY3 / Packs subscription */
export function BrandSky(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M6.335 5.144C4.681 3.945 2 3.017 2 5.97c0 .59.35 4.953.556 5.661C3.269 14.094 5.686 14.381 8 14c-4.045.665-4.889 3.208-2.667 5.41C6.363 20.428 7.246 21 8 21c2 0 3.134-2.769 3.5-3.5q.5-1 .5-1.5q0 .5.5 1.5c.366.731 1.5 3.5 3.5 3.5c.754 0 1.637-.571 2.667-1.59C20.889 17.207 20.045 14.664 16 14c2.314.38 4.73.094 5.444-2.369c.206-.708.556-5.072.556-5.661c0-2.953-2.68-2.025-4.335-.826C15.372 6.806 12.905 10.192 12 12c-.905-1.808-3.372-5.194-5.665-6.856" />
    </svg>
  );
}

/** TV device — Sky Ting TV subscription */
export function DeviceTv(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M3 9a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z" />
      <path d="M16 3l-4 4l-4-4" />
    </svg>
  );
}

/** Home with dollar sign — Class Revenue sub-item */
export function ClassRevenue(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="m19 10l-7-7l-9 9h2v7a2 2 0 0 0 2 2h6" />
      <path d="M9 21v-6a2 2 0 0 1 2-2h2c.387 0 .748.11 1.054.3M21 15h-2.5a1.5 1.5 0 0 0 0 3h1a1.5 1.5 0 0 1 0 3H17m2 0v1m0-8v1" />
    </svg>
  );
}

/** T-shirt — Merch sub-item */
export function ShoppingBag(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="m15 4l6 2v5h-3v8a1 1 0 0 1-1 1H7a1 1 0 0 1-1-1v-8H3V6l6-2a3 3 0 0 0 6 0" />
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

/** Tabler: droplet — spa/wellness */
export function Droplet(props: IconProps) {
  return (
    <svg {...iconDefaults(props)}>
      <path d="M7.975 16.138a6.012 6.012 0 0 1-1.975-4.463c0-2.21 1.326-4.862 2.67-6.98A24 24 0 0 1 12 1.49a24 24 0 0 1 3.325 3.206c1.344 2.118 2.67 4.769 2.67 6.979a6.012 6.012 0 0 1-1.975 4.463A6 6 0 0 1 12 18a6 6 0 0 1-4.025-1.862z" />
    </svg>
  );
}
