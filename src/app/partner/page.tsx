import { getPool } from "@/lib/db/database";
import { ACTIVE_STATES_SQL } from "@/lib/analytics/metrics/filters";

export const dynamic = "force-dynamic";

const FONT_BRAND = "'Cormorant Garamond', 'Times New Roman', serif";
const FONT_SANS = "'Helvetica Neue', Helvetica, Arial, sans-serif";

async function getPartnerStats() {
  const pool = getPool();
  const client = await pool.connect();

  try {
    const [activeRows, communityRow, weekly12moRow, attendanceRow, active12moRow, freqRow, regularsRow] =
      await Promise.all([
        // Active subscribers by category — row counts (matches dashboard canonical)
        client.query(`
          SELECT plan_category, COUNT(*) AS cnt
          FROM auto_renews
          WHERE plan_state IN (${ACTIVE_STATES_SQL})
          GROUP BY plan_category
        `),
        // Total unique community members ever
        client.query(`
          SELECT COUNT(DISTINCT email) AS total FROM registrations
        `),
        // Avg weekly unique visitors (last 12 complete weeks, week-boundary aligned, zeros included)
        client.query(`
          SELECT ROUND(AVG(weekly_count)) AS avg_weekly
          FROM (
            SELECT gs.wk, COUNT(DISTINCT r.email) AS weekly_count
            FROM generate_series(
              DATE_TRUNC('week', NOW()) - INTERVAL '12 weeks',
              DATE_TRUNC('week', NOW()) - INTERVAL '1 week',
              '1 week'::interval
            ) AS gs(wk)
            LEFT JOIN registrations r
              ON DATE_TRUNC('week', r.attended_at) = gs.wk
            GROUP BY gs.wk
          ) t
        `),
        // Total class attendances ever
        client.query(`
          SELECT COUNT(*) AS total FROM registrations WHERE attended_at IS NOT NULL
        `),
        // Unique people reached in last 12 months
        client.query(`
          SELECT COUNT(DISTINCT email) AS total
          FROM registrations
          WHERE attended_at >= NOW() - INTERVAL '12 months'
        `),
        // Avg classes per person per year (last 12 months)
        client.query(`
          SELECT ROUND(AVG(visits)::numeric, 1) AS avg_visits
          FROM (
            SELECT email, COUNT(*) AS visits
            FROM registrations
            WHERE attended_at >= NOW() - INTERVAL '12 months'
            GROUP BY email
          ) t
        `),
        // Loyal regulars (5+ class attendances ever)
        client.query(`
          SELECT COUNT(*) AS cnt
          FROM (
            SELECT email
            FROM registrations
            WHERE attended_at IS NOT NULL
            GROUP BY email
            HAVING COUNT(*) >= 5
          ) t
        `),
      ]);

    const activeMap = Object.fromEntries(
      activeRows.rows.map((r) => [r.plan_category, parseInt(r.cnt)])
    );
    const tvActive = activeMap["SKY_TING_TV"] ?? 0;
    const memberActive = activeMap["MEMBER"] ?? 0;
    const sky3Active = activeMap["SKY3"] ?? 0;
    const totalActive = tvActive + memberActive + sky3Active;

    return {
      totalCommunity: parseInt(communityRow.rows[0].total),
      avgWeeklyVisitors: parseInt(weekly12moRow.rows[0].avg_weekly ?? "0"),
      totalActive,
      tvActive,
      memberActive,
      sky3Active,
      totalAttendances: parseInt(attendanceRow.rows[0].total),
      active12mo: parseInt(active12moRow.rows[0].total),
      avgVisitsPerYear: parseFloat(freqRow.rows[0].avg_visits ?? "0"),
      loyalRegulars: parseInt(regularsRow.rows[0].cnt),
    };
  } finally {
    client.release();
  }
}

function fmt(n: number): string {
  return n.toLocaleString("en-US");
}

function roundDown(n: number, to: number): string {
  return fmt(Math.floor(n / to) * to) + "+";
}

export default async function PartnerPage() {
  const s = await getPartnerStats();

  const stats = [
    {
      value: roundDown(s.totalCommunity, 1000),
      label: "Community Members",
      sub: "unique students in our platform",
    },
    {
      value: roundDown(s.avgWeeklyVisitors, 100),
      label: "Weekly Visitors",
      sub: "avg in-studio per week",
    },
    {
      value: roundDown(s.totalActive, 100),
      label: "Active Subscribers",
      sub: "paying members right now",
    },
    {
      value: roundDown(s.tvActive, 100),
      label: "Online Subscribers",
      sub: "SkyTing TV digital members",
    },
    {
      value: roundDown(s.totalAttendances, 10000),
      label: "Class Check-Ins",
      sub: "total sessions attended",
    },
    {
      value: roundDown(s.active12mo, 100),
      label: "Reached Annually",
      sub: "unique people in the last 12 months",
    },
  ];

  const breakdownStats = [
    { label: "In-Studio Members", value: fmt(s.memberActive) },
    { label: "Sky3 Subscribers", value: fmt(s.sky3Active) },
    { label: "SkyTing TV", value: fmt(s.tvActive) },
    { label: "Loyal Regulars (5+ classes)", value: fmt(s.loyalRegulars) },
    { label: "Avg Classes / Person / Year", value: s.avgVisitsPerYear.toFixed(1) },
  ];

  return (
    <div
      style={{
        fontFamily: FONT_SANS,
        background: "#FAFAF8",
        minHeight: "100vh",
        color: "#413A3A",
      }}
    >
      {/* Header */}
      <div
        style={{
          background: "#fff",
          borderBottom: "1px solid #E8E4E0",
          padding: "2.5rem 3rem 2rem",
          display: "flex",
          alignItems: "flex-end",
          justifyContent: "space-between",
        }}
      >
        <div>
          <div
            style={{
              fontFamily: "Helvetica, Arial, sans-serif",
              fontSize: "0.75rem",
              fontWeight: 500,
              letterSpacing: "0.35em",
              textTransform: "uppercase",
              color: "#413A3A",
              marginBottom: "0.75rem",
            }}
          >
            SKY TING
          </div>
          <h1
            style={{
              fontFamily: FONT_BRAND,
              fontWeight: 300,
              fontSize: "3rem",
              lineHeight: 1,
              margin: 0,
              color: "#413A3A",
            }}
          >
            By the Numbers
          </h1>
        </div>
        <div
          style={{
            textAlign: "right",
            fontSize: "0.8rem",
            color: "rgba(65,58,58,0.55)",
            lineHeight: 1.6,
          }}
        >
          <div>New York City</div>
          <div>Premium Yoga & Wellness</div>
          <div>Est. 2014</div>
        </div>
      </div>

      {/* Intro */}
      <div style={{ padding: "2rem 3rem 0" }}>
        <p
          style={{
            fontSize: "1rem",
            color: "rgba(65,58,58,0.65)",
            maxWidth: "560px",
            lineHeight: 1.6,
            margin: 0,
          }}
        >
          SkyTing is New York City&apos;s premier yoga and wellness community — in-studio in Tribeca
          and online worldwide. Our members are health-conscious, brand-aware, and highly engaged.
        </p>
      </div>

      {/* Hero stat grid */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(3, 1fr)",
          gap: "1px",
          background: "#E8E4E0",
          margin: "2rem 3rem 0",
          border: "1px solid #E8E4E0",
          borderRadius: "8px",
          overflow: "hidden",
        }}
      >
        {stats.map((stat, i) => (
          <div
            key={i}
            style={{
              background: "#fff",
              padding: "2rem 1.75rem",
            }}
          >
            <div
              style={{
                fontFamily: FONT_BRAND,
                fontSize: "3rem",
                fontWeight: 400,
                lineHeight: 1,
                color: "#413A3A",
                marginBottom: "0.5rem",
              }}
            >
              {stat.value}
            </div>
            <div
              style={{
                fontSize: "0.85rem",
                fontWeight: 600,
                letterSpacing: "0.04em",
                color: "#413A3A",
                marginBottom: "0.25rem",
                textTransform: "uppercase",
              }}
            >
              {stat.label}
            </div>
            <div
              style={{
                fontSize: "0.8rem",
                color: "rgba(65,58,58,0.5)",
              }}
            >
              {stat.sub}
            </div>
          </div>
        ))}
      </div>

      {/* Subscriber breakdown + audience quality */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "1.5rem",
          margin: "1.5rem 3rem 0",
        }}
      >
        {/* Subscriber breakdown */}
        <div
          style={{
            background: "#fff",
            border: "1px solid #E8E4E0",
            borderRadius: "8px",
            padding: "1.5rem 1.75rem",
          }}
        >
          <div
            style={{
              fontSize: "0.7rem",
              fontWeight: 600,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              color: "rgba(65,58,58,0.45)",
              marginBottom: "1.25rem",
            }}
          >
            Subscriber Breakdown
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
            {breakdownStats.map((item, i) => (
              <div
                key={i}
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "baseline",
                  borderBottom: i < breakdownStats.length - 1 ? "1px solid #F0EDE9" : "none",
                  paddingBottom: i < breakdownStats.length - 1 ? "0.75rem" : 0,
                }}
              >
                <span style={{ fontSize: "0.875rem", color: "rgba(65,58,58,0.7)" }}>
                  {item.label}
                </span>
                <span
                  style={{
                    fontFamily: FONT_BRAND,
                    fontSize: "1.5rem",
                    fontWeight: 400,
                    color: "#413A3A",
                  }}
                >
                  {item.value}
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Audience quality */}
        <div
          style={{
            background: "#413A3A",
            border: "1px solid #413A3A",
            borderRadius: "8px",
            padding: "1.5rem 1.75rem",
            color: "#FAF8F5",
          }}
        >
          <div
            style={{
              fontSize: "0.7rem",
              fontWeight: 600,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              color: "rgba(255,255,255,0.45)",
              marginBottom: "1.25rem",
            }}
          >
            Audience Profile
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.85rem" }}>
            {[
              "Urban professionals in NYC and major metro areas",
              "Health-focused, wellness-invested lifestyle",
              "High disposable income — premium subscriptions and retreats",
              "Early adopters with strong brand affinity",
              "Digital-native with global online reach via SkyTing TV",
            ].map((point, i) => (
              <div
                key={i}
                style={{ display: "flex", gap: "0.75rem", alignItems: "flex-start" }}
              >
                <span
                  style={{
                    color: "rgba(255,255,255,0.35)",
                    fontSize: "0.75rem",
                    marginTop: "0.15rem",
                    flexShrink: 0,
                  }}
                >
                  —
                </span>
                <span style={{ fontSize: "0.875rem", lineHeight: 1.5, color: "rgba(255,255,255,0.8)" }}>
                  {point}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Footer */}
      <div
        style={{
          padding: "2rem 3rem",
          marginTop: "2rem",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          borderTop: "1px solid #E8E4E0",
        }}
      >
        <div
          style={{
            fontFamily: "Helvetica, Arial, sans-serif",
            fontSize: "0.7rem",
            fontWeight: 500,
            letterSpacing: "0.3em",
            textTransform: "uppercase",
            color: "rgba(65,58,58,0.4)",
          }}
        >
          SKY TING · NEW YORK
        </div>
        <div
          style={{
            fontSize: "0.75rem",
            color: "rgba(65,58,58,0.35)",
          }}
        >
          skyting.com
        </div>
      </div>
    </div>
  );
}
