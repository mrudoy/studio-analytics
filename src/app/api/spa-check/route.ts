/** Temporary diagnostic route to check if spa bookings appear in registrations table. */
import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export async function GET() {
  const pool = getPool();

  // 1. Check event_names matching spa keywords
  const eventNames = await pool.query(`
    SELECT DISTINCT event_name
    FROM registrations
    WHERE event_name ~* 'sauna|infrared|contrast|cupping|treatment|spa lounge|wellness'
    ORDER BY event_name
  `);

  // 2. Check location_names matching spa keywords
  const locationNames = await pool.query(`
    SELECT DISTINCT location_name
    FROM registrations
    WHERE location_name ~* 'sauna|infrared|contrast|cupping|treatment|spa|wellness'
    ORDER BY location_name
  `);

  // 3. Sample registrations if any match
  const samples = await pool.query(`
    SELECT event_name, location_name, pass, subscription, revenue, attended_at,
           first_name, last_name
    FROM registrations
    WHERE event_name ~* 'sauna|infrared|contrast|cupping|treatment|spa lounge|wellness'
       OR location_name ~* 'sauna|infrared|contrast|cupping|treatment|spa|wellness'
    ORDER BY attended_at DESC NULLS LAST
    LIMIT 30
  `);

  // 4. Total count
  const countRes = await pool.query(`
    SELECT COUNT(*) AS cnt
    FROM registrations
    WHERE event_name ~* 'sauna|infrared|contrast|cupping|treatment|spa lounge|wellness'
       OR location_name ~* 'sauna|infrared|contrast|cupping|treatment|spa|wellness'
  `);

  // 5. Also check all distinct location_names (might reveal naming)
  const allLocations = await pool.query(`
    SELECT DISTINCT location_name, COUNT(*) AS cnt
    FROM registrations
    GROUP BY location_name
    ORDER BY cnt DESC
  `);

  return NextResponse.json({
    spaEventNames: eventNames.rows.map((r: Record<string, unknown>) => r.event_name),
    spaLocationNames: locationNames.rows.map((r: Record<string, unknown>) => r.location_name),
    totalSpaRegistrations: Number(countRes.rows[0].cnt),
    samples: samples.rows,
    allLocations: allLocations.rows,
  });
}
