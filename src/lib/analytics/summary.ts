import type { AutoRenew } from "@/types/union-data";
import { getCategory } from "./categories";

export interface SummaryKPIs {
  mrrMember: number;
  mrrSky3: number;
  mrrSkyTingTv: number;
  mrrUnknown: number;
  mrrTotal: number;
  activeMembers: number;
  activeSky3: number;
  activeSkyTingTv: number;
  activeUnknown: number;
  activeTotal: number;
  arpuMember: number;
  arpuSky3: number;
  arpuSkyTingTv: number;
  arpuOverall: number;
  /** Map of unrecognized plan names → count of subscribers with that plan */
  unknownPlanNames: Record<string, number>;
}

export function computeSummary(activeAutoRenews: AutoRenew[]): SummaryKPIs {
  let mrrMember = 0, mrrSky3 = 0, mrrSkyTingTv = 0, mrrUnknown = 0;
  let activeMembers = 0, activeSky3 = 0, activeSkyTingTv = 0, activeUnknown = 0;
  const unknownPlanNames: Record<string, number> = {};

  for (const ar of activeAutoRenews) {
    const cat = getCategory(ar.name);
    const monthlyPrice = ar.price; // Assuming price is monthly; annual plans would need division by 12

    switch (cat) {
      case "MEMBER":
        mrrMember += monthlyPrice;
        activeMembers++;
        break;
      case "SKY3":
        mrrSky3 += monthlyPrice;
        activeSky3++;
        break;
      case "SKY_TING_TV":
        mrrSkyTingTv += monthlyPrice;
        activeSkyTingTv++;
        break;
      default:
        mrrUnknown += monthlyPrice;
        activeUnknown++;
        unknownPlanNames[ar.name] = (unknownPlanNames[ar.name] || 0) + 1;
        break;
    }
  }

  // Log unknown plan names for debugging
  if (activeUnknown > 0) {
    console.log(`[summary] ${activeUnknown} active auto-renews have UNKNOWN category:`);
    const sorted = Object.entries(unknownPlanNames).sort((a, b) => b[1] - a[1]);
    for (const [name, count] of sorted) {
      console.log(`  "${name}" × ${count}`);
    }
  }

  const mrrTotal = mrrMember + mrrSky3 + mrrSkyTingTv + mrrUnknown;
  const activeTotal = activeMembers + activeSky3 + activeSkyTingTv + activeUnknown;

  return {
    mrrMember: Math.round(mrrMember * 100) / 100,
    mrrSky3: Math.round(mrrSky3 * 100) / 100,
    mrrSkyTingTv: Math.round(mrrSkyTingTv * 100) / 100,
    mrrUnknown: Math.round(mrrUnknown * 100) / 100,
    mrrTotal: Math.round(mrrTotal * 100) / 100,
    activeMembers,
    activeSky3,
    activeSkyTingTv,
    activeUnknown,
    activeTotal,
    arpuMember: activeMembers > 0 ? Math.round((mrrMember / activeMembers) * 100) / 100 : 0,
    arpuSky3: activeSky3 > 0 ? Math.round((mrrSky3 / activeSky3) * 100) / 100 : 0,
    arpuSkyTingTv: activeSkyTingTv > 0 ? Math.round((mrrSkyTingTv / activeSkyTingTv) * 100) / 100 : 0,
    arpuOverall: activeTotal > 0 ? Math.round((mrrTotal / activeTotal) * 100) / 100 : 0,
    unknownPlanNames,
  };
}
