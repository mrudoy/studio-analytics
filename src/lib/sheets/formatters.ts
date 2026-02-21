import type { GoogleSpreadsheetWorksheet } from "google-spreadsheet";

export async function formatHeaderRow(sheet: GoogleSpreadsheetWorksheet): Promise<void> {
  await sheet.loadCells("A1:Z1");

  const headerCount = sheet.headerValues.length;
  for (let col = 0; col < headerCount; col++) {
    const cell = sheet.getCell(0, col);
    cell.textFormat = { bold: true };
    cell.backgroundColor = { red: 0.9, green: 0.9, blue: 0.9, alpha: 1 };
  }

  await sheet.saveUpdatedCells();
}

export async function formatPercentColumns(
  sheet: GoogleSpreadsheetWorksheet,
  columnIndices: number[],
  rowCount: number
): Promise<void> {
  const lastRow = Math.min(rowCount + 1, 1000);
  const lastCol = Math.max(...columnIndices) + 1;
  await sheet.loadCells({ startRowIndex: 1, endRowIndex: lastRow, startColumnIndex: 0, endColumnIndex: lastCol });

  for (let row = 1; row < lastRow; row++) {
    for (const col of columnIndices) {
      try {
        const cell = sheet.getCell(row, col);
        cell.numberFormat = { type: "NUMBER", pattern: "0.0%" };
        // Divide by 100 since our analytics returns percentages as 72.3 not 0.723
        if (typeof cell.value === "number") {
          cell.value = cell.value / 100;
        }
      } catch {
        // Cell may not exist
      }
    }
  }

  await sheet.saveUpdatedCells();
}

export async function formatCurrencyColumns(
  sheet: GoogleSpreadsheetWorksheet,
  columnIndices: number[],
  rowCount: number
): Promise<void> {
  const lastRow = Math.min(rowCount + 1, 1000);
  const lastCol = Math.max(...columnIndices) + 1;
  await sheet.loadCells({ startRowIndex: 1, endRowIndex: lastRow, startColumnIndex: 0, endColumnIndex: lastCol });

  for (let row = 1; row < lastRow; row++) {
    for (const col of columnIndices) {
      try {
        const cell = sheet.getCell(row, col);
        cell.numberFormat = { type: "CURRENCY", pattern: "$#,##0.00" };
      } catch {
        // Cell may not exist
      }
    }
  }

  await sheet.saveUpdatedCells();
}

export async function freezeHeaderRow(sheet: GoogleSpreadsheetWorksheet): Promise<void> {
  await sheet.updateProperties({
    gridProperties: {
      rowCount: sheet.gridProperties.rowCount,
      columnCount: sheet.gridProperties.columnCount,
      frozenRowCount: 1,
    },
  });
}

export async function applyStandardFormatting(sheet: GoogleSpreadsheetWorksheet, rowCount: number): Promise<void> {
  await freezeHeaderRow(sheet);
  await formatHeaderRow(sheet);
}
