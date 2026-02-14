"use client";

import { useState, useEffect } from "react";

interface RunResult {
  jobId: string;
  completedAt: string | null;
  duration: number;
  sheetUrl: string;
  rawDataSheetUrl: string;
  recordCounts: Record<string, number>;
  warnings: number;
}

interface RunError {
  jobId: string;
  failedAt: string | null;
  error: string;
}

interface ResultsData {
  latest: RunResult | null;
  history: RunResult[];
  recentErrors: RunError[];
}

export default function ResultsPage() {
  const [data, setData] = useState<ResultsData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/results")
      .then((res) => res.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <p className="text-gray-500">Loading results...</p>
      </div>
    );
  }

  return (
    <div className="min-h-screen flex flex-col items-center p-8">
      <div className="max-w-4xl w-full space-y-8">
        <div className="text-center">
          <h1 className="text-4xl font-bold tracking-tight">Pipeline Results</h1>
          <p className="mt-2 text-gray-500">View latest analytics and run history</p>
        </div>

        {/* Latest Result */}
        {data?.latest ? (
          <div className="rounded-xl border bg-white p-8 shadow-sm space-y-4">
            <h2 className="text-lg font-semibold">Latest Run</h2>
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-gray-500">Completed:</span>
                <span className="ml-2">{data.latest.completedAt ? new Date(data.latest.completedAt).toLocaleString() : "N/A"}</span>
              </div>
              <div>
                <span className="text-gray-500">Duration:</span>
                <span className="ml-2">{Math.round(data.latest.duration / 1000)}s</span>
              </div>
              {data.latest.warnings > 0 && (
                <div className="col-span-2">
                  <span className="text-yellow-600">{data.latest.warnings} warning(s)</span>
                </div>
              )}
            </div>

            {/* Record counts */}
            <div className="border rounded-lg p-4 bg-gray-50">
              <h3 className="text-sm font-medium text-gray-700 mb-2">Records Processed</h3>
              <div className="grid grid-cols-2 gap-2 text-sm">
                {Object.entries(data.latest.recordCounts).map(([key, count]) => (
                  <div key={key}>
                    <span className="text-gray-500">{key}:</span>
                    <span className="ml-2 font-mono">{count}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="flex gap-4">
              {data.latest.sheetUrl && (
                <a
                  href={data.latest.sheetUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="rounded-lg bg-green-600 px-4 py-2 text-sm font-medium text-white hover:bg-green-700"
                >
                  Open Analytics Sheet
                </a>
              )}
              {data.latest.rawDataSheetUrl && (
                <a
                  href={data.latest.rawDataSheetUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
                >
                  Open Raw Data Sheet
                </a>
              )}
            </div>
          </div>
        ) : (
          <div className="rounded-xl border bg-white p-8 shadow-sm text-center">
            <p className="text-gray-500">No completed pipeline runs yet.</p>
            <a href="/" className="mt-2 inline-block text-indigo-600 underline">
              Run the pipeline
            </a>
          </div>
        )}

        {/* Run History */}
        {data?.history && data.history.length > 1 && (
          <div className="rounded-xl border bg-white p-8 shadow-sm space-y-4">
            <h2 className="text-lg font-semibold">Run History</h2>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-gray-500">
                  <th className="py-2">Date</th>
                  <th className="py-2">Duration</th>
                  <th className="py-2">Records</th>
                  <th className="py-2">Sheet</th>
                </tr>
              </thead>
              <tbody>
                {data.history.map((run) => (
                  <tr key={run.jobId} className="border-b">
                    <td className="py-2">
                      {run.completedAt ? new Date(run.completedAt).toLocaleString() : "N/A"}
                    </td>
                    <td className="py-2">{Math.round(run.duration / 1000)}s</td>
                    <td className="py-2 font-mono">
                      {Object.values(run.recordCounts).reduce((a, b) => a + b, 0)}
                    </td>
                    <td className="py-2">
                      {run.sheetUrl && (
                        <a href={run.sheetUrl} target="_blank" rel="noopener noreferrer" className="text-indigo-600 underline">
                          Open
                        </a>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Recent Errors */}
        {data?.recentErrors && data.recentErrors.length > 0 && (
          <div className="rounded-xl border border-red-200 bg-red-50 p-8 shadow-sm space-y-4">
            <h2 className="text-lg font-semibold text-red-800">Recent Errors</h2>
            {data.recentErrors.map((err) => (
              <div key={err.jobId} className="text-sm text-red-700">
                <span className="font-medium">
                  {err.failedAt ? new Date(err.failedAt).toLocaleString() : "Unknown time"}:
                </span>{" "}
                {err.error}
              </div>
            ))}
          </div>
        )}

        <div className="flex justify-center gap-6 text-sm text-gray-500">
          <a href="/" className="hover:text-gray-700 underline">Dashboard</a>
          <a href="/pipeline" className="hover:text-gray-700 underline">Pipeline</a>
          <a href="/settings" className="hover:text-gray-700 underline">Settings</a>
        </div>
      </div>
    </div>
  );
}
