import React, { useState } from 'react';
import { useKeycloak } from '@react-keycloak/web';

type ReportRow = {
  user_id: string;
  prosthesis_id: string;
  customer_name: string;
  customer_email: string;
  device_type: string;
  total_events: number;
  avg_temperature: number;
  avg_pressure: number;
  last_seen_at: string | null;
};

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<ReportRow[]>([]);
  const [startDate, setStartDate] = useState(() => {
    const date = new Date();
    date.setDate(date.getDate() - 30);
    return date.toISOString().slice(0, 10);
  });
  const [endDate, setEndDate] = useState(() => new Date().toISOString().slice(0, 10));

  const downloadReport = async () => {
    if (!keycloak?.token) {
      setError('Not authenticated');
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const query = new URLSearchParams({
        start: startDate,
        end: endDate
      });

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports?${query.toString()}`, {
        headers: {
          'Authorization': `Bearer ${keycloak.token}`
        }
      });

      
      if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`);
      }

      const data = await response.json();
      setReport(data.report ?? []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  if (!initialized) {
    return <div>Loading...</div>;
  }

  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => keycloak.login()}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="w-full max-w-4xl p-8 bg-white rounded-lg shadow-md">
        <h1 className="text-2xl font-bold mb-6">Usage Reports</h1>
        

        <div className="mb-4 flex flex-wrap gap-4">
          <label className="flex flex-col text-sm text-gray-700">
            Start date
            <input
              type="date"
              value={startDate}
              onChange={(event) => setStartDate(event.target.value)}
              className="mt-1 rounded border border-gray-300 px-2 py-1"
            />
          </label>
          <label className="flex flex-col text-sm text-gray-700">
            End date
            <input
              type="date"
              value={endDate}
              onChange={(event) => setEndDate(event.target.value)}
              className="mt-1 rounded border border-gray-300 px-2 py-1"
            />
          </label>
        </div>

        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Generating Report...' : 'Download Report'}
        </button>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}

        {report.length > 0 && (
          <div className="mt-6 overflow-x-auto">
            <table className="min-w-full text-left border-collapse">
              <thead>
                <tr className="border-b">
                  <th className="py-2 px-3">Prosthesis</th>
                  <th className="py-2 px-3">Device</th>
                  <th className="py-2 px-3">Total Events</th>
                  <th className="py-2 px-3">Avg Temp</th>
                  <th className="py-2 px-3">Avg Pressure</th>
                  <th className="py-2 px-3">Last Seen</th>
                </tr>
              </thead>
              <tbody>
                {report.map((row) => (
                  <tr key={row.prosthesis_id} className="border-b">
                    <td className="py-2 px-3">{row.prosthesis_id}</td>
                    <td className="py-2 px-3">{row.device_type}</td>
                    <td className="py-2 px-3">{row.total_events}</td>
                    <td className="py-2 px-3">{row.avg_temperature?.toFixed(2)}</td>
                    <td className="py-2 px-3">{row.avg_pressure?.toFixed(2)}</td>
                    <td className="py-2 px-3">{row.last_seen_at ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;