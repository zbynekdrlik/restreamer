import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { ChunkRecord, ChunkStats } from "../types";

export function ChunkList() {
  const [chunks, setChunks] = useState<ChunkRecord[]>([]);
  const [stats, setStats] = useState<ChunkStats | null>(null);

  const refresh = async () => {
    try {
      const [c, s] = await Promise.all([api.getChunks(), api.getChunkStats()]);
      setChunks(c);
      setStats(s);
    } catch {
      // service unavailable
    }
  };

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <section style={{ marginBottom: "1.5rem" }}>
      <h2>Chunks</h2>
      {stats && (
        <p>
          Total: {stats.total_chunks} | Pending: {stats.pending_chunks} | Sent:{" "}
          {stats.sent_chunks} | In Process: {stats.in_process_chunks}
        </p>
      )}
      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          fontSize: "0.875rem",
        }}
      >
        <thead>
          <tr>
            <th style={thStyle}>ID</th>
            <th style={thStyle}>Size</th>
            <th style={thStyle}>MD5</th>
            <th style={thStyle}>Status</th>
            <th style={thStyle}>Created</th>
          </tr>
        </thead>
        <tbody>
          {chunks.map((chunk) => (
            <tr key={chunk.id}>
              <td style={tdStyle}>{chunk.id}</td>
              <td style={tdStyle}>{chunk.data_size}</td>
              <td style={tdStyle}>{chunk.md5.slice(0, 8)}...</td>
              <td style={tdStyle}>
                {chunk.sent
                  ? "Sent"
                  : chunk.in_process
                    ? "Uploading"
                    : "Pending"}
              </td>
              <td style={tdStyle}>{chunk.created_at}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}

const thStyle: React.CSSProperties = {
  textAlign: "left",
  padding: "0.5rem",
  borderBottom: "2px solid #e5e7eb",
};

const tdStyle: React.CSSProperties = {
  padding: "0.5rem",
  borderBottom: "1px solid #f3f4f6",
};
