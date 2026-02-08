import { useCallback } from "react";
import { useWebSocket } from "../hooks/useWebSocket";
import { useState } from "react";
import type { WsEvent } from "../types";

export function LogViewer() {
  const [logs, setLogs] = useState<string[]>([]);

  const handleEvent = useCallback((event: WsEvent) => {
    const timestamp = new Date().toLocaleTimeString();
    const entry = `[${timestamp}] ${event.type}: ${JSON.stringify(event.data)}`;
    setLogs((prev) => [...prev.slice(-99), entry]);
  }, []);

  const { connected } = useWebSocket(handleEvent);

  return (
    <section style={{ marginBottom: "1.5rem" }}>
      <h2>
        Live Log{" "}
        <span
          style={{
            fontSize: "0.75rem",
            color: connected ? "#22c55e" : "#ef4444",
          }}
        >
          {connected ? "(connected)" : "(disconnected)"}
        </span>
      </h2>
      <div
        style={{
          background: "#1e1e1e",
          color: "#d4d4d4",
          fontFamily: "monospace",
          fontSize: "0.75rem",
          padding: "0.5rem",
          borderRadius: "0.5rem",
          maxHeight: "300px",
          overflow: "auto",
        }}
      >
        {logs.length === 0 ? (
          <p style={{ color: "#666" }}>Waiting for events...</p>
        ) : (
          logs.map((log, i) => <div key={i}>{log}</div>)
        )}
      </div>
    </section>
  );
}
