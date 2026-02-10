import type { ServiceStatus } from "../types";

interface Props {
  status: ServiceStatus | null;
}

export function Dashboard({ status }: Props) {
  if (!status) {
    return <p>Loading service status...</p>;
  }

  return (
    <section style={{ marginBottom: "1.5rem" }}>
      <h2>Service Status</h2>
      <div
        style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}
      >
        <StatusCard title="Inpoint" state={status.inpoint.state} />
        <StatusCard title="Endpoint" state={status.endpoint.state} />
        <StatusCard title="Poller" state={status.poller.state} />
        {status.streaming_event && (
          <div style={cardStyle}>
            <strong>Streaming Event</strong>
            <p>ID: {status.streaming_event.identifier ?? "N/A"}</p>
            <p>
              Received: {formatBytes(status.streaming_event.received_bytes)}
            </p>
            <p>
              Receiving:{" "}
              {status.streaming_event.receiving_activated ? "Yes" : "No"} |
              Delivering:{" "}
              {status.streaming_event.delivering_activated ? "Yes" : "No"}
            </p>
          </div>
        )}
      </div>
    </section>
  );
}

function StatusCard({ title, state }: { title: string; state: string }) {
  const color =
    state === "running" ? "#22c55e" : state === "error" ? "#ef4444" : "#f59e0b";
  return (
    <div style={cardStyle}>
      <strong>{title}</strong>
      <p style={{ color }}>{state || "unknown"}</p>
    </div>
  );
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

const cardStyle: React.CSSProperties = {
  border: "1px solid #e5e7eb",
  borderRadius: "0.5rem",
  padding: "1rem",
};
