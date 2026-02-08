import { Dashboard } from "./components/Dashboard";
import { ChunkList } from "./components/ChunkList";
import { LogViewer } from "./components/LogViewer";
import { ConfigEditor } from "./components/ConfigEditor";
import { useStatus } from "./hooks/useStatus";

function App() {
  const { status, connected } = useStatus();

  return (
    <div style={{ fontFamily: "system-ui, sans-serif", padding: "1rem" }}>
      <header style={{ marginBottom: "1rem" }}>
        <h1 style={{ margin: 0 }}>Restreamer Dashboard</h1>
        <span
          style={{
            color: connected ? "#22c55e" : "#ef4444",
            fontSize: "0.875rem",
          }}
        >
          {connected ? "Connected" : "Disconnected"}
        </span>
      </header>

      <Dashboard status={status} />
      <ChunkList />
      <LogViewer />
      <ConfigEditor />
    </div>
  );
}

export default App;
