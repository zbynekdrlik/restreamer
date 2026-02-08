import { useEffect, useState } from "react";

export function ConfigEditor() {
  const [config, setConfig] = useState<string>("");
  const [error, setError] = useState<string>("");

  useEffect(() => {
    fetch("http://127.0.0.1:8910/api/v1/config")
      .then((r) => r.json())
      .then((data) => setConfig(JSON.stringify(data, null, 2)))
      .catch((e) => setError(`Failed to load config: ${e}`));
  }, []);

  return (
    <section style={{ marginBottom: "1.5rem" }}>
      <h2>Configuration</h2>
      {error && <p style={{ color: "#ef4444" }}>{error}</p>}
      <pre
        style={{
          background: "#f9fafb",
          padding: "1rem",
          borderRadius: "0.5rem",
          fontSize: "0.8rem",
          overflow: "auto",
          maxHeight: "400px",
        }}
      >
        {config || "Loading..."}
      </pre>
    </section>
  );
}
