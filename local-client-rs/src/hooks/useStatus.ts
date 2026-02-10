import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { ServiceStatus } from "../types";

export function useStatus() {
  const [status, setStatus] = useState<ServiceStatus | null>(null);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    let active = true;

    const poll = async () => {
      try {
        const data = await api.getStatus();
        if (active) {
          setStatus(data);
          setConnected(true);
        }
      } catch (e) {
        console.error("Status poll failed:", e);
        if (active) {
          setConnected(false);
        }
      }
    };

    poll();
    const interval = setInterval(poll, 3000);
    return () => {
      active = false;
      clearInterval(interval);
    };
  }, []);

  return { status, connected };
}
