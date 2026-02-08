export interface ServiceStatus {
  inpoint: ComponentStatus;
  endpoint: ComponentStatus;
  poller: ComponentStatus;
  streaming_event: StreamingEvent | null;
}

export interface ComponentStatus {
  state: string;
  details: Record<string, unknown>;
}

export interface StreamingEvent {
  id: number;
  identifier: string | null;
  short_description: string | null;
  date_of_event: string;
  server_ip: string;
  received_bytes: number;
  receiving_activated: boolean;
  delivering_activated: boolean;
}

export interface ChunkRecord {
  id: number;
  streaming_event_id: number;
  chunk_file_path: string;
  data_size: number;
  created_at: string;
  md5: string;
  in_process: boolean;
  sent: boolean;
}

export interface ChunkStats {
  total_chunks: number;
  pending_chunks: number;
  sent_chunks: number;
  in_process_chunks: number;
  total_bytes: number;
  buffer_duration_secs: number;
}

export type WsEvent =
  | {
      type: "InpointStatus";
      data: { state: string; received_bytes: number; chunk_count: number };
    }
  | {
      type: "EndpointStatus";
      data: {
        state: string;
        pending_chunks: number;
        active_uploads: number;
        buffer_duration: string;
      };
    }
  | {
      type: "ChunkReceived";
      data: { id: number; data_size: number; md5: string };
    }
  | { type: "ChunkUploaded"; data: { chunk_id: number } }
  | {
      type: "StreamingEvent";
      data: {
        action: string;
        identifier: string | null;
        receiving: boolean;
        delivering: boolean;
      };
    }
  | { type: "ManagerPoll"; data: { status_code: number; message: string } }
  | { type: "Error"; data: { service: string; message: string } };
