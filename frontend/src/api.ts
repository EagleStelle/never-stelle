import type {
  AddTaskResponse,
  CleanupNfoResponse,
  ClearTasksResponse,
  SavedSettings,
  TasksResponse,
  UiConfigResponse,
} from "./types";

async function readError(response: Response, fallback: string): Promise<string> {
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    try {
      const payload = (await response.json()) as { error?: string; detail?: string };
      return payload.error || payload.detail || fallback;
    } catch {
      return fallback;
    }
  }

  try {
    const text = (await response.text()).trim();
    return text || fallback;
  } catch {
    return fallback;
  }
}

async function jsonRequest<T>(path: string, init: RequestInit = {}, fallback = "Request failed."): Promise<T> {
  const response = await fetch(path, init);
  if (!response.ok) {
    throw new Error(await readError(response, fallback));
  }
  return (await response.json()) as T;
}

export function getUiConfig(): Promise<UiConfigResponse> {
  return jsonRequest<UiConfigResponse>("/api/ui-config", {}, "Could not load UI config.");
}

export function saveSettings(settings: SavedSettings): Promise<UiConfigResponse> {
  return jsonRequest<UiConfigResponse>(
    "/api/settings",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(settings),
    },
    "Could not save settings.",
  );
}

export function uploadInstagramCookies(file: File): Promise<UiConfigResponse> {
  const formData = new FormData();
  formData.append("file", file);
  return jsonRequest<UiConfigResponse>(
    "/api/settings/instagram-ytdlp-cookies",
    {
      method: "POST",
      body: formData,
    },
    "Could not connect yt-dlp cookies.",
  );
}

export function deleteInstagramCookies(): Promise<UiConfigResponse> {
  return jsonRequest<UiConfigResponse>(
    "/api/settings/instagram-ytdlp-cookies",
    { method: "DELETE" },
    "Could not remove yt-dlp cookies.",
  );
}

export function getTasks(): Promise<TasksResponse> {
  return jsonRequest<TasksResponse>("/api/tasks", {}, "Could not load tasks.");
}

export function addTask(payload: {
  url: string;
  site_locations: SavedSettings["site_locations"];
  save_mode: SavedSettings["save_mode"];
  client_tab_id: string;
}): Promise<AddTaskResponse> {
  return jsonRequest<AddTaskResponse>(
    "/api/tasks",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
    "Failed to add task.",
  );
}

export async function removeTask(taskId: string): Promise<void> {
  const response = await fetch(`/api/tasks/${encodeURIComponent(taskId)}`, { method: "DELETE" });
  if (response.status === 204) return;
  throw new Error(await readError(response, "Could not remove task."));
}

export async function hideTask(taskId: string): Promise<void> {
  const response = await fetch(`/api/tasks/${encodeURIComponent(taskId)}/hide`, { method: "POST" });
  if (response.status === 204) return;
  throw new Error(await readError(response, "Could not hide task."));
}

export async function markTaskDelivered(taskId: string, clientTabId: string): Promise<void> {
  const response = await fetch(`/api/tasks/${encodeURIComponent(taskId)}/delivered`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ client_tab_id: clientTabId }),
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Could not mark task delivered."));
  }
}

export function clearPendingTasks(): Promise<ClearTasksResponse> {
  return jsonRequest<ClearTasksResponse>(
    "/api/tasks/clear-pending",
    { method: "POST" },
    "Could not clear queue.",
  );
}

export function clearCompletedTasks(): Promise<ClearTasksResponse> {
  return jsonRequest<ClearTasksResponse>(
    "/api/tasks/clear-completed",
    { method: "POST" },
    "Could not clear done.",
  );
}

export function cleanupNfoFiles(): Promise<CleanupNfoResponse> {
  return jsonRequest<CleanupNfoResponse>("/api/cleanup-nfo", { method: "POST" }, "Could not delete .nfo files.");
}

export async function fetchTaskFile(taskId: string): Promise<Response> {
  const response = await fetch(`/api/tasks/${encodeURIComponent(taskId)}/file`, { credentials: "same-origin" });
  if (!response.ok) {
    throw new Error(await readError(response, "Could not download that file."));
  }
  return response;
}
