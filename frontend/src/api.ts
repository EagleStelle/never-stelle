import type {
  AddTaskResponse,
  AuthSessionResponse,
  ClearTasksResponse,
  CredentialsPayload,
  HistoryResponse,
  LoginPayload,
  ProbeResponse,
  SavedSettings,
  ScanMediaResponse,
  ScrapeRule,
  ScrapeTestResponse,
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
  const response = await fetch(path, { credentials: "same-origin", ...init });
  if (!response.ok) {
    throw new Error(await readError(response, fallback));
  }
  return (await response.json()) as T;
}

export function getAuthSession(): Promise<AuthSessionResponse> {
  return jsonRequest<AuthSessionResponse>("/api/auth/session", {}, "Could not check login.");
}

export function login(payload: LoginPayload): Promise<AuthSessionResponse> {
  return jsonRequest<AuthSessionResponse>(
    "/api/auth/login",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
    "Could not sign in.",
  );
}

export function logout(): Promise<AuthSessionResponse> {
  return jsonRequest<AuthSessionResponse>("/api/auth/logout", { method: "POST" }, "Could not sign out.");
}

export function updateCredentials(payload: CredentialsPayload): Promise<AuthSessionResponse> {
  return jsonRequest<AuthSessionResponse>(
    "/api/auth/credentials",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
    "Could not save account.",
  );
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

export function testScrapeRules(url: string, sourceKey: string, rules: ScrapeRule[]): Promise<ScrapeTestResponse> {
  return jsonRequest<ScrapeTestResponse>(
    "/api/settings/scrape-test",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url, source_key: sourceKey, rules }),
    },
    "Could not test scrape rules.",
  );
}

export function uploadPlatformCookies(platform: string, file: File, source = ""): Promise<UiConfigResponse> {
  const formData = new FormData();
  formData.append("file", file);
  if (source) formData.append("source", source);
  return jsonRequest<UiConfigResponse>(
    `/api/settings/ytdlp-cookies/${encodeURIComponent(platform || "others")}`,
    {
      method: "POST",
      body: formData,
    },
    "Could not connect cookies.",
  );
}

export function deletePlatformCookies(platform: string): Promise<UiConfigResponse> {
  return jsonRequest<UiConfigResponse>(
    `/api/settings/ytdlp-cookies/${encodeURIComponent(platform)}`,
    { method: "DELETE" },
    "Could not remove cookies.",
  );
}

export function getTasks(): Promise<TasksResponse> {
  return jsonRequest<TasksResponse>("/api/tasks", {}, "Could not load tasks.");
}

export function getHistory(offset: number, limit: number, sourceKey = "", search = ""): Promise<HistoryResponse> {
  const params = new URLSearchParams({ offset: String(offset), limit: String(limit) });
  if (sourceKey) params.set("source_key", sourceKey);
  if (search) params.set("search", search);
  return jsonRequest<HistoryResponse>(`/api/history?${params.toString()}`, {}, "Could not load history.");
}

export function probeUrl(url: string): Promise<ProbeResponse> {
  return jsonRequest<ProbeResponse>(
    "/api/tasks/probe",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    },
    "Could not read that link.",
  );
}

export function addTask(payload: {
  url?: string;
  urls?: string[];
  site_locations: SavedSettings["site_locations"];
  template_settings?: SavedSettings["template_settings"];
  source_profiles?: SavedSettings["source_profiles"];
  source_templates?: SavedSettings["source_templates"];
  quality?: SavedSettings["default_quality"];
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

export async function cancelTask(taskId: string): Promise<void> {
  const response = await fetch(`/api/tasks/${encodeURIComponent(taskId)}/cancel`, { method: "POST" });
  if (response.status === 204) return;
  throw new Error(await readError(response, "Could not cancel download."));
}

export async function retryTask(taskId: string): Promise<void> {
  const response = await fetch(`/api/tasks/${encodeURIComponent(taskId)}/retry`, { method: "POST" });
  if (response.status === 204) return;
  throw new Error(await readError(response, "Could not retry download."));
}

export function setTaskSource(taskId: string, sourceKey: string): Promise<{ source_key: string }> {
  return jsonRequest<{ source_key: string }>(
    `/api/tasks/${encodeURIComponent(taskId)}/source`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source_key: sourceKey }),
    },
    "Could not set source.",
  );
}

export function clearPendingTasks(): Promise<ClearTasksResponse> {
  return jsonRequest<ClearTasksResponse>(
    "/api/tasks/clear-pending",
    { method: "POST" },
    "Could not clear queue.",
  );
}

export function scanMediaLibrary(): Promise<ScanMediaResponse> {
  return jsonRequest<ScanMediaResponse>(
    "/api/scan",
    { method: "POST" },
    "Could not refresh history.",
  );
}

export async function fetchTaskFile(taskId: string): Promise<Response> {
  const response = await fetch(`/api/tasks/${encodeURIComponent(taskId)}/file`, { credentials: "same-origin" });
  if (!response.ok) {
    throw new Error(await readError(response, "Could not download that file."));
  }
  return response;
}
