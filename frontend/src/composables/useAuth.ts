import { computed, ref } from "vue";

import {
  getAuthSession,
  login as loginRequest,
  logout as logoutRequest,
  updateCredentials as updateCredentialsRequest,
} from "../api";
import type { AuthSessionResponse, CredentialsPayload, LoginPayload } from "../types";

const ready = ref(false);
const authenticated = ref(false);
const username = ref("");
const loading = ref(false);

let sessionRequest: Promise<void> | null = null;

function applySession(session: AuthSessionResponse): void {
  authenticated.value = Boolean(session.authenticated);
  username.value = session.authenticated ? session.username || "" : "";
}

async function refreshSession(): Promise<void> {
  if (sessionRequest) return sessionRequest;
  loading.value = true;
  sessionRequest = getAuthSession()
    .then((session) => applySession(session))
    .catch(() => applySession({ authenticated: false, username: "" }))
    .finally(() => {
      ready.value = true;
      loading.value = false;
      sessionRequest = null;
    });
  return sessionRequest;
}

async function login(payload: LoginPayload): Promise<void> {
  loading.value = true;
  try {
    applySession(await loginRequest(payload));
    ready.value = true;
  } finally {
    loading.value = false;
  }
}

async function logout(): Promise<void> {
  loading.value = true;
  try {
    await logoutRequest();
  } finally {
    applySession({ authenticated: false, username: "" });
    ready.value = true;
    loading.value = false;
  }
}

async function updateCredentials(payload: CredentialsPayload): Promise<void> {
  loading.value = true;
  try {
    applySession(await updateCredentialsRequest(payload));
    ready.value = true;
  } finally {
    loading.value = false;
  }
}

export function useAuth() {
  return {
    authenticated: computed(() => authenticated.value),
    loading: computed(() => loading.value),
    ready: computed(() => ready.value),
    username: computed(() => username.value),
    login,
    logout,
    refreshSession,
    updateCredentials,
  };
}

