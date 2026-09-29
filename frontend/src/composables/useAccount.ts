import { computed } from "vue";
import IconGear from "~icons/material-symbols/settings";
import IconLogout from "~icons/material-symbols/logout";
import IconMoon from "~icons/material-symbols/dark-mode";
import IconSun from "~icons/material-symbols/light-mode";

import { useAuth } from "@/composables/useAuth";
import { useDashboard } from "@/composables/useDashboard";
import type { ItemAction } from "@/types";

// Who is signed in and what they can do, shared by the sidebar menu and the phone's account page.
export function useAccount() {
  const auth = useAuth();
  const { isLightMode, openSettings, toggleThemeMode } = useDashboard();

  const username = computed(() => auth.username.value || "root");
  const role = "Root account";
  const actions = computed<ItemAction[]>(() => [
    {
      key: "theme",
      label: isLightMode.value ? "Dark Mode" : "Light Mode",
      icon: isLightMode.value ? IconMoon : IconSun,
      variant: "ghost",
      run: toggleThemeMode,
    },
    { key: "settings", label: "Settings", icon: IconGear, variant: "ghost", run: () => openSettings(undefined, "account") },
    { key: "logout", label: "Logout", icon: IconLogout, variant: "destructive-ghost", run: () => void auth.logout() },
  ]);

  return { username, role, actions };
}
