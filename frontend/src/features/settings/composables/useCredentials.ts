import { computed, reactive, ref, watch, type ComputedRef } from "vue";
import { toast } from "vue-sonner";

import { useAuth } from "../../../composables/useAuth";
import type { RuntimeSettings } from "../../../types";
import { errorMessage } from "../../../utils/dashboard";

// Account form state, kept in sync with the live auth session and reset whenever
// the dialog reopens so stale password entry never lingers.
export function useCredentials(open: ComputedRef<boolean>, settings: RuntimeSettings) {
  const auth = useAuth();
  const form = reactive({
    username: "",
    current_password: "",
    new_password: "",
    confirm_password: "",
  });
  const saving = ref(false);

  const isChanged = computed(() => {
    const original = auth.username.value || settings.auth.username || "";
    return form.username !== original || form.new_password.length > 0;
  });

  function reset(): void {
    form.username = auth.username.value || settings.auth.username || form.username;
    form.current_password = "";
    form.new_password = "";
    form.confirm_password = "";
  }

  async function save(): Promise<void> {
    if (!form.current_password) {
      toast.error("Enter your current password.");
      return;
    }
    if (form.new_password !== form.confirm_password) {
      toast.error("New passwords do not match.");
      return;
    }
    saving.value = true;
    try {
      await auth.updateCredentials({
        username: form.username,
        current_password: form.current_password,
        new_password: form.new_password || "",
      });
      form.username = auth.username.value || form.username;
      form.current_password = "";
      form.new_password = "";
      form.confirm_password = "";
      toast.success("Account saved.");
    } catch (error) {
      toast.error(errorMessage(error, "Could not save account."));
    } finally {
      saving.value = false;
    }
  }

  watch(open, (isOpen) => isOpen && reset(), { immediate: true });
  watch(
    () => [settings.auth.username, auth.username.value],
    () => {
      if (!form.username) reset();
    },
  );

  return { auth, form, saving, isChanged, save };
}
