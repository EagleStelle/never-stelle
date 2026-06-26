import { ref } from "vue";

import type { ToastMessage, ToastType } from "../types";

export function useToastStack() {
  const toasts = ref<ToastMessage[]>([]);
  const srStatus = ref("");
  let toastId = 0;

  function toast(message: string, type: ToastType = "success"): void {
    const id = ++toastId;
    toasts.value.push({ id, message, type });
    srStatus.value = message;
    window.setTimeout(() => {
      toasts.value = toasts.value.filter((item) => item.id !== id);
    }, 3200);
  }

  return {
    srStatus,
    toast,
    toasts,
  };
}
