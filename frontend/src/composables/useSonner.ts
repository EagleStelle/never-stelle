import { toast as sonnerToast } from "vue-sonner";

import type { ToastType } from "@/types";

// Single boundary to the sonner engine; call sites use toast(message, type).
export function useSonner() {
  function toast(message: string, type: ToastType = "success"): void {
    if (type === "error") sonnerToast.error(message);
    else sonnerToast.success(message);
  }

  return { toast };
}
