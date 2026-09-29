<script setup lang="ts">
import { computed } from "vue";
import { DialogFooter, DialogShell as Dialog } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { ACTION_ICONS, syncIconClass } from "@/ui";
import type { ResolveScope } from "@/types";

const props = defineProps<{
  open: boolean;
  flagged: number;
  total: number;
  pending?: boolean;
}>();

const emit = defineEmits<{
  "update:open": [open: boolean];
  confirm: [scope: ResolveScope];
}>();

const openModel = computed({
  get: () => props.open,
  set: (value) => emit("update:open", value),
});

const format = (count: number) => count.toLocaleString();
</script>

<template>
  <Dialog
    v-model:open="openModel"
    title="Resolve History"
    description="Looks up missing details from each source so these files can be named."
    content-class="fixed left-1/2 top-1/2 z-70 flex w-[min(460px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none"
  >
    <DialogFooter class="mt-5">
      <Button variant="ghost" type="button" :disabled="pending" @click="openModel = false">
        Cancel
      </Button>
      <Button
        variant="destructive-ghost"
        type="button"
        :disabled="pending || total === 0"
        @click="emit('confirm', 'all')"
      >
        <template #icon>
          <component :is="ACTION_ICONS.resolve" aria-hidden="true" :class="syncIconClass(false)" />
        </template>
        Resolve All ({{ format(total) }})
      </Button>
      <Button
        variant="primary"
        type="button"
        :disabled="pending || flagged === 0"
        @click="emit('confirm', 'flagged')"
      >
        <template #icon>
          <component :is="ACTION_ICONS.resolve" aria-hidden="true" :class="syncIconClass(false)" />
        </template>
        Resolve Missing ({{ format(flagged) }})
      </Button>
    </DialogFooter>
  </Dialog>
</template>
