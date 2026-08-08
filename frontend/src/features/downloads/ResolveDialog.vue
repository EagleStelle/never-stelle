<script setup lang="ts">
import { computed } from "vue";
import { DialogShell as Dialog } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
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
    description="Looks up missing details from each source so these files can be named. Reports back when the pass finishes."
    content-class="fixed left-1/2 top-1/2 z-70 flex w-[min(460px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none"
  >
    <div
      class="flex flex-wrap items-center justify-end gap-2 px-5 pb-5 pt-4 sm:px-6"
    >
      <Button variant="cancel" type="button" :disabled="pending" @click="openModel = false">
        Cancel
      </Button>
      <Button
        variant="primary"
        type="button"
        :disabled="pending || flagged === 0"
        @click="emit('confirm', 'flagged')"
      >
        Resolve Missing ({{ format(flagged) }})
      </Button>
      <Button
        variant="destructive"
        type="button"
        :disabled="pending || total === 0"
        @click="emit('confirm', 'all')"
      >
        Resolve All ({{ format(total) }})
      </Button>
    </div>
  </Dialog>
</template>
