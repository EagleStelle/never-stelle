<script setup lang="ts">
import IconSearch from "~icons/material-symbols/search";

import TaskFilters from "@/components/task/Filters.vue";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useDashboard } from "@/composables/useDashboard";

const { historySearch, submitHistorySearch } = useDashboard();
</script>

<template>
  <!-- Mobile reverses the rows so the search field sits closest to the keyboard. -->
  <div class="flex flex-col-reverse lg:flex-col gap-3 w-full">
    <form
      class="flex items-center gap-2"
      aria-label="Search history"
      @submit.prevent="submitHistorySearch"
    >
      <Input
        v-model="historySearch"
        class="flex-1 min-w-0"
        name="q"
        type="text"
        autocomplete="off"
        placeholder="Search history..."
      >
        <template #icon>
          <IconSearch class="w-5 h-5" aria-hidden="true" />
        </template>
      </Input>

      <Button
        variant="primary"
        type="submit"
        aria-label="Search history"
        title="Search history"
      >
        <template #icon>
          <IconSearch aria-hidden="true" class="group-hover:scale-110" />
        </template>
      </Button>
    </form>

    <div
      class="flex overflow-x-auto no-scrollbar items-end justify-between lg:justify-end gap-3 w-full py-1"
    >
      <TaskFilters class="shrink-0 lg:ml-auto" />
    </div>
  </div>
</template>
