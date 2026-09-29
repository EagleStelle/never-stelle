<script setup lang="ts">
import ActionButton from "@/components/task/ActionButton.vue";
import { Separator } from "@/components/ui/separator";
import { useAccount } from "@/composables/useAccount";
import { opensDestructiveGroup } from "@/utils/task";

// The account as a page, where phones reach it from the bottom bar.
const { actions, role, username } = useAccount();
</script>

<template>
  <section aria-labelledby="accountName" class="flex w-full flex-col gap-8 pt-6">
    <header class="flex flex-col gap-0.5 text-center">
      <h1 id="accountName" class="text-2xl font-semibold tracking-tight">{{ username }}</h1>
      <p class="text-sm text-white/60 in-[.light-mode]:text-black/60">{{ role }}</p>
    </header>

    <nav aria-label="Account" class="flex flex-col gap-2">
      <template v-for="(action, index) in actions" :key="action.key">
        <Separator v-if="opensDestructiveGroup(actions, index)" class="my-2 bg-(--glass-border)" />
        <ActionButton :action="action" class="w-full justify-start" />
      </template>
    </nav>
  </section>
</template>
