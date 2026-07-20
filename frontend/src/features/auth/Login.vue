<script setup lang="ts">
import { reactive, ref } from "vue";
import IconLock from "~icons/material-symbols/lock";
import IconPerson from "~icons/material-symbols/person";

import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import { useAuth } from "../../composables/useAuth";
import { errorMessage } from "../../utils/dashboard";

const auth = useAuth();
const form = reactive({
  username: "",
  password: "",
});
const error = ref("");

async function submit(): Promise<void> {
  error.value = "";
  try {
    await auth.login({
      username: form.username,
      password: form.password,
    });
    form.password = "";
  } catch (err) {
    error.value = errorMessage(err, "Could not sign in.");
  }
}
</script>

<template>
  <main
    class="min-h-dvh w-full bg-primary text-white in-[.light-mode]:text-black grid place-items-center px-4"
  >
    <form
      class="glass-chrome flex w-full max-w-sm flex-col gap-4 rounded-lg p-5"
      aria-label="Sign in"
      @submit.prevent="submit"
    >
      <div class="flex items-center gap-3">
        <img src="/assets/logo.png" alt="" class="h-10 w-10 rounded-lg" />
        <div class="min-w-0">
          <h1 class="text-xl font-semibold leading-tight">Never Stelle</h1>
        </div>
      </div>

      <div class="flex flex-col gap-2">
        <Input
          v-model="form.username"
          type="text"
          autocomplete="username"
          placeholder="Username"
          autofocus
        >
          <template #icon>
            <IconPerson class="h-5 w-5" aria-hidden="true" />
          </template>
        </Input>
        <Input
          v-model="form.password"
          type="password"
          autocomplete="current-password"
          placeholder="Password"
        >
          <template #icon>
            <IconLock class="h-5 w-5" aria-hidden="true" />
          </template>
        </Input>
      </div>

      <p
        v-if="error"
        class="rounded-lg border border-[#ef4444]/35 bg-[#ef4444]/15 px-3 py-2 text-sm text-[#fecaca]"
        role="alert"
      >
        {{ error }}
      </p>

      <Button
        type="submit"
        class="w-full"
        :disabled="auth.loading.value"
      >
        {{ auth.loading.value ? "Signing in..." : "Sign in" }}
      </Button>
    </form>
  </main>
</template>

