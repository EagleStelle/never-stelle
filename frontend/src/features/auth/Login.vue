<script setup lang="ts">
import { reactive, ref } from "vue";
import IconCapsLock from "~icons/material-symbols/keyboard-capslock";
import IconLock from "~icons/material-symbols/lock";
import IconMoon from "~icons/material-symbols/dark-mode";
import IconPerson from "~icons/material-symbols/person";
import IconSpinner from "~icons/material-symbols/progress-activity";
import IconSun from "~icons/material-symbols/light-mode";
import IconWarning from "~icons/material-symbols/warning";
import IconGithub from "~icons/simple-icons/github";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useAuth } from "@/composables/useAuth";
import { useDashboard } from "@/composables/useDashboard";
import { errorMessage } from "@/utils/dashboard";

const LABEL =
  "text-xs font-medium uppercase tracking-[0.12em] text-white/50 in-[.light-mode]:text-black/50";
// Both credit lines share one type ramp so the link and the copyright read as a
// single block rather than a link with a caption under it.
const CREDIT =
  "inline-flex items-center gap-2 text-sm font-medium leading-normal tracking-normal text-white/60 in-[.light-mode]:text-black/60";
const CREDIT_LINK =
  "transition-colors duration-300 ease-glass hover:text-white in-[.light-mode]:hover:text-black";
const GITHUB_URL = "https://github.com/EagleStelle";

const auth = useAuth();
const { isLightMode, toggleThemeMode } = useDashboard();
const form = reactive({
  username: "",
  password: "",
});
const error = ref("");
const capsLock = ref(false);

// Caps lock has no state to read on its own: it only surfaces through a key event,
// so the warning tracks the last key struck in the field and clears when it blurs.
function trackCapsLock(event: KeyboardEvent): void {
  capsLock.value = event.getModifierState("CapsLock");
}

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
    class="relative grid min-h-dvh w-full max-w-full overflow-hidden bg-primary text-white in-[.light-mode]:text-black lg:grid-cols-2"
  >
    <!-- Ambient wash spans the whole page, so the two columns differ by content only
         and never read as two separate surfaces meeting at a seam. -->
    <div
      class="pointer-events-none absolute inset-0 bg-[radial-gradient(88%_64%_at_24%_30%,var(--glass-strong),transparent_76%)]"
      aria-hidden="true"
    />
    <div
      class="pointer-events-none absolute inset-0 opacity-[0.11] bg-[radial-gradient(46%_40%_at_86%_10%,var(--accent),transparent_72%)]"
      aria-hidden="true"
    />

    <section
      class="relative hidden flex-col justify-between p-10 xl:p-16 lg:flex"
    >
      <div class="glass-rise flex items-center gap-3 leading-none">
        <img src="/assets/logo.png" alt="" class="h-9 w-9 shrink-0" />
        <span class="text-xl font-bold tracking-tight">Never Stelle</span>
      </div>

      <div class="glass-rise [animation-delay:90ms] flex flex-col items-start gap-1">
        <a
          :href="GITHUB_URL"
          target="_blank"
          rel="noreferrer noopener"
          :class="[CREDIT, CREDIT_LINK]"
        >
          <IconGithub class="h-4 w-4 shrink-0" aria-hidden="true" />
          <span>Eagle Stelle</span>
        </a>
        <p :class="CREDIT">
          <!-- Sized to the GitHub mark so the symbol takes the same gutter and both
               credit lines start their text on one edge. -->
          <span class="w-4 shrink-0 text-center">&copy;</span>
          <span>Never Stelle 2026</span>
        </p>
      </div>
    </section>

    <!-- Sign-in half: no card. The form sits straight on the page. -->
    <section class="relative flex flex-col px-6 py-8 sm:px-10">
      <header class="flex items-center gap-4">
        <div class="flex items-center gap-3 leading-none lg:hidden">
          <img src="/assets/logo.png" alt="" class="h-8 w-8 shrink-0" />
          <span class="text-lg font-bold tracking-tight">Never Stelle</span>
        </div>

        <Button
          type="button"
          variant="ghost"
          size="icon"
          class="ml-auto"
          :aria-label="
            isLightMode ? 'Switch to dark mode' : 'Switch to light mode'
          "
          @click="toggleThemeMode"
        >
          <template #icon>
            <component
              :is="isLightMode ? IconMoon : IconSun"
              class="group-hover:scale-110"
              aria-hidden="true"
            />
          </template>
        </Button>
      </header>

      <div class="flex flex-1 items-center justify-center py-10">
        <form
          class="glass-rise [animation-delay:120ms] flex w-full max-w-sm flex-col gap-6"
          aria-label="Sign in"
          :aria-describedby="error ? 'loginError' : undefined"
          @submit.prevent="submit"
        >
          <h1 class="text-3xl font-semibold leading-tight tracking-tight">
            Sign in
          </h1>

          <p
            v-if="error"
            id="loginError"
            class="flex items-start gap-2 rounded-lg border border-destructive/40 bg-destructive/15 px-3 py-2.5 text-sm font-medium text-destructive"
            role="alert"
          >
            <IconWarning class="mt-px h-4 w-4 shrink-0" aria-hidden="true" />
            <span class="min-w-0">{{ error }}</span>
          </p>

          <div class="flex flex-col gap-4">
            <div class="flex flex-col gap-2">
              <Label for="loginUsernameInput" :class="LABEL">Username</Label>
              <Input
                id="loginUsernameInput"
                v-model="form.username"
                type="text"
                name="username"
                autocomplete="username"
                :aria-invalid="error ? 'true' : undefined"
                required
                autofocus
              >
                <template #icon>
                  <IconPerson class="h-5 w-5" aria-hidden="true" />
                </template>
              </Input>
            </div>

            <div class="flex flex-col gap-2">
              <Label for="loginPasswordInput" :class="LABEL">Password</Label>
              <Input
                id="loginPasswordInput"
                v-model="form.password"
                type="password"
                name="password"
                autocomplete="current-password"
                :aria-invalid="error ? 'true' : undefined"
                required
                @keydown="trackCapsLock"
                @keyup="trackCapsLock"
                @blur="capsLock = false"
              >
                <template #icon>
                  <IconLock class="h-5 w-5" aria-hidden="true" />
                </template>
              </Input>

              <p
                v-if="capsLock"
                class="flex items-center gap-1.5 text-xs font-medium text-accent"
                aria-live="polite"
              >
                <IconCapsLock class="h-4 w-4 shrink-0" aria-hidden="true" />
                Caps lock is on.
              </p>
            </div>
          </div>

          <Button type="submit" class="w-full" :disabled="auth.loading.value">
            <IconSpinner
              v-if="auth.loading.value"
              class="animate-spin"
              aria-hidden="true"
            />
            {{ auth.loading.value ? "Signing in" : "Sign in" }}
          </Button>
        </form>
      </div>

      <footer class="flex flex-col items-center gap-1 lg:hidden">
        <a
          :href="GITHUB_URL"
          target="_blank"
          rel="noreferrer noopener"
          :class="[CREDIT, CREDIT_LINK]"
        >
          <IconGithub class="h-4 w-4 shrink-0" aria-hidden="true" />
          <span>Eagle Stelle</span>
        </a>
        <p :class="CREDIT">
          <!-- Sized to the GitHub mark so the symbol takes the same gutter and both
               credit lines start their text on one edge. -->
          <span class="w-4 shrink-0 text-center">&copy;</span>
          <span>Never Stelle 2026</span>
        </p>
      </footer>
    </section>
  </main>
</template>
