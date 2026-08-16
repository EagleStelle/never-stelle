<template>
  <div
    class="grid h-dvh w-full place-items-center bg-primary bg-[radial-gradient(72%_56%_at_50%_42%,var(--glass-strong),var(--primary)_72%)] px-4 pb-[6vh] text-white in-[.light-mode]:text-black"
    role="status"
    aria-live="polite"
  >
    <!-- Stacked splash lockup: logo carries the scale, wordmark keeps the exact
         sidebar type so the two headers read as one brand. -->
    <div class="flex flex-col items-center gap-5 leading-none">
      <img src="/assets/logo.png" alt="" class="h-20 w-20 shrink-0" />
      <span
        class="whitespace-nowrap overflow-hidden font-sans font-bold text-white in-[.light-mode]:text-black text-xl tracking-tight"
        >Never Stelle</span
      >
    </div>
  </div>
</template>
