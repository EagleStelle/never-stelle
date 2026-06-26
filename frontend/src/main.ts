import { createApp } from "vue";
import { QueryClient, VueQueryPlugin } from "@tanstack/vue-query";

import App from "./App.vue";
import "./styles.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

createApp(App).use(VueQueryPlugin, { queryClient }).mount("#app");
