import { fileURLToPath, URL } from "node:url";

import tailwindcss from "@tailwindcss/vite";
import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vite";

import Icons from "unplugin-icons/vite";

const outDir = process.env.FRONTEND_OUT_DIR || "dist";

export default defineConfig({
  plugins: [
    vue(),
    tailwindcss(),
    Icons({
      compiler: "vue3",
    }),
  ],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  build: {
    outDir,
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8088",
    },
  },
});
