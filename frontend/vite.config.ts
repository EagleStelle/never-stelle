import { fileURLToPath, URL } from "node:url";

import tailwindcss from "@tailwindcss/vite";
import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vite";

import Icons from "unplugin-icons/vite";

const outDir = process.env.FRONTEND_OUT_DIR || "dist";
const apiTarget = process.env.VITE_API_TARGET || "http://127.0.0.1:8840";
const devHost = process.env.VITE_DEV_HOST || "127.0.0.1";
const devPort = Number(process.env.VITE_DEV_PORT || 5173);

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
    host: devHost,
    port: devPort,
    strictPort: true,
    proxy: {
      "/api": apiTarget,
    },
  },
});
