import { resolve } from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [tailwindcss(), react()],
  root: resolve(__dirname),
  resolve: {
    alias: {
      "@": resolve(__dirname),
    },
    dedupe: ["react", "react-dom"],
  },
  build: {
    outDir: resolve(__dirname, "../../../dist/app/ui"),
    emptyOutDir: true,
    chunkSizeWarningLimit: 3000,
  },
  server: {
    proxy: {
      "/vex": "http://localhost:3457",
      "/health": "http://localhost:3457",
    },
  },
});
