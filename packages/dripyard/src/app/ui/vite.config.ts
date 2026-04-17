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
    // Proxy every engine endpoint plus /health to the running
    // dripyard server. Dev runs vite on a different port than
    // dripyard; this stitches them together.
    proxy: {
      "/query": "http://localhost:3457",
      "/mutate": "http://localhost:3457",
      "/subscribe": "http://localhost:3457",
      "/webhook": "http://localhost:3457",
      "/login": "http://localhost:3457",
      "/logout": "http://localhost:3457",
      "/health": "http://localhost:3457",
    },
  },
});
