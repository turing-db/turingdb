import react from "@vitejs/plugin-react";
import tailwindcss from 'tailwindcss';
import { defineConfig } from "vite";
import jsconfigPaths from "vite-jsconfig-paths";
import tsconfigPaths from "vite-tsconfig-paths";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react(), jsconfigPaths(), tsconfigPaths()],
  server: {
    port: 3000,
    proxy: {
      "/api": {
        target: "http://localhost:5000",
        changeOrigin: true,
        secure: false,
        ws: true,
      },
    },
  },
  css: {
    postcss: {
      plugins: [tailwindcss],
    },
  },
  build: {
    outDir: "site",
  },
});
