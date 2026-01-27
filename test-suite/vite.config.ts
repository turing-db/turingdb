import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5555,
    host: true,
    proxy: {
      "/api": {
        target: "http://localhost:5566",
        changeOrigin: true
      }
    }
  }
});
