import path from "path";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
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
  build: {
    outDir: "site",
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
      "@App": path.resolve(__dirname, "./src/App"),
      "@DatabasePage": path.resolve(__dirname, "./src/DatabasePage"),
      "@ViewerPage": path.resolve(__dirname, "./src/ViewerPage"),
      "@AdminPage": path.resolve(__dirname, "./src/AdminPage"),
      "@turingvisualizer": path.resolve(__dirname, "./src/turingvisualizer"),
      "@VisualizerOld": path.resolve(__dirname, "./src/Visualizer.old"),
      "@Components": path.resolve(__dirname, "./src/Components"),
      "@cytoscape-cola": path.resolve(__dirname, "./src/cytoscape-cola"),
    },
  },
});
