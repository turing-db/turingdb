import { defineConfig } from 'vite';
import preact from '@preact/preset-vite';
import path from 'path';

// https://vitejs.dev/config/
export default defineConfig({
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
      '@assets': path.resolve(__dirname, './src/assets'),
      '@components': path.resolve(__dirname, './src/components'),
    },
  },
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:6666",
        rewrite: path => path.replace(/^\/api/, ''),
      }
    }
  },
  plugins: [preact()],
});
