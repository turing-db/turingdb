/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#e2e8f0",
        paper: "#0b0f14",
        accent: "#38bdf8",
        moss: "#22c55e",
        steel: "#1e293b"
      },
      fontFamily: {
        display: ["Space Grotesk", "Poppins", "sans-serif"],
        mono: ["IBM Plex Mono", "Menlo", "monospace"]
      }
    }
  },
  plugins: []
};
