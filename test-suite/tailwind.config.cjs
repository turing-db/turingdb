/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#14131f",
        paper: "#f4f0e8",
        accent: "#ff6b3d",
        moss: "#2a5c4d",
        steel: "#1f2937"
      },
      fontFamily: {
        display: ["Space Grotesk", "Poppins", "sans-serif"],
        mono: ["IBM Plex Mono", "Menlo", "monospace"]
      }
    }
  },
  plugins: []
};
