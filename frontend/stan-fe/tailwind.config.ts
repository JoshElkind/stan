/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./pages/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        monoton: ["Monoton", "cursive"],
        custom: ["Orbitron", "sans-serif"],
        futuristic: ["Audiowide", "sans-serif"],
        tech: ["Rajdhani", "sans-serif"],
        elegant: ["Playfair Display", "serif"],
        modern: ["Poppins", "sans-serif"],
        oxanium: ["Oxanium", "sans-serif"],
        kanit: ["Kanit", "sans-serif"],
        titillium: ["Titillium Web", "sans-serif"],
        ubuntu: ["Ubuntu Mono", "monospace"],
        kode: ["Kode Mono", "monospace"],
        megrim: ["Megrim", "cursive"],
      },
      colors: {
        debug: "#ff00ff", // add this line
      },
    },
  },
  plugins: [],
};
