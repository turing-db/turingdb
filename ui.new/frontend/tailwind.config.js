/** @type {import('tailwindcss').Config} */
module.exports = {
    mode: 'jit',
    content: [
        "./src/**/*.{js,jsx,ts,tsx}",
    ],
    theme: {
        extend: {
            colors: {
                primary: '#0d47a1',
                secondary: '#03a9f4',
                turing1: '#09092E',
                turing2: '#80BEE9',
            }
        },
    },
    plugins: [],
}
