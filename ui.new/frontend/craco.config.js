module.exports = {
    style: {
        postcss: {
            plugins: [
                require('tailwindcss'),
                require('autoprefixer'),
            ],
        },
    },
    devServer: {
        devMiddleware: {
            writeToDisk: true,
        },
    },
    content: ["./src/**/*.{html,js}"],
    plugins: [],
};

