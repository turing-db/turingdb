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
    babel: {
        "presets": [
            "@babel/preset-env",
            ["@babel/preset-react", { "runtime": "automatic" }]
        ],
        "plugins": [
            [
                "@simbathesailor/babel-plugin-use-what-changed",
                {
                    "active": process.env.NODE_ENV === "development" // boolean
                }
            ]
        ]
    }
};

