import { darkScrollbar } from "@mui/material";

export const themeOptions = {
    dark: {
        components: {
            MuiCssBaseline: {
                styleOverrides: (themeParam) => ({
                    body: themeParam.palette.mode === 'dark' ? darkScrollbar() : null,
                }),
            },
        },
        palette: {
            mode: 'dark',
            background: {
                paper: '#0d0d0e',
            },
        },
    },
    light: {
        palette: {
            mode: 'light',
        },
    }
};

