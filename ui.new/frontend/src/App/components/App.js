import { ThemeProvider, createTheme } from '@mui/material/styles';
import React from 'react'
import { styled } from '@mui/material/styles';
import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import { themeOptions } from '../../theme'

import DatabasePage from './DatabasePage';
import ViewerPage from './ViewerPage';
import AdminPage from './AdminPage';
import CustomAppBar from './CustomAppBar'
import { Provider, useSelector } from 'react-redux';
import store from '../store'

export const DrawerHeader = styled('div')(({ theme }) => ({
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-end',
    padding: theme.spacing(0, 1),
    ...theme.mixins.toolbar,
}));

function Page() {
    const page = useSelector((state) => state.page)

    switch (page) {
        case 'Database': return <DatabasePage />;
        case 'Viewer': return <ViewerPage />;
        case 'Admin': return <AdminPage />;
        default: return "404";
    }
}

const useTheme = (options) => {
    const themeMode = useSelector(state => state.themeMode);
    return React.useMemo(() => {
        const theme = createTheme(options[themeMode]);
        themeMode === "light"
            ? (() => { // Light
                theme.nodes = {
                    selected: {
                        icon: theme.palette.secondary.dark,
                        text: theme.palette.secondary.dark,
                    },
                    neighbor: {
                        icon: theme.palette.info.light,
                        text: theme.palette.info.light,
                    }
                };
                theme.edges = {
                    connecting: {
                        line: theme.palette.secondary.dark,
                        text: theme.palette.secondary.dark,
                    },
                    neighbor: {
                        line: theme.palette.info.light,
                        text: theme.palette.info.light,
                    }
                };
            })()
            : (() => { // Dark
                theme.nodes = {
                    selected: {
                        icon: "rgb(204,204,204)",
                        text: "rgb(204,204,204)",
                    },
                    neighbor: {
                        icon: "rgb(155,155,155)",
                        text: "rgb(155,155,155)",
                    }
                };
                theme.edges = {
                    connecting: {
                        line: "rgb(0,193,0)",
                        text: "rgb(0,193,0)",
                    },
                    neighbor: {
                        line: "rgb(0,130,0)",
                        text: "rgb(0,130,0)",
                    }
                };
            })();
        return theme;
    }, [options, themeMode])
}

const ThemedApp = () => {
    const theme = useTheme(themeOptions);

    return <ThemeProvider theme={theme}>
        <CssBaseline />
        <Box display="flex" sx={{ flexGrow: 1 }}>
            <CustomAppBar />
            <Box component="main" flex={1} sx={{
                p: 0,
                display: "flex",
                flexDirection: "column",
                minHeight: "100vh"
            }}>
                <DrawerHeader />
                <Page />
            </Box>
        </Box>
    </ThemeProvider>;
}

export default function App() {
    return <Provider store={store}>
        <ThemedApp />
    </Provider>
}


