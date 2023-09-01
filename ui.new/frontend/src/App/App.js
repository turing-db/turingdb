import { ThemeProvider, createTheme } from '@mui/material/styles';
import React from 'react'
import { styled } from '@mui/material/styles';
import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import { themeOptions } from '../theme'

import DatabasePage from '../Components/DatabasePage';
import ViewerPage from '../Components/ViewerPage';
import CustomAppBar from '../Components/CustomAppBar'
import { Provider, useSelector } from 'react-redux';
import store from './store'

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
        default: return "404";
    }
}

export default function App() {
    const theme = createTheme(themeOptions.dark);

    return <ThemeProvider theme={theme}>
        <Provider store={store}>
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
        </Provider>
    </ThemeProvider>;
}

