import { ThemeProvider, createTheme } from '@mui/material/styles';
import React from 'react'
import { AppBar, DatabasePage, ViewerPage } from './'
import { styled } from '@mui/material/styles';
import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import { CircularProgress } from '@mui/material';
import { themeOptions } from '../theme'
import SelectedNodes from '../SelectedNodes'

export const AppContext = React.createContext({});

function IdleStatus() {
    return <Box sx={{
        display: "flex",
        alignItems: "center",
        opacity: 0.2,
    }}>
        <Box sx={{ paddingRight: 2 }}> </Box>
        <Box sx={{ opacity: 0 }}><CircularProgress size={20} /></Box>
    </Box>
};

const useSelectedNodes = () => {
    const [nodes, setNodes] = React.useState([]);
    const [ids, setIds] = React.useState(new Set());

    return new SelectedNodes({nodes, setNodes, ids, setIds});
}

export default function App() {
    const [currentDb, setCurrentDb] = React.useState(null);
    const [StatusComponent, setStatusComponent] = React.useState(IdleStatus);
    const [currentMenu, setCurrentMenu] = React.useState("Database");
    const selectedNodes = useSelectedNodes();
    const [selectedCyLayout, setSelectedCyLayout] = React.useState("dagre");
    const theme = createTheme(themeOptions);

    const selectedNodesRef = React.useRef(selectedNodes);
    React.useEffect(() => {
        selectedNodesRef.current.clear();
    }, [currentDb])

    const DrawerHeader = styled('div')(({ theme }) => ({
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'flex-end',
        padding: theme.spacing(0, 1),
        ...theme.mixins.toolbar,
    }));

    const setStatus = (content, isLoading) => {
        setStatusComponent(_ => {
            return <Box sx={{
                display: "flex",
                alignItems: "center",
                opacity: 0.2,
            }}>
                <Box sx={{ paddingRight: 2 }}>{content}</Box>
                {isLoading && <Box><CircularProgress size={20} /></Box>}
            </Box>
        });
    }

    const setIdleStatus = () => {
        setStatusComponent(IdleStatus);
    }

    const context = {
        currentDb,
        setCurrentDb,
        DrawerHeader,
        StatusComponent,
        setStatus,
        setIdleStatus,
        selectedNodes,
        selectedCyLayout,
        setSelectedCyLayout,
    }

    const renderPage = _ => {
        switch (currentMenu) {
            case 'Database':
                return <DatabasePage />
            case 'Viewer':
                return <ViewerPage />
            default:
                return "404";
        }
    }

    return <AppContext.Provider value={context}>
        <ThemeProvider theme={theme}>
            <CssBaseline />
            <Box display="flex" sx={{ flexGrow: 1 }}>
                <AppBar setCurrentMenu={setCurrentMenu} />
                <Box component="main" flex={1} sx={{
                    p: 2,
                    display: "flex",
                    flexDirection: "column",
                    bgcolor: theme.palette.background.default,
                    minHeight: "100vh"
                }}>
                    <context.DrawerHeader />
                    {renderPage()}
                </Box>
            </Box>
        </ThemeProvider>
    </AppContext.Provider>;
}
