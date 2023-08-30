import React from 'react'
import { styled, useTheme } from '@mui/material/styles';
import Box from '@mui/material/Box';
import MuiDrawer from '@mui/material/Drawer';
import MuiAppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import List from '@mui/material/List';
import IconButton from '@mui/material/IconButton';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import DnsIcon from '@mui/icons-material/Dns';
import VisibilityIcon from '@mui/icons-material/Visibility';

import { DBSelector, SideNodeInspector } from './';
import { DrawerHeader } from '../App/App';
import { useDispatch, useSelector } from 'react-redux';
import * as actions from '../App/actions';
import { Divider } from '@mui/material';

const drawerWidth = 240;

const openedMixin = (theme) => ({
    width: drawerWidth,
    transition: theme.transitions.create('width', {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.enteringScreen,
    }),
    overflowX: 'hidden',
});

const closedMixin = (theme) => ({
    transition: theme.transitions.create('width', {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.leavingScreen,
    }),
    overflowX: 'hidden',
    width: `calc(${theme.spacing(7)} + 1px)`,
    [theme.breakpoints.up('sm')]: {
        width: `calc(${theme.spacing(8)} + 1px)`,
    },
});

const AppBar = styled(MuiAppBar, {
    shouldForwardProp: (prop) => prop !== 'open',
})(({ theme, open }) => ({
    zIndex: theme.zIndex.drawer + 1,
    transition: theme.transitions.create(['width', 'margin'], {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.leavingScreen,
    }),
    ...(open && {
        marginLeft: drawerWidth,
        width: `calc(100% - ${drawerWidth}px)`,
        transition: theme.transitions.create(['width', 'margin'], {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.enteringScreen,
        }),
    }),
}));

const Drawer = styled(MuiDrawer, { shouldForwardProp: (prop) => prop !== 'open' })(
    ({ theme, open }) => ({
        width: drawerWidth,
        flexShrink: 0,
        whiteSpace: 'nowrap',
        boxSizing: 'border-box',
        ...(open && {
            ...openedMixin(theme),
            '& .MuiDrawer-paper': openedMixin(theme),
        }),
        ...(!open && {
            ...closedMixin(theme),
            '& .MuiDrawer-paper': closedMixin(theme),
        }),
    }),
);

export default function CustomAppBar() {
    const theme = useTheme();
    const [open, setOpen] = React.useState(false);
    const dbName = useSelector((state) => state.dbName);
    const page = useSelector((state) => state.page);
    const dispatch = useDispatch();

    React.useEffect(() => {
        dispatch(actions.clearSelectedNodes());
    }, [dbName]); //eslint-disable-line react-hooks/exhaustive-deps

    const handleDrawerOpen = () => {
        setOpen(true);
    };

    const handleDrawerClose = () => {
        setOpen(false);
    };

    return <>
        <AppBar position="fixed" open={open}>
            <Toolbar>
                <IconButton
                    color="inherit"
                    aria-label="open menu"
                    onClick={handleDrawerOpen}
                    edge="start"
                    sx={{
                        mr: 5,
                        ...(open && { display: 'none' }),
                    }}
                >
                    {theme.direction === 'rtl' ? <ChevronLeftIcon /> : <ChevronRightIcon />}
                </IconButton>
                <Box sx={{
                    display: "flex",
                    flexGrow: 1,
                    justifyContent: "space-between",
                    alignItems: "center"
                }}>
                    <Box variant="h5">
                        {dbName ? dbName : "No database selected"}
                    </Box>
                </Box>
            </Toolbar>
        </AppBar>

        <Drawer variant="permanent" open={open}>

            <DrawerHeader>
                <ListItemButton
                    sx={{ justifyContent: "space-between" }}
                    onClick={handleDrawerClose}>

                    <div >Turing Biosystems</div>
                    {theme.direction === 'rtl' ? <ChevronRightIcon /> : <ChevronLeftIcon />}
                </ListItemButton>
            </DrawerHeader>

            <List>
                <ListItem
                    key="Dabatase"
                    disablePadding
                    sx={{ display: 'block' }}
                    onClick={
                        dbName
                            ? () => {
                                dispatch(actions.setPage("Database"));
                                dispatch(actions.inspectNode(null));
                            }
                            : handleDrawerOpen
                    }
                >
                    <ListItemButton
                        sx={{
                            minHeight: 48,
                            justifyContent: open ? 'initial' : 'center',
                            px: 2.5,
                        }}
                    >

                        <ListItemIcon
                            sx={{
                                minWidth: 0,
                                mr: open ? 3 : 'auto',
                                justifyContent: 'center',
                            }}
                        >
                            <DnsIcon />
                        </ListItemIcon>

                        {open ? <DBSelector /> : ""}
                    </ListItemButton>
                </ListItem>
                <ListItem
                    key="Viewer"
                    disablePadding
                    sx={{ display: 'block' }}
                    onClick={
                        dbName
                            ? () => {
                                dispatch(actions.setPage("Viewer"));
                                dispatch(actions.inspectNode(null));
                            }
                            : handleDrawerOpen
                    }
                >
                    <ListItemButton
                        sx={{
                            minHeight: 48,
                            justifyContent: open ? 'initial' : 'center',
                            px: 2.5,
                        }}
                    >

                        <ListItemIcon
                            sx={{
                                minWidth: 0,
                                mr: open ? 3 : 'auto',
                                justifyContent: 'center',
                            }}
                        >
                            <VisibilityIcon />
                        </ListItemIcon>

                        {open ? "Viewer" : ""}
                    </ListItemButton>
                </ListItem>
                <Divider />
            </List>

            <SideNodeInspector menuExpanded={open} open={open && page === 'Viewer'} />
        </Drawer>
    </>
}
