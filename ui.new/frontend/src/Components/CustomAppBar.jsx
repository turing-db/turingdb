// React
import React from "react";
import { useDispatch, useSelector } from "react-redux";

// @mui/material
import { styled, useTheme } from "@mui/material/styles";
import {
  Box,
  Divider,
  ListItemIcon,
  Toolbar,
  List,
  IconButton,
  ListItem,
  ListItemButton,
} from "@mui/material";
import { AppBar as MuiAppBar } from "@mui/material";
import { Drawer as MuiDrawer } from "@mui/material";

// @mui/icons-material
import ChevronLeftIcon from "@mui/icons-material/ChevronLeft";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import DnsIcon from "@mui/icons-material/Dns";
import VisibilityIcon from "@mui/icons-material/Visibility";
import AdminPanelSettingsIcon from "@mui/icons-material/AdminPanelSettings";
import Brightness4Icon from "@mui/icons-material/Brightness4";
import Brightness7Icon from "@mui/icons-material/Brightness7";
import ChatIcon from "@mui/icons-material/Chat";

// Turing
import * as actions from "src/App/actions";
import * as thunks from "src/App/thunks";
import DBSelector from "src/Components/DBSelector";
import SideNodeInspector from "src/Components/SideNodeInspector";

const drawerWidth = 240;

export const DrawerHeader = styled("div")(({ theme }) => ({
  display: "flex",
  alignItems: "center",
  justifyContent: "flex-end",
  padding: theme.spacing(0, 1),
  ...theme.mixins.toolbar,
}));

const openedMixin = (theme) => ({
  width: drawerWidth,
  transition: theme.transitions.create("width", {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.enteringScreen,
  }),
  overflowX: "hidden",
});

const closedMixin = (theme) => ({
  transition: theme.transitions.create("width", {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
  }),
  overflowX: "hidden",
  width: `calc(${theme.spacing(7)} + 1px)`,
  [theme.breakpoints.up("sm")]: {
    width: `calc(${theme.spacing(8)} + 1px)`,
  },
});

const AppBar = styled(MuiAppBar, {
  shouldForwardProp: (prop) => prop !== "open",
})(({ theme, open }) => ({
  zIndex: theme.zIndex.drawer + 1,
  transition: theme.transitions.create(["width", "margin"], {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
  }),
  ...(open && {
    marginLeft: drawerWidth,
    width: `calc(100% - ${drawerWidth}px)`,
    transition: theme.transitions.create(["width", "margin"], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
  }),
}));

const Drawer = styled(MuiDrawer, {
  shouldForwardProp: (prop) => prop !== "open",
})(({ theme, open }) => ({
  width: drawerWidth,
  flexShrink: 0,
  whiteSpace: "nowrap",
  boxSizing: "border-box",
  ...(open && {
    ...openedMixin(theme),
    "& .MuiDrawer-paper": openedMixin(theme),
  }),
  ...(!open && {
    ...closedMixin(theme),
    "& .MuiDrawer-paper": closedMixin(theme),
  }),
  zIndex: 5,
}));

export default function CustomAppBar() {
  const theme = useTheme();
  const [open, setOpen] = React.useState(false);
  const dbName = useSelector((state) => state.dbName);
  const page = useSelector((state) => state.page);
  const dispatch = useDispatch();

  React.useEffect(() => {
    dispatch(actions.clear());
  }, [dbName]); //eslint-disable-line react-hooks/exhaustive-deps

  const handleDrawerOpen = () => {
    setOpen(true);
  };

  const handleDrawerClose = () => {
    setOpen(false);
  };

  return (
    <>
      <AppBar position="fixed" open={open}>
        <Toolbar>
          <IconButton
            color="inherit"
            aria-label="open menu"
            onClick={handleDrawerOpen}
            edge="start"
            sx={{
              mr: 5,
              ...(open && { display: "none" }),
            }}>
            {theme.direction === "rtl" ? (
              <ChevronLeftIcon />
            ) : (
              <ChevronRightIcon />
            )}
          </IconButton>
          <Box className="flex flex-grow justify-between items-center">
            <Box variant="h5">{dbName ? dbName : "No database selected"}</Box>
            <Box>
              <IconButton
                sx={{ ml: 1 }}
                onClick={() =>
                  theme.palette.mode === "dark"
                    ? dispatch(actions.setThemeMode("light"))
                    : dispatch(actions.setThemeMode("dark"))
                }
                color="inherit">
                {theme.palette.mode === "dark" ? (
                  <Brightness7Icon />
                ) : (
                  <Brightness4Icon />
                )}
              </IconButton>
            </Box>
          </Box>
        </Toolbar>
      </AppBar>

      <Drawer variant="permanent" open={open}>
        <DrawerHeader>
          <ListItemButton
            sx={{ justifyContent: "space-between" }}
            onClick={handleDrawerClose}>
            <div>Turing Biosystems</div>
            {theme.direction === "rtl" ? (
              <ChevronRightIcon />
            ) : (
              <ChevronLeftIcon />
            )}
          </ListItemButton>
        </DrawerHeader>

        <List>
          <ListItem
            key="Dabatase"
            disablePadding
            sx={{ display: "block" }}
            onClick={
              dbName
                ? () => {
                    dispatch(actions.setPage("Database"));
                    dispatch(thunks.inspectNode(dbName, null));
                  }
                : handleDrawerOpen
            }>
            <ListItemButton
              sx={{
                minHeight: 48,
                justifyContent: open ? "initial" : "center",
                px: 2.5,
              }}>
              <ListItemIcon
                sx={{
                  minWidth: 0,
                  mr: open ? 3 : "auto",
                  justifyContent: "center",
                }}>
                <DnsIcon />
              </ListItemIcon>

              {open ? <DBSelector /> : ""}
            </ListItemButton>
          </ListItem>
          <ListItem
            key="Viewer"
            disablePadding
            sx={{ display: "block" }}
            onClick={
              dbName
                ? () => {
                    dispatch(actions.setPage("Viewer"));
                    dispatch(thunks.inspectNode(dbName, null));
                  }
                : handleDrawerOpen
            }>
            <ListItemButton
              sx={{
                minHeight: 48,
                justifyContent: open ? "initial" : "center",
                px: 2.5,
              }}>
              <ListItemIcon
                sx={{
                  minWidth: 0,
                  mr: open ? 3 : "auto",
                  justifyContent: "center",
                }}>
                <VisibilityIcon />
              </ListItemIcon>

              {open ? "Viewer" : ""}
            </ListItemButton>
          </ListItem>
          <ListItem
            key="Admin"
            disablePadding
            sx={{ display: "block" }}
            onClick={
              dbName
                ? () => {
                    dispatch(actions.setPage("Admin"));
                    dispatch(thunks.inspectNode(dbName, null));
                  }
                : handleDrawerOpen
            }>
            <ListItemButton
              disabled
              sx={{
                minHeight: 48,
                justifyContent: open ? "initial" : "center",
                px: 2.5,
              }}>
              <ListItemIcon
                sx={{
                  minWidth: 0,
                  mr: open ? 3 : "auto",
                  justifyContent: "center",
                }}>
                <AdminPanelSettingsIcon />
              </ListItemIcon>

              {open ? "Admin" : ""}
            </ListItemButton>
          </ListItem>
          <ListItem
            key="Chat"
            disablePadding
            sx={{ display: "block" }}
            onClick={() => {
              dispatch(actions.setPage("Chat"));
              dispatch(thunks.inspectNode(dbName, null));
            }}>
            <ListItemButton
              sx={{
                minHeight: 48,
                justifyContent: open ? "initial" : "center",
                px: 2.5,
              }}>
              <ListItemIcon
                sx={{
                  minWidth: 0,
                  mr: open ? 3 : "auto",
                  justifyContent: "center",
                }}>
                <ChatIcon />
              </ListItemIcon>

              {open ? "Chat" : ""}
            </ListItemButton>
          </ListItem>
          <Divider />
        </List>

        <SideNodeInspector
          menuExpanded={open}
          open={open && page === "Viewer"}
        />
      </Drawer>
    </>
  );
}
