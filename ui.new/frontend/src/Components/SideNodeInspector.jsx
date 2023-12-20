// Core
import React from "react";
import { useSelector } from "react-redux";
import { useTheme } from "@emotion/react";

// @mui/material
import {
  Box,
  Button,
  CircularProgress,
  ListItem,
  ListItemIcon,
  Typography,
} from "@mui/material";

// @mui/icons-material
import HubIcon from "@mui/icons-material/Hub";

import { Secondary } from "src/Components/Span";
import NodeInspector from "src/Components/NodeInspector";

const SideNodeInspector = (props) => {
  const theme = useTheme();
  const { open, menuExpanded } = props;
  const inspectedNode = useSelector((state) => state.inspectedNode);
  const [showFullInspector, setShowFullInspector] = React.useState(false);

  if (!inspectedNode) return <></>;

  const properties = inspectedNode.properties;

  return (
    <Box
      m={1}
      p={1}
      {...(menuExpanded && {
        border: 1,
        borderRadius: 1,
        borderColor: theme.palette.secondary.main,
      })}>
      <ListItem
        key="NodeInspector"
        sx={{
          display: "block",
          px: 0.5,
          mr: menuExpanded ? 3 : 0,
          justifyContent: open ? "initial" : "center",
        }}>
        <ListItemIcon
          sx={{
            minHeight: 48,
          }}>
          <HubIcon sx={{ mr: 3, color: theme.palette.primary.main }} />
          <Typography sx={{ color: theme.palette.primary.main }}>
            Node {inspectedNode.id}
          </Typography>
        </ListItemIcon>
        {menuExpanded &&
          (!inspectedNode ? (
            <CircularProgress size={20} />
          ) : (
            <Box>
              <Box overflow="auto">
                <Typography variant="body2">
                  <Secondary>Node type:</Secondary> {inspectedNode.node_type}
                </Typography>
                {Object.keys(properties).map((pName) => (
                  <Box
                    key={"box-key-" + pName}
                    display="flex"
                    alignItems="center">
                    <Typography variant="body2">
                      <Secondary>{pName}</Secondary>:{" "}
                      <span>{properties[pName].toString()}</span>
                    </Typography>
                  </Box>
                ))}
              </Box>
              <Button onClick={() => setShowFullInspector(true)}>
                Inspect node
              </Button>
              <NodeInspector
                open={showFullInspector}
                onClose={() => setShowFullInspector(false)}
              />
            </Box>
          ))}
      </ListItem>
    </Box>
  );
};

export default SideNodeInspector;
