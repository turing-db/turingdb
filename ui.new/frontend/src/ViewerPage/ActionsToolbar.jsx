// Core
import { useTheme } from "@emotion/react";
import React from "react";

// @mui
import { IconButton, Tooltip } from "@mui/material";
import VisibilityIcon from "@mui/icons-material/Visibility";
import CleaningServicesIcon from "@mui/icons-material/CleaningServices";
import FitScreenIcon from "@mui/icons-material/FitScreen";
import SettingsIcon from "@mui/icons-material/Settings";
import UnfoldMoreIcon from "@mui/icons-material/UnfoldMore";
import SearchIcon from "@mui/icons-material/Search";

// @blueprintjs
import { Icon, Popover } from "@blueprintjs/core";

// Turing
import { useVisualizerContext, useCanvasTrigger } from "@turingvisualizer";
import SearchNodesWindow from "@ViewerPage/SearchNodesWindow";
import HiddenNodesPopover from "@ViewerPage/HiddenNodesPopover";

const btnStyle = {
  size: "large",
  color: "primary",
};

const CellCellInteractionIcon = (props) => {
  const theme = useTheme();

  return (
    <Icon
      icon="intersection"
      color={props.disabled ? undefined : theme.palette.primary.main}
      size={25}
    />
  );
};

export const showCellCellInteraction = (addLayout, cy) => {
  const nodes = cy.nodes();
  if (nodes.length === 0) return;

  const uniqueTitleValues = nodes
    .map((n) => n.data().properties.title)
    .filter((title, i, arr) => arr.indexOf(title) === i);

  const filteredNodes = uniqueTitleValues.map((title) =>
    nodes.filter((n) => n.data().properties.title === title)
  );

  const nLines = uniqueTitleValues.length;
  const maxLength = Math.max.apply(
    Math,
    filteredNodes.map((arr) => arr.length)
  );
  const yStretch = 35.0;
  const aspectRatio = cy.width() / cy.height();
  const canvasWidth = maxLength * yStretch * aspectRatio;
  const xStretch = canvasWidth / nLines;

  uniqueTitleValues.forEach((_title, x) => {
    const currentNodes = filteredNodes[x];
    const xPos = x * xStretch;
    const yShift = (maxLength - currentNodes.length) / 2;

    addLayout(
      {
        name: "preset",
        fit: false,
        positions: {
          ...Object.fromEntries(
            currentNodes.map((n, y) => [
              n.id(),
              { x: xPos, y: (y + yShift) * yStretch },
            ])
          ),
        },
      },
      currentNodes.map((n) => n.id())
    );
  });
};

const ActionsToolbar = (props) => {
  const vis = useVisualizerContext();
  const [interactionDisabled, setInteractionDisabled] = React.useState(true);

  useCanvasTrigger({
    category: "elements",
    name: "actionsToolbar-setInteractionDisabled",
    callback: () => {
      setInteractionDisabled(
        (() => {
          const nodes = vis
            .cy()
            .nodes()
            .filter((n) => n.id);
          if (nodes.length === 0) return true;

          const uniqueTitleValues = nodes
            .map((n) => n.data().properties.title)
            .filter((title, i, arr) => arr.indexOf(title) === i);

          if (uniqueTitleValues.length !== 2 && uniqueTitleValues.length !== 3)
            return true;

          return false;
        })()
      );
    },
  });

  return (
    <>
      <Tooltip title="Settings" arrow>
        <IconButton
          {...btnStyle}
          onClick={() => {
            props.setShowSettings(true);
          }}>
          <SettingsIcon />
        </IconButton>
      </Tooltip>

      <Tooltip title="Fit canvas" arrow>
        <IconButton
          {...btnStyle}
          onClick={() =>
            vis.cy().animate(
              {
                fit: vis.cy().nodes(),
              },
              {
                duration: 600,
                easing: "ease-in-out-sine",
              }
            )
          }>
          <FitScreenIcon />
        </IconButton>
      </Tooltip>

      <Tooltip title="Clean up canvas" arrow>
        <IconButton
          {...btnStyle}
          onClick={() => vis.callbacks().requestLayoutRun(true)}>
          <CleaningServicesIcon />
        </IconButton>
      </Tooltip>

      <Popover
        enforceFocus={false}
        placement="bottom-start"
        interactionKind="click"
        content={<HiddenNodesPopover />}
        renderTarget={({ isOpen, ...p }) => (
          <Tooltip title="Hidden nodes" arrow>
            <IconButton {...btnStyle} {...p}>
              <VisibilityIcon />
            </IconButton>
          </Tooltip>
        )}
      />

      <Tooltip title="Show cell-cell interaction" arrow>
        <span>
          {" "}
          {/*Span is necessary for material-ui <Tooltip>*/}
          <IconButton
            {...btnStyle}
            disabled={interactionDisabled}
            onClick={() => {
              showCellCellInteraction(vis.callbacks().addLayout, vis.cy());
            }}>
            <CellCellInteractionIcon disabled={interactionDisabled} />
          </IconButton>
        </span>
      </Tooltip>
      <Tooltip title="Expand all neighbors" arrow>
        <IconButton
          {...btnStyle}
          onClick={() => {
            const neighborIds = vis
              .cy()
              .nodes()
              .filter((n) => n.data().type === "neighbor")
              .map((n) => n.data().turing_id);
            vis
              .callbacks()
              .setSelectedNodeIds(
                [...vis.state().selectedNodeIds, ...neighborIds].flat()
              );
          }}>
          <UnfoldMoreIcon />
        </IconButton>
      </Tooltip>

      <Popover
        enforceFocus={false}
        placement="bottom-start"
        interactionKind="click"
        content={<SearchNodesWindow />}
        renderTarget={({ isOpen, ...p }) => (
          <IconButton {...btnStyle} {...p}>
            <SearchIcon />
          </IconButton>
        )}
      />
    </>
  );
};

export default ActionsToolbar;
