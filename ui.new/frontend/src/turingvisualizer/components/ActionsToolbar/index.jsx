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
import { useVisualizerContext, useCanvasTrigger } from "../../";
import SelectNodesMenu from "./SelectNodesMenu";
import HiddenNodesPopover from "./HiddenNodesPopover";

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

const ActionsToolbar = ({
  settingsAction = false,
  selectAction = true,
  fitAction = true,
  cleanAction = true,
  hiddenNodesAction = true,
  cellCellInteraction = true,
  expandAction = true,
  collapseAction = true,
  searchAction = true,
  setShowSettings,
}) => {
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
      {settingsAction && (
        <Tooltip title="Settings" arrow>
          <IconButton
            {...btnStyle}
            onClick={() => {
              setShowSettings(true);
            }}>
            <SettingsIcon />
          </IconButton>
        </Tooltip>
      )}

      {selectAction && (
        <Popover
          enforceFocus={false}
          placement="bottom-start"
          interactionKind="click"
          content={<SelectNodesMenu />}
          renderTarget={({ isOpen, ...p }) => (
            <Tooltip title="Select nodes" arrow>
              <span>
                {" "}
                {/*Span is necessary for material-ui <Tooltip>*/}
                <IconButton {...btnStyle} {...p}>
                  <Icon icon="select" size={22} style={{ paddingTop: 2 }} />
                </IconButton>
              </span>
            </Tooltip>
          )}
        />
      )}

      {fitAction && (
        <Tooltip title="Fit canvas" arrow>
          <IconButton
            {...btnStyle}
            onClick={() =>
              vis.cy().animate(
                {
                  fit: {
                    eles: vis.cy().nodes(),
                    padding: 100,
                  },
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
      )}

      {cleanAction && (
        <Tooltip title="Clean up canvas" arrow>
          <IconButton
            {...btnStyle}
            onClick={() => vis.callbacks().requestLayoutRun(true)}>
            <CleaningServicesIcon />
          </IconButton>
        </Tooltip>
      )}

      {hiddenNodesAction && (
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
      )}

      {cellCellInteraction && (
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
      )}

      {expandAction && (
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
      )}

      {collapseAction && (
        <Tooltip title="Collapse all neighbors" arrow>
          <IconButton
            {...btnStyle}
            onClick={() => {
              const selNodes = vis
                .cy()
                .nodes()
                .filter((e) => e.data().type === "selected");
              const neiData = Object.fromEntries(
                selNodes
                  .neighborhood()
                  .filter(
                    (e) => e.group() === "nodes" && e.data().type === "neighbor"
                  )
                  .map((n) => [
                    n.data().turing_id,
                    {
                      ...n.data(),
                      neighborNodeIds: n
                        .connectedEdges()
                        .map((e) =>
                          e.data().turing_source_id !== n.data().turing_id
                            ? e.data().turing_source_id
                            : e.data().turing_target_id
                        ),
                    },
                  ])
              );
              vis.callbacks().hideNodes(neiData);
            }}>
            <Icon icon="collapse-all" />
          </IconButton>
        </Tooltip>
      )}

      {searchAction && (
        <IconButton
          {...btnStyle}
          onClick={vis.dialogs()["search-nodes"].toggle}>
          <SearchIcon />
        </IconButton>
      )}
    </>
  );
};

export default ActionsToolbar;
