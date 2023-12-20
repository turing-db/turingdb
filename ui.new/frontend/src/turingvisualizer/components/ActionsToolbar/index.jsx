// Core
import React from "react";

// @blueprintjs
import { ButtonGroup, Tooltip, Popover, Button } from "@blueprintjs/core";

// Turing
import { useVisualizerContext } from "../../context";
import { useCanvasTrigger } from "../../useCanvasTrigger";

import { ttParams } from "./tools";
import LabelMenus from "./LabelMenus";
import SelectNodesMenu from "./SelectNodesMenu";
import SettingsDialog from "./SettingsDialog";
import HiddenNodesDialog from "./HiddenNodesDialog";
import { SearchNodesDialogButton } from "./SearchNodesDialog";
import AddNodeDialog from "./AddNodeDialog";
import { useMenuActions } from "../ContextMenu/hooks";

const showCellCellInteraction = (addLayout, cy) => {
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
  const yStretch = 150.0;
  const aspectRatio = cy.width() / cy.height();
  const layoutHeight = maxLength * yStretch;
  const layoutWidth = layoutHeight * aspectRatio;
  const xStretch = layoutWidth / (nLines - 1);

  uniqueTitleValues.forEach((_title, x) => {
    const currentNodes = filteredNodes[x];
    const xPos = x * xStretch - layoutWidth / 2;
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
  settingsAction = true,
  selectAction = true,
  fitAction = true,
  cleanAction = true,
  hiddenNodesAction = true,
  cellCellInteraction = true,
  expandAction = true,
  collapseAction = true,
  searchAction = true,
  searchDatabaseAction = true,
}) => {
  const vis = useVisualizerContext();
  const [interactionDisabled, setInteractionDisabled] = React.useState(true);
  const [collapseDisabled, setCollapseDisabled] = React.useState(false);
  const actions = useMenuActions();

  vis.hookEvent("select", "actionDisabled", () => {
    const selectedNodes = vis.cy().$(":selected");
    const selectedMainNodes = selectedNodes.filter(
      (n) => n.data().type === "selected"
    );

    setCollapseDisabled(
      selectedNodes.length !== 0 && selectedMainNodes.length === 0
    );
  });

  useCanvasTrigger({
    category: "elements",
    name: "actionsToolbar-actionDisabled",
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

  const bpTheme = vis.state().themeMode === "dark" ? "bp5-dark" : "";

  return (
    <div className={`flex flex-1 justify-between flex-wrap ${bpTheme}`}>
      <ButtonGroup>
        {fitAction && (
          <Tooltip {...ttParams} content="Fit canvas">
            <Button
              icon="zoom-to-fit"
              onClick={() => {
                vis.cy().animate(
                  {
                    fit: {
                      eles: vis.cy().elements(),
                      padding: 100,
                    },
                  },
                  {
                    duration: 600,
                    easing: "ease-in-out-sine",
                    queue: true,
                  }
                );
              }}
            />
          </Tooltip>
        )}

        {cleanAction && (
          <Tooltip {...ttParams} content="Clean up canvas">
            <Button
              onClick={() => vis.callbacks().requestLayoutRun(true)}
              icon="eraser"
            />
          </Tooltip>
        )}

        {hiddenNodesAction && <HiddenNodesDialog />}

        {cellCellInteraction && (
          <Tooltip {...ttParams} content="Show cell-cell interaction">
            <Button
              icon="intersection"
              disabled={interactionDisabled}
              onClick={() => {
                showCellCellInteraction(vis.callbacks().addLayout, vis.cy());
                vis.callbacks().requestLayoutFit(true);
              }}
            />
          </Tooltip>
        )}

        {expandAction && (
          <Tooltip {...ttParams} content="Expand all neighbors">
            <Button
              icon="expand-all"
              onClick={() => {
                actions.expandNeighbors();
              }}
            />
          </Tooltip>
        )}

        {collapseAction && (
          <Tooltip {...ttParams} content="Hides neighbors">
            <Button
              icon="collapse-all"
              disabled={collapseDisabled}
              onClick={() => {
                actions.collapseNeighbors();
              }}
            />
          </Tooltip>
        )}

        <LabelMenus />
        {searchDatabaseAction && <AddNodeDialog />}
      </ButtonGroup>

      <ButtonGroup>
        {settingsAction && <SettingsDialog />}
        {searchAction && <SearchNodesDialogButton />}

        {selectAction && (
          <Popover
            enforceFocus={false}
            placement="bottom-start"
            interactionKind="click"
            content={<SelectNodesMenu />}
            renderTarget={({ isOpen, ...p }) => (
              <Tooltip content="Select nodes" {...ttParams}>
                <Button text={"Select"} icon="select" {...p} />
              </Tooltip>
            )}
          />
        )}
      </ButtonGroup>
    </div>
  );
};

export default ActionsToolbar;
