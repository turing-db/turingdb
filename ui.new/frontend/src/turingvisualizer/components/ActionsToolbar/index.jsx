// Core
import React from "react";

// @blueprintjs
import { ButtonGroup, Tooltip, Popover, Button } from "@blueprintjs/core";

// Turing
import { useVisualizerContext, useCanvasTrigger } from "../../";
import SelectNodesMenu from "./SelectNodesMenu";
import LabelMenus from "./LabelMenus";
import { useMenuActions } from "../ContextMenu/hooks";

const ttParams = {
  hoverCloseDelay: 40,
  hoverOpenDelay: 400,
  compact: true,
  openOnTargetFocus: false,
};

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
  developAction = true,
  collapseAction = true,
  searchAction = true,
  searchDatabaseAction = true,
}) => {
  const vis = useVisualizerContext();
  const [interactionDisabled, setInteractionDisabled] = React.useState(true);
  const actions = useMenuActions();

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
    <div
      style={{
        display: "flex",
        flex: 1,
        alignItems: "space-between",
        flexWrap: "wrap",
      }}>
      <div style={{ display: "flex", flex: 1 }}>
        <ButtonGroup style={{ padding: 5 }}>
          {fitAction && (
            <Tooltip {...ttParams} content="Fit canvas">
              <Button
                icon="zoom-to-fit"
                onClick={() =>
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
                  )
                }
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

          {hiddenNodesAction && (
            <Tooltip {...ttParams} content="Hidden nodes">
              <Button
                icon="eye-open"
                onClick={vis.dialogs()["hidden-nodes"].toggle}
              />
            </Tooltip>
          )}

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
                onClick={() => {
                  actions.collapseNeighbors();
                }}
              />
            </Tooltip>
          )}

          {developAction && (
            <Tooltip {...ttParams} content="Develop neighbors">
              <Button
                icon="fullscreen"
                onClick={() => {
                  actions.developNeighbors();
                }}
              />
            </Tooltip>
          )}
        </ButtonGroup>

        <ButtonGroup style={{ padding: 5 }}>
          <LabelMenus />
        </ButtonGroup>
        <ButtonGroup style={{ padding: 5 }}>
          {searchDatabaseAction && (
            <Tooltip {...ttParams} content="Search nodes in the database">
              <Button
                text="Add node"
                icon="database"
                onClick={vis.dialogs()["search-nodes-in-database"].toggle}
              />
            </Tooltip>
          )}
        </ButtonGroup>
      </div>

      <ButtonGroup style={{ padding: 5 }}>
        {settingsAction && (
          <Tooltip {...ttParams} content="Settings">
            <Button
              onClick={vis.dialogs()["show-settings"].toggle}
              text="Settings"
              icon="settings"
            />
          </Tooltip>
        )}

        {searchAction && (
          <Tooltip {...ttParams} content="Search nodes in current view">
            <Button
              icon="search"
              text="Search view"
              onClick={vis.dialogs()["search-nodes"].toggle}
            />
          </Tooltip>
        )}

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
