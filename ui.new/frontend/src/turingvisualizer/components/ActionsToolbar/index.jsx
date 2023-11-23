// Core
import React from "react";

// @blueprintjs
import {
  ButtonGroup,
  Tooltip,
  Popover,
  Button,
} from "@blueprintjs/core";

// Turing
import { useVisualizerContext, useCanvasTrigger } from "../../";
import SelectNodesMenu from "./SelectNodesMenu";
import LabelMenus from "./LabelMenus";

const ttParams = {
  hoverCloseDelay: 40,
  hoverOpenDelay: 400,
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
  settingsAction = true,
  selectAction = true,
  fitAction = true,
  cleanAction = true,
  hiddenNodesAction = true,
  cellCellInteraction = true,
  expandAction = true,
  collapseAction = true,
  searchAction = true,
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
    <div
      style={{
        display: "flex",
        flex: 1,
        alignItems: "space-between",
        flexWrap: "wrap"
      }}>
      <div style={{ display: "flex", flex: 1 }}>
        <ButtonGroup style={{ padding: 5 }}>
          {fitAction && (
            <Tooltip  {...ttParams} content="Fit canvas">
              <Button
                icon="zoom-to-fit"
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
                }
              />
            </Tooltip>
          )}

          {cleanAction && (
            <Tooltip  {...ttParams}content="Clean up canvas">
              <Button
                onClick={() => vis.callbacks().requestLayoutRun(true)}
                icon="eraser"
              />
            </Tooltip>
          )}

          {hiddenNodesAction && (
            <Tooltip  {...ttParams}content="Hidden nodes">
              <Button
                icon="eye-open"
                onClick={vis.dialogs()["hidden-nodes"].toggle}
              />
            </Tooltip>
          )}

          {cellCellInteraction && (
            <Tooltip  {...ttParams}content="Show cell-cell interaction">
              <Button
                icon="intersection"
                disabled={interactionDisabled}
                onClick={() => {
                  showCellCellInteraction(vis.callbacks().addLayout, vis.cy());
                }}
              />
            </Tooltip>
          )}

          {expandAction && (
            <Tooltip  {...ttParams}content="Expand all neighbors">
              <Button
                icon="expand-all"
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
                }}
              />
            </Tooltip>
          )}

          {collapseAction && (
            <Tooltip  {...ttParams}content="Collapse all neighbors">
              <Button
                icon="collapse-all"
                onClick={() => {
                  const selNodes = vis
                    .cy()
                    .nodes()
                    .filter((e) => e.data().type === "selected");
                  const neiData = Object.fromEntries(
                    selNodes
                      .neighborhood()
                      .filter(
                        (e) =>
                          e.group() === "nodes" && e.data().type === "neighbor"
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
                }}
              />
            </Tooltip>
          )}
        </ButtonGroup>

        <ButtonGroup style={{ padding: 5 }}>
          <LabelMenus />
        </ButtonGroup>
      </div>

      <ButtonGroup style={{ padding: 5 }}>
        {settingsAction && (
          <Tooltip  {...ttParams}content="Settings">
            <Button
              onClick={vis.dialogs()["show-settings"].toggle}
              text="Settings"
              icon="settings"
            />
          </Tooltip>
        )}

        {searchAction && (
          <Tooltip  {...ttParams}content="Search nodes in current view">
            <Button
              icon="search"
              text="Search"
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
