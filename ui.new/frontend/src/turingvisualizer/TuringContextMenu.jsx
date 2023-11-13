// Core
import React from "react";

// @blueprintjs
import { Icon, Menu, MenuDivider, MenuItem } from "@blueprintjs/core";

// Turing
import { COLOR_MODES, DISCRETE_COLORS } from "./constants";
import { ContextMenu, useContextMenuData, useVisualizerContext } from "./";

const NodeContextMenu = (props) => {
  const vis = useVisualizerContext();
  const cy = vis.cy();
  const selectedNodeIds = vis.state().selectedNodeIds;
  const layouts = vis.state().layouts;
  const nodeIds = Object.fromEntries(selectedNodeIds.map((id) => [id, true]));
  const data = vis.contextMenuData().data;
  const isSelected = nodeIds[data.turing_id];
  const nodeProps = data.properties;

  const highlightedNodes = cy.nodes().filter((e) => e.selected());
  const highlightedSelectedNodes = highlightedNodes.filter(
    (e) => e.data().type === "selected"
  );
  const highlightedNeighborNodes = highlightedNodes.filter(
    (e) => e.data().type === "neighbor"
  );
  const highlightedSelectedNodeIds = Object.fromEntries(
    highlightedSelectedNodes.map((n) => [n.data().turing_id, true])
  );
  const highlightedNeighborNodeData = Object.fromEntries(
    highlightedNeighborNodes.map((n) => [
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

  const hideNodes = React.useCallback(() => {
    vis
      .callbacks()
      .setSelectedNodeIds(
        selectedNodeIds.filter((id) => !highlightedSelectedNodeIds[id])
      );
    vis.callbacks().hideNodes(highlightedNeighborNodeData);
  }, [
    vis,
    highlightedSelectedNodeIds,
    highlightedNeighborNodeData,
    selectedNodeIds,
  ]);

  const collapseNeighbors = React.useCallback(() => {
    const neighborData = Object.fromEntries(
      highlightedSelectedNodes
        .neighborhood()
        .filter((e) => e.group() === "nodes" && e.data().type === "neighbor")
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
    vis.callbacks().hideNodes(neighborData);
  }, [vis, highlightedSelectedNodes]);

  const keepOnly = React.useCallback(() => {
    const eles = vis
      .cy()
      .nodes()
      .filter((e) => e.selected());
    vis.callbacks().setSelectedNodeIds(eles.map((e) => e.data().turing_id));
  }, [vis]);

  const addToNetwork = React.useCallback(() => {
    const eles = vis
      .cy()
      .nodes()
      .filter((e) => e.selected());
    vis
      .callbacks()
      .setSelectedNodeIds([
        ...vis.state().selectedNodeIds,
        ...eles.map((e) => e.data().turing_id),
      ]);
  }, [vis]);

  const setVerticalLine = React.useCallback(() => {
    const eles = cy.filter((e) => e.selected());
    if (eles.length === 0) return;

    const newLayout = {
      name: "preset",
      fit: false,
      positions: {
        ...Object.fromEntries(
          eles.map((e, i) => [
            e.id(),
            { x: layouts.layoutCount * 50.0, y: i * 40.0 },
          ])
        ),
      },
    };

    vis.callbacks().addLayout(
      newLayout,
      eles.map((e) => e.id())
    );
  }, [vis, cy, layouts.layoutCount]);

  const VerticalLineIcon = () => (
    <Icon icon="layout-linear" style={{ rotate: "90deg" }} />
  );

  return (
    <Menu>
      <NodeColorContextMenu nodePropertyTypes={props.nodePropertyTypes} />
      <MenuItem icon="layout" text="Set layout...">
        <MenuItem
          icon={<VerticalLineIcon />}
          text="Vertical line"
          onClick={setVerticalLine}
        />
        <MenuItem
          icon="layout-auto"
          text="Auto"
          onClick={() => {
            const eles = cy.filter((e) => e.selected());
            if (eles.length === 0) return;
            vis.callbacks().resetLayout(eles.map((e) => e.id()));
          }}
        />
      </MenuItem>
      <MenuItem text="Select all..." icon="select">
        <MenuItem icon="property" text="by same property...">
          {Object.keys(nodeProps).map((propName) => (
            <MenuItem
              key={propName}
              icon="label"
              text={propName}
              onClick={() =>
                cy
                  .filter(
                    (e) =>
                      e.data().properties[propName] &&
                      e.data().properties[propName] === nodeProps[propName]
                  )
                  .forEach((e) => e.select())
              }
            />
          ))}
        </MenuItem>
        <MenuItem
          icon="property"
          text="by same layout"
          onClick={() =>
            cy
              .filter(
                (e) => layouts.mapping[e.id()] === layouts.mapping[data.id]
              )
              .forEach((e) => e.select())
          }
        />
      </MenuItem>

      <MenuItem
        text="Hide"
        intent="danger"
        icon="graph-remove"
        onClick={() => hideNodes()}
      />
      <MenuItem
        icon="hurricane"
        text="Keep only"
        intent="danger"
        onClick={keepOnly}
      />

      {isSelected ? (
        // Selected nodes only
        <>
          <MenuItem
            text="Collapse neighbors"
            icon="collapse-all"
            onClick={() => collapseNeighbors()}
          />
        </>
      ) : (
        // Neighbors nodes only
        <>
          <MenuItem
            text="Add to network"
            intent="success"
            icon="add"
            onClick={addToNetwork}
          />
        </>
      )}
    </Menu>
  );
};

const EdgeColorContextMenu = (props) => {
  return (
    <Menu>
      <MenuItem text="Set edge colors..." icon="one-to-one">
        <MenuDivider title="General" />
        <colorMenuItems.EdgeColorContextMenu_None />

        <colorMenuItems.EdgeColorContextMenu_Preset />

        <MenuDivider title="Gradient" />
        <colorMenuItems.EdgeColorContextMenu_GradientProperty
          propertyTypes={props.edgePropertyTypes}
        />

        <MenuDivider title="Discrete" />
        <colorMenuItems.EdgeColorContextMenu_DiscreteProperty
          propertyTypes={props.edgePropertyTypes}
        />
        <colorMenuItems.EdgeColorContextMenu_EdgeType />
      </MenuItem>
    </Menu>
  );
};

const NodeColorContextMenu = (props) => {
  return (
    <>
      <MenuItem text="Set node colors..." icon="graph">
        <MenuDivider title="General" />
        <colorMenuItems.NodeColorContextMenu_None />

        <colorMenuItems.NodeColorContextMenu_Preset />

        <MenuDivider title="Gradient" />
        <colorMenuItems.NodeColorContextMenu_GradientProperty
          propertyTypes={props.nodePropertyTypes}
        />

        <MenuDivider title="Discrete" />
        <colorMenuItems.NodeColorContextMenu_DiscreteProperty
          propertyTypes={props.nodePropertyTypes}
        />
        <colorMenuItems.NodeColorContextMenu_NodeType />
      </MenuItem>
    </>
  );
};

const EdgeContextMenu = (props) => {
  return (
    <Menu>
      <EdgeColorContextMenu edgePropertyTypes={props.edgePropertyTypes} />
    </Menu>
  );
};

const getPropertyTypesFromElements = (elements) =>
  elements
    .map((e) => Object.keys(e.data().properties))
    .flat()
    .filter((p, i, arr) => arr.indexOf(p) === i);

const BackgroundContextMenu = (props) => {
  return (
    <Menu>
      <MenuDivider title="colors" />
      <EdgeColorContextMenu edgePropertyTypes={props.edgePropertyTypes} />
      <NodeColorContextMenu nodePropertyTypes={props.nodePropertyTypes} />
    </Menu>
  );
};
const TuringContextMenu = () => {
  // Call this hook at the beginning of your ContextMenuComponent
  useContextMenuData();

  const vis = useVisualizerContext();
  const cy = vis.cy();

  const edgePropertyTypes = getPropertyTypesFromElements(cy?.edges() || []);
  const nodePropertyTypes = getPropertyTypesFromElements(cy?.nodes() || []);

  const menuData = vis.contextMenuData();

  return (
    <ContextMenu>
      {menuData.group === "nodes" && (
        <NodeContextMenu nodePropertyTypes={nodePropertyTypes} />
      )}
      {menuData.group === "edges" && (
        <EdgeContextMenu edgePropertyTypes={edgePropertyTypes} />
      )}
      {menuData.group === "background" && (
        <BackgroundContextMenu
          nodePropertyTypes={nodePropertyTypes}
          edgePropertyTypes={edgePropertyTypes}
        />
      )}
    </ContextMenu>
  );
};

const ColorMenuItem = ({
  text, // text that appears on the menu item
  icon, // icon that appears on the menu item
  elGroup, // "nodes" | "edges"
  colorMode, // ex: COLOR_MODES.NodeType
  colorData, // Data required for the color mode, ex { propName, propValues }
}) => {
  const vis = useVisualizerContext();
  const cy = vis.cy();

  return (
    <MenuItem
      text={text}
      icon={icon}
      onClick={() => {
        const selectedElements = cy[elGroup]().filter((n) => n.selected());
        const elements =
          selectedElements.length === 0 ? cy[elGroup]() : selectedElements;

        switch (colorMode) {
          case COLOR_MODES.DiscreteProperty: {
            colorData = {
              ...colorData,
              propValues: [
                ...new Set(
                  cy[elGroup]().map(
                    (e) => e.data().properties[colorData.propName]
                  )
                ),
              ],
            };
            break;
          }

          case COLOR_MODES.GradientProperty: {
            colorData = {
              ...colorData,
              propValues: Object.fromEntries(
                elements
                  .filter(
                    (e) =>
                      !isNaN(
                        parseFloat(e.data().properties[colorData.propName])
                      )
                  )
                  .map((e) => [
                    e.id(),
                    parseFloat(e.data().properties[colorData.propName]),
                  ])
              ),
            };
            colorData.min = Math.min.apply(
              Math,
              Object.values(colorData.propValues)
            );
            colorData.max = Math.max.apply(
              Math,
              Object.values(colorData.propValues)
            );
            break;
          }

          default:
            break;
        }

        const setColorMode =
          elGroup === "nodes"
            ? vis.callbacks().setNodeColorMode
            : vis.callbacks().setEdgeColorMode;

        setColorMode(
          colorMode,
          selectedElements.map((n) => n.id()),
          colorData
        );
      }}
    />
  );
};

const colorMenuItems = {
  NodeColorContextMenu_None: () => (
    <ColorMenuItem
      text="None"
      icon="cross"
      elGroup="nodes"
      colorMode={COLOR_MODES.None}
      colorData={{}}
      key="node-none"
    />
  ),
  EdgeColorContextMenu_None: () => (
    <ColorMenuItem
      text="None"
      icon="cross"
      elGroup="edges"
      colorMode={COLOR_MODES.None}
      colorData={{}}
      key="edge-none"
    />
  ),
  NodeColorContextMenu_Preset: () => (
    <MenuItem text="Preset" icon="edit">
      {DISCRETE_COLORS.map((col) => (
        <ColorMenuItem
          text={col}
          icon={<Icon icon="symbol-square" color={col} />}
          elGroup="nodes"
          colorMode={COLOR_MODES.Preset}
          colorData={{ color: col }}
          key={"node-preset-" + col}
        />
      ))}
    </MenuItem>
  ),
  EdgeColorContextMenu_Preset: () => (
    <MenuItem text="Preset" icon="edit">
      {DISCRETE_COLORS.map((col) => (
        <ColorMenuItem
          text={col}
          icon={<Icon icon="symbol-square" color={col} />}
          elGroup="edges"
          colorMode={COLOR_MODES.Preset}
          colorData={{ color: col }}
          key={"edge-preset-" + col}
        />
      ))}
    </MenuItem>
  ),
  NodeColorContextMenu_NodeType: () => (
    <ColorMenuItem
      text="Based on node type"
      icon="graph"
      elGroup="nodes"
      colorMode={COLOR_MODES.NodeType}
      colorData={{}}
      key="node-type"
    />
  ),
  EdgeColorContextMenu_EdgeType: () => (
    <ColorMenuItem
      text="Based on edge type"
      icon="one-to-one"
      elGroup="edges"
      colorMode={COLOR_MODES.EdgeType}
      colorData={{}}
      key="edge-type"
    />
  ),
  NodeColorContextMenu_GradientProperty: ({ propertyTypes }) => (
    <MenuItem
      text="Based on property..."
      icon="property"
      key="node-gradient-property">
      {propertyTypes.map((propName) => (
        <ColorMenuItem
          text={propName}
          icon="label"
          elGroup="nodes"
          colorMode={COLOR_MODES.GradientProperty}
          colorData={{ propName }}
          key={"node-gradient-" + propName}
        />
      ))}
    </MenuItem>
  ),
  EdgeColorContextMenu_GradientProperty: ({ propertyTypes }) => (
    <MenuItem
      text="Based on property..."
      icon="property"
      key="edge-gradient-property">
      {propertyTypes.map((propName) => (
        <ColorMenuItem
          text={propName}
          icon="label"
          elGroup="edges"
          colorMode={COLOR_MODES.GradientProperty}
          colorData={{ propName }}
          key={"edge-gradient-" + propName}
        />
      ))}
    </MenuItem>
  ),
  NodeColorContextMenu_DiscreteProperty: ({ propertyTypes }) => (
    <MenuItem
      text="Based on property..."
      icon="property"
      key="node-discrete-property">
      {propertyTypes.map((propName) => (
        <ColorMenuItem
          text={propName}
          icon="label"
          elGroup="nodes"
          colorMode={COLOR_MODES.DiscreteProperty}
          colorData={{ propName }}
          key={"node-discrete-" + propName}
        />
      ))}
    </MenuItem>
  ),
  EdgeColorContextMenu_DiscreteProperty: ({ propertyTypes }) => (
    <MenuItem
      text="Based on property..."
      icon="property"
      key="edge-discrete-property">
      {propertyTypes.map((propName) => (
        <ColorMenuItem
          text={propName}
          icon="label"
          elGroup="edges"
          colorMode={COLOR_MODES.DiscreteProperty}
          colorData={{ propName }}
          key={"edge-discrete-" + propName}
        />
      ))}
    </MenuItem>
  ),
};

export default TuringContextMenu;
