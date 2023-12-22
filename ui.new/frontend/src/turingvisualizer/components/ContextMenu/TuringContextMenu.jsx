// Core
import React from "react";

// @blueprintjs
import { Alert, Icon, Menu, MenuDivider, MenuItem } from "@blueprintjs/core";

// Turing
import { COLOR_MODES, DISCRETE_COLORS } from "../../constants";
import { ContextMenu, useContextMenuData } from "./ContextMenu";
import { useVisualizerContext } from "../../context";
import { useMenuActions } from "./hooks";
import { useCanvasTrigger } from "../../useCanvasTrigger";
import * as items from "./items";
import { useDialog } from "../../tools";

const NodeContextMenu = (props) => {
  const vis = useVisualizerContext();
  const selectedNodeIds = vis.state().selectedNodeIds;
  const nodeIds = Object.fromEntries(selectedNodeIds.map((id) => [id, true]));
  const data = vis.contextMenuData().data;
  const isSelected = nodeIds[data.turing_id];
  const actions = useMenuActions();

  return (
    <Menu>
      <MenuDivider title="General" />
      <NodeColorContextMenu nodePropertyTypes={props.nodePropertyTypes} />
      <MenuItem icon="layout" text="Set layout...">
        <items.ItemLayoutVerticalLine actions={actions} />
        <items.ItemLayoutAuto />
      </MenuItem>

      <MenuDivider title="Selection" />
      <items.ItemSelectNeighborhood />
      <items.ItemSelectUniqueNeighbors />
      <items.ItemSelectFragment />
      <MenuItem text="Select all..." icon="select">
        <items.ItemSelectAllBySameNodeType actions={actions} />
        <items.ItemSelectAllBySameProperty actions={actions} />
        <items.ItemSelectAllBySameLayout />
      </MenuItem>
      <items.ItemSearchNodes/>

      <MenuDivider title="Add / Remove" />
      <items.ItemCollapseNeighbors disabled={!isSelected} actions={actions} />
      <items.ItemExpandNeighbors actions={actions} />
      <items.ItemHideNodes actions={actions} />
      <items.ItemKeepOnly
        actions={actions}
        keepOnlyAlert={props.keepOnlyAlert}
      />
      {!isSelected && <items.ItemAddToNetwork actions={actions} />}
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
  const actions = useMenuActions();

  return (
    <Menu>
      <MenuDivider title="Colors" />
      <EdgeColorContextMenu edgePropertyTypes={props.edgePropertyTypes} />
      <NodeColorContextMenu nodePropertyTypes={props.nodePropertyTypes} />

      <MenuDivider title="Selection" />

      <MenuItem text="Select all..." icon="select">
        <items.ItemSelectAllBySameNodeTypeNoData actions={actions} />
        <items.ItemSelectAllBySamePropertyNoData actions={actions} />
        {/*<items.ItemSelectAllBySameLayout />*/}
      </MenuItem>

      <items.ItemSearchNodes />
    </Menu>
  );
};
const TuringContextMenu = () => {
  const [nodePropertyTypes, setNodePropertyTypes] = React.useState([]);
  const [edgePropertyTypes, setEdgePropertyTypes] = React.useState([]);
  const keepOnlyAlert = useDialog();

  // Call this hook at the beginning of your ContextMenuComponent
  useContextMenuData();

  const vis = useVisualizerContext();
  const cy = vis.cy();
  const actions = useMenuActions();

  useCanvasTrigger({
    category: "elements",
    name: "contextMenu-setPropertyTypes",

    callback: () => {
      setNodePropertyTypes(getPropertyTypesFromElements(cy?.nodes() || []));
      setEdgePropertyTypes(getPropertyTypesFromElements(cy?.edges() || []));
    },
  });

  const menuData = vis.contextMenuData();

  return (
    <>
      <Alert
        isOpen={keepOnlyAlert.isOpen}
        cancelButtonText="Cancel"
        confirmButtonText={`Keep only selected nodes`}
        icon="trash"
        intent="danger"
        onConfirm={() => {
          actions.keepOnly();
          keepOnlyAlert.close();
        }}
        onCancel={keepOnlyAlert.close}>
        <p>
          Are you sure you want to keep only the selected nodes ? You will start
          a new network exploration from this set.
        </p>
      </Alert>
      <ContextMenu>
        {menuData.group === "nodes" && (
          <NodeContextMenu
            nodePropertyTypes={nodePropertyTypes}
            keepOnlyAlert={keepOnlyAlert}
          />
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
    </>
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
          elements.map((n) => n.id()),
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
