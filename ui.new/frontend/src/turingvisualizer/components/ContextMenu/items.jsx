// @blueprintjs
import { Icon, Button, MenuItem } from "@blueprintjs/core";

// Turing
import { useVisualizerContext } from "../../";
import { SelectMenu } from "./select";

const titleSizeLimit = 40;

export const ItemLayoutVerticalLine = ({ actions }) => (
  <MenuItem
    icon={<Icon icon="layout-linear" style={{ rotate: "90deg" }} />}
    text="Vertical line"
    onClick={actions.setVerticalLine}
  />
);

export const ItemLayoutAuto = () => {
  const vis = useVisualizerContext();

  return (
    <MenuItem
      icon="layout-auto"
      text="Auto"
      onClick={() => {
        const eles = vis.cy().filter((e) => e.selected());
        if (eles.length === 0) return;
        vis.callbacks().resetLayout(eles.map((e) => e.id()));
      }}
    />
  );
};

export const ItemSelectNeighborhood = () => {
  const vis = useVisualizerContext();
  const data = vis.contextMenuData().data;

  return (
    <MenuItem
      icon="search-around"
      text="Select neighborhood"
      onClick={() => {
        vis.cy().elements().unselect();
        vis.cy().$id(data.id).neighborhood().select();
        vis.cy().$id(data.id).select();
      }}
    />
  );
};

export const ItemSelectAllBySameNodeType = ({ actions }) => {
  const vis = useVisualizerContext();
  const nt = vis.contextMenuData().data.node_type_name;
  return (
    <MenuItem
      icon="form"
      text="by same node type"
      onClick={() => actions.selectAllBySameNodeType(nt)}></MenuItem>
  );
};

export const ItemSelectAllBySameNodeTypeNoData = ({ actions }) => {
  const vis = useVisualizerContext();
  const nodeTypes = [
    ...new Set(
      vis
        .cy()
        .nodes()
        .map((n) => n.data().node_type_name)
    ),
  ];

  const renderNodeType = (nt) => {
    return (
      <MenuItem
        key={"node-type-" + nt}
        text={nt.length < titleSizeLimit
          ? nt
          : nt.slice(0, titleSizeLimit) + "..."}
        onClick={() => actions.selectAllBySameNodeType(nt)}
      />
    );
  };

  return (
    <SelectMenu
      key={"select-menu-select-node-by-node-type"}
      renderChild={renderNodeType}
      items={nodeTypes}
      Parent={
        <Button
          key="select-node-by-node-type"
          fill
          alignText="left"
          icon="label"
          text="Select by node type..."
          rightIcon="caret-right"
          small
        />
      }
    />
  );
};

export const ItemSelectAllBySameProperty = ({ actions }) => {
  const vis = useVisualizerContext();
  const nodeProps = vis.contextMenuData().data.properties;

  return (
    <MenuItem icon="property" text="by same property...">
      {Object.keys(nodeProps).map((propName) => (
        <MenuItem
          key={propName}
          icon="label"
          text={propName}
          onClick={() =>
            actions.selectAllBySameNodeProperty(nodeProps[propName], propName)
          }
        />
      ))}
    </MenuItem>
  );
};

export const ItemSelectAllBySamePropertyNoData = ({ actions }) => {
  const vis = useVisualizerContext();
  const nodeProps = [
    ...new Set(
      vis
        .cy()
        .nodes()
        .map((n) => Object.keys(n.data().properties))
        .flat()
    ),
  ];

  const renderPropName = (propName) => {
    const propValues = [
      ...new Set(
        vis
          .cy()
          .nodes()
          .map((n) => n.data().properties[propName])
          .filter((p) => p !== undefined)
      ),
    ];

    const renderPropValue = (propValue) => {
      return (
        <MenuItem
          key={"select-node-by-property-" + propName + "-" + propValue}
          text={
            propValue.length < titleSizeLimit
              ? propValue
              : propValue.slice(0, titleSizeLimit) + "..."
          }
          onClick={() =>
            actions.selectAllBySameNodeProperty(propValue, propName)
          }
        />
      );
    };

    return (
      <SelectMenu
        key={"select-menu-select-node-by-property-" + propName}
        renderChild={renderPropValue}
        items={propValues}
        Parent={
          <MenuItem
            key={"select-node-by-property-" + propName}
            text={
              propName.length < titleSizeLimit
                ? propName
                : propName.slice(0, titleSizeLimit) + "..."
            }
            shouldDismissPopover={false}
          />
        }
      />
    );
  };

  return (
    <SelectMenu
      key={"select-menu-select-node-by-property"}
      renderChild={renderPropName}
      items={nodeProps}
      Parent={
        <Button
          key="select-node-by-property"
          fill
          alignText="left"
          icon="label"
          text="Select by property..."
          rightIcon="caret-right"
          small
        />
      }
    />
  );
};

export const ItemSelectAllBySamePropertyUnique = ({ propName, actions }) => {
  const vis = useVisualizerContext();
  const values = [
    ...new Set(
      vis
        .cy()
        .nodes()
        .map((n) => n.data().properties[propName])
        .flat()
    ),
  ];

  return (
    <MenuItem text={propName} icon="label">
      {values.map((v) => (
        <MenuItem
          key={"node-property-" + v}
          text={v}
          onClick={() => actions.selectAllBySameNodeProperty(v, propName)}
        />
      ))}
    </MenuItem>
  );
};

export const ItemSelectAllBySameLayout = () => {
  const vis = useVisualizerContext();
  const data = vis.contextMenuData().data;

  return (
    <MenuItem
      icon="layout"
      text="by same layout"
      onClick={() =>
        vis
          .cy()
          .filter(
            (e) =>
              vis.state().layouts.mapping[e.id()] ===
              vis.state().layouts.mapping[data.id]
          )
          .forEach((e) => e.select())
      }
    />
  );
};

export const ItemHideNodes = ({ actions }) => (
  <MenuItem
    text="Hide"
    intent="danger"
    icon="graph-remove"
    onClick={() => actions.hideNodes()}
  />
);

export const ItemKeepOnly = ({ actions }) => {
  const vis = useVisualizerContext();

  return (
    <MenuItem
      icon="hurricane"
      text="Keep only"
      intent="danger"
      onClick={() => vis.dialogs()["keep-only-alert"].toggle({actions})}
    />
  );
};

export const ItemCollapseNeighbors = ({ actions }) => {
  return (
    <MenuItem
      text="Collapse neighbors"
      icon="collapse-all"
      onClick={() => actions.collapseNeighbors()}
    />
  );
};

export const ItemExpandNeighbors = ({ actions }) => {
  return (
    <MenuItem
      text="Expand neighbors"
      icon="expand-all"
      intent="success"
      onClick={() => actions.expandNeighbors()}
    />
  );
};

export const ItemAddToNetwork = ({ actions }) => {
  return (
    <MenuItem
      text="Add to network"
      intent="success"
      icon="add"
      onClick={() => actions.addToNetwork()}
    />
  );
};

export const ItemSearchNodes = () => {
  const vis = useVisualizerContext();
  return (
    <MenuItem
      text="Search nodes..."
      icon="search"
      labelElement={<Icon icon="share" />}
      onClick={vis.dialogs()["search-nodes"].toggle}
      popoverProps={{
        interactionKind: "click",
      }}
    />
  );
};
