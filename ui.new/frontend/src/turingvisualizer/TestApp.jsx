import {
  Alignment,
  Menu,
  MenuItem,
  Button,
  Navbar,
  Section,
  Popover,
  ButtonGroup,
} from "@blueprintjs/core";
import React from "react";

import {
  Canvas,
  Visualizer,
  VisualizerContextProvider,
  TuringContextMenu,
  useVisualizerContext,
  useCanvasTrigger,
  style,
} from "./";

const AddNodeButton = () => {
  const vis = useVisualizerContext();
  const onClick = React.useCallback(() => {
    const ids = vis.state().selectedNodeIds;
    vis
      .callbacks()
      .setSelectedNodeIds([...ids, ids.length === 0 ? 1930248 : ids.pop() + 1]);
  }, [vis]);

  return (
    <Button
      className="bp5-minimal"
      icon="add"
      text="Add node"
      outlined
      onClick={onClick}
      style={{ backgroundColor: "white" }}
    />
  );
};

const RemoveNodeButton = () => {
  const vis = useVisualizerContext();

  const onClick = React.useCallback(() => {
    const ids = vis.state().selectedNodeIds;
    vis.callbacks().setSelectedNodeIds(ids.slice(0, ids.length - 1));
  }, [vis]);

  return (
    <Button
      className="bp5-minimal"
      icon="trash"
      text="Delete node"
      outlined
      onClick={onClick}
      style={{ backgroundColor: "white" }}
    />
  );
};

const AppContent = ({ hideHowTo }) => {
  const [nodeLabels, setNodeLabels] = React.useState([]);
  const [edgeLabels, setEdgeLabels] = React.useState([]);

  const vis = useVisualizerContext();
  const cyStyle = [
    ...style,
    {
      selector: "node",
      style: {
        "font-size": "19px", // Example of style modification
      },
    },
  ];

  useCanvasTrigger({
    category: "elements",
    name: "setNodeLabels",

    callback: () => {
      const nodeProperties = [
        ...new Set(
          vis
            .state()
            .elements.filter((e) => e.group === "nodes")
            .map((e) => Object.keys(e.data.properties))
            .flat()
        ),
        "None",
        "Node Type",
      ];

      if (nodeProperties.toString() !== nodeLabels.toString())
        setNodeLabels(nodeProperties);
    },
  });

  useCanvasTrigger({
    category: "elements",
    name: "setEdgeLabels",

    callback: () => {
      const edgeProperties = [
        ...new Set(
          vis
            .state()
            .elements.filter((e) => e.group === "edges")
            .map((e) => Object.keys(e.data.properties))
            .flat()
        ),
        "None",
        "Edge Type",
      ];

      if (edgeProperties.toString() !== edgeLabels.toString())
        setEdgeLabels(edgeProperties);
    },
  });

  return (
    <div
      style={{
        display: "flex",
        flexGrow: 1,
        flexDirection: "column",
      }}>
      <Navbar>
        <Navbar.Group align={Alignment.LEFT}>
          <Navbar.Heading>Turing visualizer</Navbar.Heading>

          <Navbar.Divider />

          <Popover
            placement="bottom"
            content={
              <Menu>
                {nodeLabels.map((label) => (
                  <MenuItem
                    key={"item-" + label}
                    text={label}
                    icon="property"
                    onClick={() => vis.callbacks().setNodeLabel(label)}
                  />
                ))}
              </Menu>
            }>
            <Button
              text="Node label"
              placement="bottom"
              rightIcon="caret-down"
            />
          </Popover>

          <Popover
            placement="bottom"
            content={
              <Menu>
                {edgeLabels.map((label) => (
                  <MenuItem
                    key={"item-" + label}
                    text={label}
                    icon="property"
                    onClick={() => vis.callbacks().setEdgeLabel(label)}
                  />
                ))}
              </Menu>
            }>
            <Button
              text="Edge label"
              placement="bottom"
              rightIcon="caret-down"
            />
          </Popover>
        </Navbar.Group>
      </Navbar>

      {!hideHowTo && (
        <Section title="How to use">
          <ul>
            <li>&lt;CTRL-left mouse drag&gt; Box selection</li>
            <li>
              Right click on nodes, edges or background to open the context menu
            </li>
            <li>Double click a node to expand its neighbors</li>
            <li>Double click background to fit the canvas</li>
          </ul>
        </Section>
      )}
      <Visualizer
        canvas={<Canvas />}
        contextMenu={<TuringContextMenu />}
        cyStyle={cyStyle}>
        <div style={{ padding: 10 }}>
          <ButtonGroup>
            <AddNodeButton />
            <RemoveNodeButton />
          </ButtonGroup>
        </div>
      </Visualizer>
    </div>
  );
};

const App = () => {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "row",
        width: "100vw",
      }}>
      <VisualizerContextProvider
        themeMode="light"
        dbName="reactome"
        containerId="cy1"
        devMode // Use a fake connection to the database
      >
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            height: "100vh",
            flex: 1,
          }}>
          <AppContent />
        </div>
      </VisualizerContextProvider>

      <VisualizerContextProvider
        themeMode="dark"
        dbName="reactome"
        containerId="cy2"
        devMode>
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            height: "100vh",
            backgroundColor: "black",
            flex: 1,
          }}>
          <AppContent hideHowTo />
        </div>
      </VisualizerContextProvider>
    </div>
  );
};

export default App;
