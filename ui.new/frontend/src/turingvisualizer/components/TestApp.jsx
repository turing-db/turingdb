import { Button, Section, ButtonGroup } from "@blueprintjs/core";
import React from "react";

import { useVisualizerContext } from "../context";
import { useCanvasTrigger } from "../useCanvasTrigger";
import style from "../style";

import {
  Canvas,
  Visualizer,
  VisualizerContextProvider,
  ActionsToolbar,
  TuringContextMenu,
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
    <>
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
        <div style={{ margin: 10 }}>
          <ButtonGroup>
            <AddNodeButton />
            <RemoveNodeButton />
          </ButtonGroup>
        </div>
        <div style={{ margin: 10, marginTop: 0 }}>
          <ActionsToolbar />
        </div>
      </Visualizer>
    </>
  );
};

const App = () => {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "row",
        height: "100vh",
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
            flexGrow: 1,
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
            backgroundColor: "black",
            flex: 1,
            flexGrow: 1,
          }}>
          <AppContent hideHowTo />
        </div>
      </VisualizerContextProvider>
    </div>
  );
};

export default App;
