// Core
import React from "react";
import { useTheme } from "@emotion/react";
import { useDispatch, useSelector } from "react-redux";

// @mui
import { Stack, Paper, Autocomplete, TextField } from "@mui/material";

// Turing
import {
  Canvas,
  Visualizer,
  VisualizerContextProvider,
  TuringContextMenu,
  useVisualizerContext,
  style,
} from "@turingvisualizer";
import SettingsModal from "@ViewerPage/SettingsModal";
import * as actions from "@App/actions";
import * as thunks from "@App/thunks";
import { useCanvasTrigger } from "@turingvisualizer/tools";
import { useSelectorRef } from "@App/tools";
import * as cyEvents from "@turingvisualizer/events";
import ActionsToolbar from "@ViewerPage/ActionsToolbar";

const useDefinedState = (initValue) => {
  const [value, set] = React.useState(initValue);
  const ready = React.useRef(false);

  return {
    value, // The actual value
    set, // Sets the value
    ready, // Holds true if value was initialized
    init: (v) => {
      // Sets the value only the first time it is called
      if (!ready.current) {
        ready.current = true;
        set(v);
      }
    },
  };
};

const LabelsToolbar = () => {
  const vis = useVisualizerContext();
  const dispatch = useDispatch();

  const [nodeLabels, setNodeLabels] = React.useState([]);
  const [edgeLabels, setEdgeLabels] = React.useState([]);
  const appNodeLabel = useSelector((state) => state.nodeLabel);
  const appEdgeLabel = useSelector((state) => state.edgeLabel);
  const currentNodeLabel = useDefinedState("Node Type");
  const currentEdgeLabel = useDefinedState("Edge Type");

  // NodeLabel
  React.useEffect(() => {
    if (!nodeLabels.includes(appNodeLabel)) return;
    currentNodeLabel.init(appNodeLabel);
  }, [nodeLabels, appNodeLabel, currentNodeLabel]);

  React.useEffect(() => {
    if (currentNodeLabel.value === vis.state().nodeLabel) return;
    vis.callbacks().setNodeLabel(currentNodeLabel.value);
    if (currentNodeLabel.value === appNodeLabel) return;
    dispatch(actions.setNodeLabel(currentNodeLabel.value));
  }, [vis, dispatch, currentNodeLabel.value, appNodeLabel]);

  useCanvasTrigger({
    category: "elements",
    name: "setNodeLabels",

    callback: () => {
      const properties = [
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

      if (JSON.stringify(properties) !== JSON.stringify(nodeLabels)) {
        setNodeLabels(properties);
      }
    },
  });

  // EdgeLabel
  React.useEffect(() => {
    if (!edgeLabels.includes(appEdgeLabel)) return;
    currentEdgeLabel.init(appEdgeLabel);
  }, [edgeLabels, appEdgeLabel, currentEdgeLabel]);

  React.useEffect(() => {
    if (currentEdgeLabel.value === vis.state().edgeLabel) return;
    vis.callbacks().setEdgeLabel(currentEdgeLabel.value);
    if (currentEdgeLabel.value === appEdgeLabel) return;
    dispatch(actions.setEdgeLabel(currentEdgeLabel.value));
  }, [vis, dispatch, currentEdgeLabel.value, appEdgeLabel]);

  useCanvasTrigger({
    category: "elements",
    name: "setEdgeLabels",

    callback: () => {
      const properties = [
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

      if (JSON.stringify(properties) !== JSON.stringify(edgeLabels)) {
        setEdgeLabels(properties);
      }
    },
  });

  return (
    <>
      <Autocomplete
        disablePortal
        blurOnSelect
        id="node-property-selector"
        autoSelect
        autoHighlight
        options={[...nodeLabels]}
        sx={{ width: 200, border: "none" }}
        value={currentNodeLabel.value}
        size="small"
        onChange={(_e, v) => currentNodeLabel.set(v)}
        renderInput={(params) => (
          <TextField {...params} label="Node label" variant="standard" />
        )}
      />
      <Autocomplete
        disablePortal
        blurOnSelect
        id="edge-property-selector"
        autoSelect
        autoHighlight
        options={[...edgeLabels]}
        sx={{ width: 200, border: "none" }}
        value={currentEdgeLabel.value}
        size="small"
        onChange={(_e, v) => currentEdgeLabel.set(v)}
        renderInput={(params) => (
          <TextField {...params} label="Edge label" variant="standard" />
        )}
      />
    </>
  );
};

const ViewerPageContent = () => {
  const vis = useVisualizerContext();
  const selectedNodesRef = useSelectorRef("selectedNodes");
  const dbName = useSelector((state) => state.dbName);
  const dispatch = useDispatch();

  const [showSettings, setShowSettings] = React.useState(false);

  React.useEffect(
    () =>
      vis.callbacks().setSelectedNodeIds(Object.keys(selectedNodesRef.current)),
    [vis, selectedNodesRef]
  );

  vis.refs.events.current.onetap = (vis, e) => {
    if (e.target.group === undefined) return;
    if (e.target.group() !== "nodes") return;
    dispatch(thunks.inspectNode(dbName, e.target.data().turing_id));
    cyEvents.onetap(vis, e);
  };

  useCanvasTrigger({
    category: "selectedNodeIds",
    name: "setSelectedNodeIds",

    callback: () => {
      const nodeIds = vis
        .state()
        .elements.filter(
          (e) => e.group === "nodes" && e.data.type === "selected"
        )
        .map((e) => e.data.turing_id)
        .sort();
      const currentIds = Object.keys(selectedNodesRef.current).sort();

      if (nodeIds.toString() !== currentIds.toString()) {
        const currentIdsMap = Object.fromEntries(
          currentIds.map((id) => [id, true])
        );
        const nodeIdsMap = Object.fromEntries(nodeIds.map((id) => [id, true]));
        const unknownNodeIds = nodeIds.filter((id) => !currentIdsMap[id]);
        const selectedNodesWithoutRemoved = Object.values(
          selectedNodesRef.current
        ).filter((n) => nodeIdsMap[n.id]);

        dispatch(
          thunks.getNodes(dbName, unknownNodeIds, { yield_edges: false })
        ).then((res) => {
          const unknownNodes = Object.values(res);
          dispatch(
            actions.setSelectedNodes([
              ...selectedNodesWithoutRemoved,
              ...unknownNodes,
            ])
          );
        });
      }
    },
  });

  return (
    <div style={{ display: "flex", flex: 1, flexDirection: "column" }}>
      <SettingsModal open={showSettings} setOpen={setShowSettings} />
      <Visualizer
        canvas={<Canvas />}
        contextMenu={<TuringContextMenu />}
        cyStyle={style}>
        <div
          style={{
            margin: 10,
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
          }}>
          <Paper
            elevation={5}
            style={{
              width: "max-content",
              borderRadius: "25vh",
              paddingRight: 20,
            }}>
            <Stack
              direction="row"
              style={{
                overflow: "hidden",
              }}>
              <ActionsToolbar setShowSettings={setShowSettings} />
              <LabelsToolbar />
            </Stack>
          </Paper>
        </div>
      </Visualizer>
    </div>
  );
};

const ViewerPage = () => {
  const theme = useTheme();
  const dbName = useSelector((state) => state.dbName);

  return (
    <VisualizerContextProvider
      themeMode={theme.palette.mode}
      dbName={dbName}
      containerId="cy-admin">
      <ViewerPageContent />
    </VisualizerContextProvider>
  );
};

export default ViewerPage;
