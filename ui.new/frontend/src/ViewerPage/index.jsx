// Core
import React from "react";
import { useTheme } from "@emotion/react";
import { useDispatch, useSelector } from "react-redux";

// @mui
import { Paper } from "@mui/material";

// Turing
import {
  Canvas,
  Visualizer,
  VisualizerContextProvider,
  TuringContextMenu,
} from "src/turingvisualizer/components";
import { useVisualizerContext, style } from "src/turingvisualizer";
import * as actions from "src/App/actions";
import * as thunks from "src/App/thunks";
import { useCanvasTrigger } from "src/turingvisualizer/tools";
import { useSelectorRef } from "src/App/tools";
import * as cyEvents from "src/turingvisualizer/events";
import ActionsToolbar from "src/turingvisualizer/components/ActionsToolbar";

const ViewerPageContent = () => {
  const vis = useVisualizerContext();
  const selectedNodesRef = useSelectorRef("selectedNodes");
  const selectedNodes = useSelector(state => state.selectedNodes);
  const dbName = useSelector((state) => state.dbName);
  const dispatch = useDispatch();

  React.useEffect(
    () =>
      vis.callbacks().setSelectedNodeIds(Object.keys(selectedNodes)),
    [vis, selectedNodes]
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
      const nodeIds = vis.state().selectedNodeIds;
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
          thunks.getNodes(dbName, unknownNodeIds, { yield_edges: true })
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
      <Visualizer
        canvas={<Canvas />}
        contextMenu={<TuringContextMenu />}
        cyStyle={style}
        onNodeAdd={(n) => {
          dispatch(thunks.getNodes(dbName, [n.id], { yield_edges: true })).then(
            (res) => dispatch(actions.selectNode(Object.values(res)[0]))
          );
        }}
        onNodeRemove={(n) => {
          dispatch(actions.unselectNode(n))
        }}
      >
        <div
          style={{
            margin: 10,
            marginLeft: 20,
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
          }}>
          <ActionsToolbar
            settingsAction
            selectAction
            fitAction
            cleanAction
            hiddenNodesAction
            cellCellInteraction
            expandAction
            collapseAction
            searchAction
          />
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
