// Core
import React from "react";
import { useTheme } from "@emotion/react";
import { useDispatch, useSelector } from "react-redux";

// Turing
import {
  Canvas,
  Visualizer,
  VisualizerContextProvider,
  TuringContextMenu,
} from "src/turingvisualizer/components";

import { useVisualizerContext } from "src/turingvisualizer/context";
import style from "src/turingvisualizer/style";
import * as actions from "src/App/actions";
import * as thunks from "src/App/thunks";
import { useCanvasTrigger } from "src/turingvisualizer/useCanvasTrigger";
import { useSelectorRef } from "src/App/tools";
import ActionsToolbar from "src/turingvisualizer/components/ActionsToolbar";
import DialogContainer from "src/turingvisualizer/components/DialogContainer";
import { PortalProvider } from "@blueprintjs/core";

const ViewerPageContent = () => {
  const vis = useVisualizerContext();
  const selectedNodesRef = useSelectorRef("selectedNodes");
  const selectedNodes = useSelector((state) => state.selectedNodes);
  const dbName = useSelector((state) => state.dbName);
  const theme = useTheme();
  const dispatch = useDispatch();

  React.useEffect(
    () => vis.callbacks().setSelectedNodeIds(Object.keys(selectedNodes)),
    [vis, selectedNodes]
  );

  useCanvasTrigger({
    category: "inspectedNode",
    name: "setInspectedNode",

    callback: () => {
      if (!vis.state().inspectedNode) return;
      dispatch(thunks.inspectNode(dbName, vis.state().inspectedNode.turing_id));
    },
  });

  useCanvasTrigger({
    category: "selectedNodeIds",
    name: "setSelectedNodeIds",

    callback: () => {
      const nodeIds = vis.state().selectedNodeIds;
      const currentIds = Object.keys(selectedNodesRef.current);

      if (nodeIds.length !== currentIds.length) {
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

  const bpTheme = theme.palette.mode === "dark" ? "bp5-dark" : "";
  return (
    <div className="flex flex-1 flex-col">
      <PortalProvider portalClassName={bpTheme}>
        <Visualizer
          canvas={<Canvas />}
          contextMenu={<TuringContextMenu />}
          cyStyle={style}
          onNodeAdd={(n) => {
            dispatch(
              thunks.getNodes(dbName, [n.id], { yield_edges: true })
            ).then((res) =>
              dispatch(actions.selectNode(Object.values(res)[0]))
            );
          }}
          onNodeRemove={(n) => {
            dispatch(actions.unselectNode(n));
          }}
          onNodeInspect={(n) => {
            dispatch(thunks.inspectNode(dbName, n.id));
          }}>
          <div
            style={{
              margin: 10,
              marginLeft: 20,
              display: "flex",
              flexDirection: "row",
              alignItems: "center",
            }}>
            <DialogContainer/>
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
      </PortalProvider>
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
      containerId="cy-viewer">
      <ViewerPageContent />
    </VisualizerContextProvider>
  );
};

export default ViewerPage;
