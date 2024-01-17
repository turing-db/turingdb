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
import { ttParams } from "src/turingvisualizer/tools";
import { Button, Icon, PortalProvider, Popover } from "@blueprintjs/core";

const KeyIcon = ({ keyIcon = undefined, keyText = undefined }) => {
  const props = {
    ...(keyIcon && { icon: keyIcon }),
    ...(keyText && { text: keyText }),
  };
  return <Button className="inline mx-1" {...props} />;
};

const ViewerPageContent = () => {
  const vis = useVisualizerContext();
  const selectedNodesRef = useSelectorRef("selectedNodes");
  const selectedNodes = useSelector((state) => state.selectedNodes);
  const dbName = useSelector((state) => state.dbName);
  const [slutterWarning, setSlutterWarning] = React.useState(false);
  const [elementCount, setElementCount] = React.useState(0);
  const theme = useTheme();
  const dispatch = useDispatch();

  React.useEffect(
    () => vis.callbacks().setSelectedNodeIds(Object.keys(selectedNodes)),
    [vis, selectedNodes]
  );

  useCanvasTrigger({
    category: "elements",
    name: "slutterWarning",

    callback: () => {
      if (vis.state().elements.length > 1000) {
        setSlutterWarning(true);
      } else {
        setSlutterWarning(false);
      }
      setElementCount(vis.state().elements.length);
    },
  });

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
    <div className="flex flex-1 flex-row">
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
            className="h-full flex flex-col p-4 items-start justify-between"
            style={{
              pointerEvents: "none",
            }}>
            <DialogContainer />
            <div className="w-full">
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
            <div className="w-full flex flex-col items-end">
              <div className="w-min pointer-events-auto">
                {slutterWarning && (
                  <Popover
                    {...ttParams}
                    className="warning"
                    interactionKind="hover"
                    placement="right-end"
                    content={
                      <div className="flex flex-col space-y-4 p-4 mr-4">
                        Many elements will be displayed at the same time, you
                        may experience flickering of the visualization
                      </div>
                    }>
                    <Icon
                      className="opacity-50 warning"
                      size={30}
                      icon="warning-sign"
                    />
                  </Popover>
                )}
                <Popover
                  {...ttParams}
                  className="gray1"
                  interactionKind="hover"
                  placement="right-end"
                  content={
                    <div className="flex flex-col space-y-4 p-4 mr-4">
                      <div className="flex flex-row space-x-2">
                        <p>
                          Press
                          <KeyIcon keyIcon="key-control" keyText="ctrl" />
                          <KeyIcon keyIcon="" keyText="A" />
                          to select all elements in the graph
                        </p>
                      </div>

                      <div className="flex flex-row space-x-2">
                        <p>
                          Press
                          <KeyIcon keyIcon="key-control" keyText="ctrl" />
                          <KeyIcon keyIcon="" keyText="F" />
                          to search for an element in the graph
                        </p>
                      </div>

                      <div className="flex flex-row space-x-2">
                        <p>
                          Press
                          <KeyIcon keyIcon="key-control" keyText="ctrl" />
                          while moving elements to move their unique neighbors
                          as well
                        </p>
                      </div>

                      <div className="flex flex-row space-x-2">
                        <p>
                          Press
                          <KeyIcon keyIcon="key-shift" keyText="shift" />
                          while moving elements to move all connected nodes as
                          well
                        </p>
                      </div>
                    </div>
                  }>
                  <Icon className="opacity-20" size={30} icon="help" />
                </Popover>
              </div>
            </div>
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
