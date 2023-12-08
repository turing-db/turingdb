// Core
import React from "react";

// Turing
import { useVisualizerContext } from "../../";

const useHighlightedNodes = () => {
  const vis = useVisualizerContext();

  return React.useCallback(() => {
    const rawNodes = vis
      .cy()
      .nodes()
      .filter((e) => e.selected());
    const nodes = rawNodes.length !== 0 ? rawNodes : vis.cy().nodes();
    const selNodes = nodes.filter((e) => e.data().type === "selected");
    const neiNodes = nodes.filter((e) => e.data().type === "neighbor");
    const selNodeIds = Object.fromEntries(
      selNodes.map((n) => [n.data().turing_id, true])
    );
    const selNodeData = Object.fromEntries(
      selNodes.map((n) => [
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
    const neiNodeData = Object.fromEntries(
      neiNodes.map((n) => [
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
    const neiData = Object.fromEntries(
      selNodes
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

    return {
      nodes,
      selNodes,
      selNodeIds,
      selNodeData,
      neiNodes,
      neiNodeData,
      neiData,
    };
  }, [vis]);
};

export const useMenuActions = () => {
  const vis = useVisualizerContext();
  const getHighlightedData = useHighlightedNodes();

  const hideNodes = React.useCallback(() => {
    const nodes = getHighlightedData();

    vis
      .callbacks()
      .setSelectedNodeIds(
        vis.state().selectedNodeIds.filter((id) => !nodes.selNodeIds[id])
      );
    vis.callbacks().hideNodes({ ...nodes.neiNodeData, ...nodes.selNodeData });
  }, [vis, getHighlightedData]);

  const collapseNeighbors = React.useCallback(() => {
    const nodes = getHighlightedData();

    vis.callbacks().hideNodes(nodes.neiData);
  }, [vis, getHighlightedData]);

  const expandNeighbors = React.useCallback(() => {
    const { nodes } = getHighlightedData();
    let newNodeIds = [];

    for (const node of nodes) {
      if (node.data().type === "neighbor") {
        newNodeIds.push(node.data().turing_id);
      }

      Object.entries(vis.state().hiddenNodes).forEach(([hiddenNodeId, n]) => {
        if (n.neighborNodeIds.includes(node.data().turing_id)) {
          vis.callbacks().showNodes([hiddenNodeId]);
        }
      });
    }

    vis
      .callbacks()
      .setSelectedNodeIds([...vis.state().selectedNodeIds, ...newNodeIds]);
  }, [vis, getHighlightedData]);

  const developNeighbors = React.useCallback(() => {
    const nodes = getHighlightedData();
    const neighbors = nodes.selNodes
      .neighborhood()
      .nodes()
      .filter((n) => n.data().type === "neighbor");

    vis
      .callbacks()
      .setSelectedNodeIds([
        ...vis.state().selectedNodeIds,
        ...neighbors.map((n) => n.data().turing_id),
      ]);
  }, [vis, getHighlightedData]);

  const selectAllBySameNodeType = React.useCallback(
    (nt) => {
      vis.cy().elements().unselect();
      vis
        .cy()
        .filter((e) => e.data().node_type_name === nt)
        .select();
      vis
        .cy()
        .filter((e) => e.data().node_type_name !== nt)
        .animate({
          style: {
            opacity: 0.5,
          },
          duration: 400,
          easing: "ease-in",
        });
    },
    [vis]
  );

  const selectAllBySameNodeProperty = React.useCallback(
    (propValue, propName) => {
      vis.cy().elements().unselect();
      vis
        .cy()
        .filter(
          (e) =>
            e.data().properties[propName] &&
            e.data().properties[propName] === propValue
        )
        .select();

      vis
        .cy()
        .filter(
          (e) =>
            !e.data().properties[propName] ||
            e.data().properties[propName] !== propValue
        )
        .animate({
          style: {
            opacity: 0.5,
          },
          duration: 400,
          easing: "ease-in",
        });
    },
    [vis]
  );

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
    const eles = vis.cy().filter((e) => e.selected());
    if (eles.length === 0) return;

    const newLayout = {
      name: "preset",
      fit: false,
      positions: {
        ...Object.fromEntries(
          eles.map((e, i) => [
            e.id(),
            { x: vis.state().layouts.layoutCount * 50.0, y: i * 40.0 },
          ])
        ),
      },
    };

    vis.callbacks().addLayout(
      newLayout,
      eles.map((e) => e.id())
    );
  }, [vis]);

  return {
    hideNodes,
    selectAllBySameNodeType,
    selectAllBySameNodeProperty,
    collapseNeighbors,
    expandNeighbors,
    developNeighbors,
    keepOnly,
    addToNetwork,
    setVerticalLine,
  };
};
