// Core
import React from "react";

// Turing
import useFilters from "./reducers/filters";
import useSelectedNodeIds from "./reducers/selectedNodeIds";
import useNodeLabel from "./reducers/nodeLabel";
import useEdgeLabel from "./reducers/edgeLabel";
import useNodeColors from "./reducers/nodeColors";
import useEdgeColors from "./reducers/edgeColors";
import useLayouts from "./reducers/layouts";
import useHiddenNodes from "./reducers/hiddenNodes";
import useInspectedNode from "./reducers/inspectedNode";
import { useCytoscapeElements } from "./cytoscape/tools";
import { VisualizerContext } from "./components/VisualizerContext";

const useVisualizerState = (cyStyle) => {
  const vis = React.useContext(VisualizerContext);
  const { nodeLabel, setNodeLabel } = useNodeLabel();
  const { edgeLabel, setEdgeLabel } = useEdgeLabel();
  const { nodeColors, setNodeColorMode } = useNodeColors();
  const { edgeColors, setEdgeColorMode } = useEdgeColors();
  const { selectedNodeIds, setSelectedNodeIds } = useSelectedNodeIds();
  const { inspectedNode, setInspectedNode } = useInspectedNode();
  const {
    layouts,
    setDefaultCyLayout,
    addLayout,
    updateLayout,
    resetLayout,
    requestLayoutRun,
    requestLayoutFit,
    centerOnDoubleClicked,
  } = useLayouts();
  const { hiddenNodes, hideNode, hideNodes, showNodes } = useHiddenNodes();
  const { filters, setFilters } = useFilters();

  vis.refs.state.current = {
    dbName: vis.dbName,
    themeMode: vis.themeMode,
    canvasTheme: vis.canvasTheme,
    selectedNodeIds,
    inspectedNode,
    hiddenNodes,
    nodeLabel,
    edgeLabel,
    nodeColors,
    edgeColors,
    layouts,
    filters,
    devMode: vis.devMode,
    cytoscapeProps: {
      style: cyStyle,
    },
  };

  const elements = useCytoscapeElements(vis.refs.state.current);

  vis.refs.state.current.elements = elements;
  const trig = vis.triggers;

  // Core
  React.useEffect(() => {
    Object.values(trig().elements.core).forEach((f) => f());
  }, [trig, elements]);

  React.useEffect(() => {
    Object.values(trig().selectedNodeIds.core).forEach((f) => f());
  }, [trig, selectedNodeIds]);

  React.useEffect(() => {
    Object.values(trig().inspectedNode.core).forEach((f) => f());
  }, [trig, inspectedNode]);

  React.useEffect(() => {
    Object.values(trig().filters.core).forEach((f) => f());
  }, [trig, filters]);

  React.useEffect(() => {
    Object.values(trig().layouts.core).forEach((f) => f());
  }, [trig, layouts]);

  React.useEffect(() => {
    Object.values(trig().nodeLabel.core).forEach((f) => f());
  }, [trig, nodeLabel]);

  React.useEffect(() => {
    Object.values(trig().edgeLabel.core).forEach((f) => f());
  }, [trig, edgeLabel]);

  React.useEffect(() => {
    Object.values(trig().hiddenNodes.core).forEach((f) => f());
  }, [trig, hiddenNodes]);

  // Secondary
  React.useEffect(() => {
    Object.values(trig().elements.secondary).forEach((f) => f());
  }, [trig, elements]);

  React.useEffect(() => {
    Object.values(trig().selectedNodeIds.secondary).forEach((f) => f());
  }, [trig, selectedNodeIds]);

  React.useEffect(() => {
    Object.values(trig().inspectedNode.secondary).forEach((f) => f());
  }, [trig, inspectedNode]);

  React.useEffect(() => {
    Object.values(trig().filters.secondary).forEach((f) => f());
  }, [trig, filters]);

  React.useEffect(() => {
    Object.values(trig().layouts.secondary).forEach((f) => f());
  }, [trig, layouts]);

  React.useEffect(() => {
    Object.values(trig().nodeLabel.secondary).forEach((f) => f());
  }, [trig, nodeLabel]);

  React.useEffect(() => {
    Object.values(trig().edgeLabel.secondary).forEach((f) => f());
  }, [trig, edgeLabel]);

  React.useEffect(() => {
    Object.values(trig().hiddenNodes.secondary).forEach((f) => f());
  }, [trig, hiddenNodes]);

  vis.refs.callbacks.current = {
    setSelectedNodeIds: (ids) => setSelectedNodeIds(ids),
    setInspectedNode: (inspectedNode) => setInspectedNode(inspectedNode),
    setDefaultCyLayout: (cyLayout) => setDefaultCyLayout(cyLayout),
    addLayout: (definition, nodeIds) => addLayout(definition, nodeIds),
    updateLayout: (layoutId, patch) => updateLayout(layoutId, patch),
    resetLayout: (nodeIds) => resetLayout(nodeIds),
    setNodeColorMode: (mode, elementIds, data) =>
      setNodeColorMode(mode, elementIds, data),
    setEdgeColorMode: (mode, elementIds, data) =>
      setEdgeColorMode(mode, elementIds, data),
    setNodeLabel: (label) => setNodeLabel(label),
    setEdgeLabel: (label) => setEdgeLabel(label),
    hideNode: (node) => hideNode(node),
    hideNodes: (nodes) => hideNodes(nodes),
    showNodes: (nodeIds) => showNodes(nodeIds),
    requestLayoutRun: (request) => requestLayoutRun(request),
    requestLayoutFit: (request) => requestLayoutFit(request),
    centerOnDoubleClicked: (value) => centerOnDoubleClicked(value),
    setFilters: (filters) => setFilters(filters),
  };

  return {
    // Canvas State
    dbName: vis.dbName,
    selectedNodeIds,
    elements,
    hiddenNodes,
    nodeLabel,
    edgeLabel,
    filters,
    nodeColors,
    edgeColors,
    layouts,

    // Cytoscape props
    cytoscapeProps: {
      style: cyStyle,
    },

    // Callbacks
    setSelectedNodeIds,
    setInspectedNode,
    setNodeLabel,
    setEdgeLabel,
    setFilters,
    setNodeColorMode,
    setEdgeColorMode,
  };
};

export default useVisualizerState;
