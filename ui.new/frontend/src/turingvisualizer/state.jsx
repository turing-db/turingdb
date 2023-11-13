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
import { useCytoscapeElements } from "./tools";
import { VisualizerContext } from ".";

const useVisualizerState = (cyStyle) => {
  const vis = React.useContext(VisualizerContext);
  const { nodeLabel, setNodeLabel } = useNodeLabel();
  const { edgeLabel, setEdgeLabel } = useEdgeLabel();
  const { nodeColors, setNodeColorMode } = useNodeColors();
  const { edgeColors, setEdgeColorMode } = useEdgeColors();
  const { selectedNodeIds, setSelectedNodeIds } = useSelectedNodeIds();
  const {
    layouts,
    setDefaultCyLayout,
    addLayout,
    updateLayout,
    resetLayout,
    requestLayoutRun,
  } = useLayouts();
  const { hiddenNodes, hideNode, hideNodes, showNodes } = useHiddenNodes();
  const { filters, setFilters } = useFilters();

  vis.refs.state.current = {
    dbName: vis.dbName,
    themeMode: vis.themeMode,
    canvasTheme: vis.canvasTheme,
    selectedNodeIds,
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

  // Core
  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.elements.core).forEach((f) => f());
  }, [vis.refs.triggers, elements]);

  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.filters.core).forEach((f) => f());
  }, [vis.refs.triggers, filters]);

  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.layouts.core).forEach((f) => f());
  }, [vis.refs.triggers, layouts]);

  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.nodeLabel.core).forEach((f) => f());
  }, [vis.refs.triggers, nodeLabel]);

  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.edgeLabel.core).forEach((f) => f());
  }, [vis.refs.triggers, edgeLabel]);

  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.hiddenNodes.core).forEach((f) =>
      f()
    );
  }, [vis.refs.triggers, hiddenNodes]);

  // Secondary
  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.elements.secondary).forEach((f) =>
      f()
    );
  }, [vis.refs.triggers, elements]);

  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.filters.secondary).forEach((f) =>
      f()
    );
  }, [vis.refs.triggers, filters]);

  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.layouts.secondary).forEach((f) =>
      f()
    );
  }, [vis.refs.triggers, layouts]);

  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.nodeLabel.secondary).forEach((f) =>
      f()
    );
  }, [vis.refs.triggers, nodeLabel]);

  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.edgeLabel.secondary).forEach((f) =>
      f()
    );
  }, [vis.refs.triggers, edgeLabel]);

  React.useEffect(() => {
    Object.values(vis.refs.triggers.current.hiddenNodes.secondary).forEach(
      (f) => f()
    );
  }, [vis.refs.triggers, hiddenNodes]);

  vis.refs.callbacks.current = {
    setSelectedNodeIds: (ids) => setSelectedNodeIds(ids),
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
    setNodeLabel,
    setEdgeLabel,
    setFilters,
    setNodeColorMode,
    setEdgeColorMode,
  };
};

export default useVisualizerState;
