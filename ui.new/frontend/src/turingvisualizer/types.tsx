import { CanvasThemes } from "./cytoscape/tools";
import { Filters } from "./getRawFilters";

export type Property = [string, string];

export type ColorSet = {
  mode: string;
  data: {
    min?: number;
    max?: number;
    propValues?: string[];
    propName?: string;
    color?: string;
  };
};

export type CxtMenuData = {
  group: string;
  data: cytoscape.Collection;
};

export type LayoutDefinition = {
  name: string;
  positions: [];
  lockBehaviour?: number,
  edgeLengthVal?: number,
  nodeSpacing?: number,
};

export type ElementProperties = {
  [pName: string]: string;
};

export type GraphNodeData = {
  id: string;
  turing_id: string;
  type: "selected" | "neighbor";
  node_type_name: string;
  properties: ElementProperties[];
  neighborNodeIds?: string[];
};

export type GraphEdgeData = {
  id: string;
  turing_id: string;
  type: "connecting" | "neighbor";
  turing_source_id: string;
  turing_target_id: string;
  edge_type_name: string;
  properties: ElementProperties;
};

export type GraphNode = {
  group: "nodes";
  data: GraphNodeData;
};

export type GraphEdge = {
  group: "edges";
  data: GraphEdgeData;
};

export type GraphElement = GraphNode | GraphEdge;

export type NodeColors = {
  colorSets: {
    [id: number]: ColorSet;
  };
  mapping: {
    // 'nodeId: setId' entries
    [edgeId: string]: number;
  };
  setCount: number;
};

export type EdgeColors = {
  colorSets: {
    [id: number]: ColorSet;
  };
  mapping: {
    // 'nodeId: setId' entries
    [nodeId: string]: number;
  };
  setCount: number;
};

export type HiddenNodes = {
  [id: string]: GraphNodeData;
};

export type VisualizerState = {
  dbName: string;
  themeMode?: "light" | "dark";
  canvasTheme?: CanvasThemes;
  devMode?: boolean,
  inspectedNode: GraphNodeData;
  layouts: {
    definitions: {
      [id: number]: LayoutDefinition;
    };
    mapping: {};
    layoutCount: number;
    runRequested: boolean;
    fitRequested: boolean;
    centerOnDoubleClicked: boolean;
  };
  hiddenNodes: HiddenNodes;
  selectedNodeIds: string[];
  filters: Filters;
  nodeLabel: string;
  edgeLabel: string;
  nodeColors: NodeColors;
  edgeColors: EdgeColors;
};

export type CanvasEvent = (_vis: Visualizer, _e: cytoscape.EventObject) => void;

export type HookEvent = () => void;

export type VisualizerEventHooks = {
  select: { [key: string]: HookEvent };
};

export type VisualizerCallbacks = {
  setSelectedNodeIds: (ids: string[]) => void;
  setInspectedNode: (inspectedNode: GraphNodeData) => void;
  setDefaultCyLayout: (cyLayout: LayoutDefinition) => void;
  addLayout: (LayoutDefinition: LayoutDefinition, nodeIds: string[]) => void;
  updateLayout: (layoutId: string, patch: Partial<LayoutDefinition>) => void;
  resetLayout: (nodeIds: string[]) => void;
  setNodeColorMode: (mode: number, elementIds: string[], data: any) => void;
  setEdgeColorMode: (mode: number, elementIds: string[], data: any) => void;
  setNodeLabel: (label: string) => void;
  setEdgeLabel: (label: string) => void;
  hideNode: (node: GraphNodeData) => void;
  hideNodes: (nodes: GraphNodeData[]) => void;
  showNodes: (nodeIds: string[]) => void;
  requestLayoutRun: (request: boolean) => void;
  requestLayoutFit: (request: boolean) => void;
  centerOnDoubleClicked: (value: boolean) => void;
  setFilters: (filters: Filters) => void;
};

export type VisualizerEvents = { [key: string]: CanvasEvent };
export type VisualizerTrigger = { core: {}; secondary: {} };
export type VisualizerTriggers = {
  elements: VisualizerTrigger;
  selectedNodeIds: VisualizerTrigger;
  inspectedNode: VisualizerTrigger;
  filters: VisualizerTrigger;
  layouts: VisualizerTrigger;
  nodeLabel: VisualizerTrigger;
  edgeLabel: VisualizerTrigger;
  hiddenNodes: VisualizerTrigger;
};

export type HookEventFunction = (
  eventName: string,
  key: string,
  callback: () => void
) => void;

export type Visualizer = {
  cy: () => cytoscape.Core;
  state: () => VisualizerState;
  callbacks: () => VisualizerCallbacks;
  eventHooks: () => VisualizerEventHooks;
  contextMenu: () => {
    dispatchEvent: (e: Event) => void;
  };
  contextMenuSetData: (d: CxtMenuData) => void;
};
