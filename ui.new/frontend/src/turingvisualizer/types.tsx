import { Filters } from "./getRawFilters";

export type CxtMenuData = {
  group: string;
  data: cytoscape.Collection;
};

export type LayoutDefinition = {
  name: string;
  positions: [];
};

type GraphNodeProperties = {
  [propName: string]: string;
};

export type GraphNodeData = {
  id: string;
  turing_id: string;
  node_type_name?: string;
  neighborNodeIds?: string[];
  properties: GraphNodeProperties;
};

export type VisualizerState = {
  dbName: string;
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
  hiddenNodes: {
    [id: string]: GraphNodeData;
  };
  selectedNodeIds: string[];
  filters: Filters;
  nodeLabel: string;
  edgeLabel: string;
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
