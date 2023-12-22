// Core
import React from "react";
import cytoscape from "cytoscape";

//Turing
import { useDevElements, useElementsQuery } from "../queries";
import { useElementColorMakers } from "../colors";
import { useVisualizerContext } from "../";
import { GraphNode, GraphEdge, GraphElement, VisualizerState } from "../types";

export const getFitViewport = (
  cy: cytoscape.Core,
  elements: cytoscape.Collection,
  padding = 0
) => {
  const bb = elements.boundingBox();

  let w = cy.width();
  let h = cy.height();
  let zoom: number;

  zoom = Math.min((w - 2 * padding) / bb.w, (h - 2 * padding) / bb.h);

  // crop zoom
  zoom = zoom > cy.maxZoom() ? cy.maxZoom() : zoom;
  zoom = zoom < cy.minZoom() ? cy.minZoom() : zoom;

  let pan = {
    // now pan to middle
    x: (w - zoom * (bb.x1 + bb.x2)) / 2,
    y: (h - zoom * (bb.y1 + bb.y2)) / 2,
  };

  return {
    zoom: zoom,
    pan: pan,
  };
};

export const fit = (
  cy: cytoscape.Core,
  elements: cytoscape.Collection,
  padding = 0
) => {
  const viewport = getFitViewport(cy, elements, padding);
  viewport.zoom *= 0.9;
  cy.viewport(viewport);
};

const useGetElementLabel = (state: VisualizerState) => {
  const getElementLabel = React.useMemo(
    () => ({
      nodes: (el: GraphNode) => {
        if (state.nodeLabel === "None") return "";
        if (state.nodeLabel === "Node Type") return el.data.node_type_name;
        const label: string =
          el.data.properties[state.nodeLabel] ||
          el.data.properties["displayName"] ||
          el.data.properties["label"] ||
          "";

        return label;
      },
      edges: (el: GraphEdge) => {
        if (state.edgeLabel === "None") return "";
        if (state.edgeLabel === "Edge Type")
          return el.data.edge_type_name + "\n\u2060";
        return el.data.properties[state.edgeLabel] + "\n\u2060" || "";
      },
    }),
    [state.nodeLabel, state.edgeLabel]
  );

  return getElementLabel;
};

export type CanvasTheme = {
  nodes: {
    selected: {
      icon: string;
      text: string;
    };
    neighbor: {
      icon: string;
      text: string;
    };
  };
  edges: {
    connecting: {
      line: string;
      text: string;
    };
    neighbor: {
      line: string;
      text: string;
    };
  };
};

export function getDefaultCanvasTheme(): {
  light: CanvasTheme;
  dark: CanvasTheme;
} {
  return {
    light: {
      // Light
      nodes: {
        selected: {
          icon: "#3f51b5",
          text: "#3f51b5",
        },
        neighbor: {
          icon: "#64b5f6",
          text: "#64b5f6",
        },
      },
      edges: {
        connecting: {
          line: "#c51162",
          text: "#c51162",
        },
        neighbor: {
          line: "#c51162",
          text: "#c51162",
        },
      },
    },
    dark: {
      // Dark
      nodes: {
        selected: {
          icon: "rgb(204,204,204)",
          text: "rgb(204,204,204)",
        },
        neighbor: {
          icon: "rgb(155,155,155)",
          text: "rgb(155,155,155)",
        },
      },
      edges: {
        connecting: {
          line: "rgb(0,230,0)",
          text: "rgb(0,230,0)",
        },
        neighbor: {
          line: "rgb(0,130,0)",
          text: "rgb(0,130,0)",
        },
      },
    },
  };
}

export type CanvasThemes = ReturnType<typeof getDefaultCanvasTheme>;

const useGetElementColors = (
  state: VisualizerState,
  rawElements: GraphElement[],
  theme: CanvasTheme
) => {
  const { nodeColorMakers, edgeColorMakers } =
    useElementColorMakers(rawElements);

  const getElementColors = React.useCallback(
    (el: GraphElement) => {
      if (el.group === "nodes") {
        const n = el as GraphNode;
        const colorSetId = state.nodeColors.mapping[n.data.id] || 0;
        const colorSet = state.nodeColors.colorSets[colorSetId]!;
        const colorMaker = nodeColorMakers[colorSet.mode]!;

        if (n.data.type === "selected") {
          return {
            iconColor: colorMaker(n, colorSet) || theme.nodes.selected.icon,
            textColor: colorMaker(n, colorSet) || theme.nodes.selected.text,
          };
        } else {
          // neighbor
          return {
            iconColor: colorMaker(n, colorSet) || theme.nodes.neighbor.icon,
            textColor: colorMaker(n, colorSet) || theme.nodes.neighbor.text,
          };
        }
      } else {
        // edges
        const e = el as GraphEdge;
        const colorSetId = state.edgeColors.mapping[e.data.id] || 0;
        const colorSet = state.edgeColors.colorSets[colorSetId]!;
        const colorMaker = edgeColorMakers[colorSet.mode]!;

        if (el.data.type === "connecting")
          return {
            lineColor: colorMaker(e, colorSet) || theme.edges.connecting.line,
            textColor: colorMaker(e, colorSet) || theme.edges.connecting.text,
          };
        // neighbor
        else
          return {
            lineColor: colorMaker(e, colorSet) || theme.edges.neighbor.line,
            textColor: colorMaker(e, colorSet) || theme.edges.neighbor.text,
          };
      }
    },
    [nodeColorMakers, edgeColorMakers, state, theme]
  );

  return getElementColors;
};

export const useCytoscapeElements = (
  state: VisualizerState
): GraphElement[] => {
  const themeMode = state.themeMode || "dark";
  const themeDef = state.canvasTheme || getDefaultCanvasTheme();
  const theme = themeDef[themeMode];
  const previousElements = React.useRef<GraphElement[]>([]);
  const vis = useVisualizerContext();

  const { data: rawElements } = state.devMode
    ? useDevElements({
        selectedNodeIds: state.selectedNodeIds,
        filters: state.filters,
        hiddenNodeIds: Object.keys(state.hiddenNodes),
      })
    : useElementsQuery({
        selectedNodeIds: state.selectedNodeIds,
        dbName: state.dbName,
        filters: state.filters,
        hiddenNodeIds: Object.keys(state.hiddenNodes),
      });

  const elements = React.useMemo(() => {
    return rawElements || previousElements.current;
  }, [rawElements]);

  const getElementLabel = useGetElementLabel(state);
  const getElementColors = useGetElementColors(state, elements, theme);

  const data = React.useMemo(() => {
    const nodesHaveHiddenNodes = Object.fromEntries(
      [...Object.values(state.hiddenNodes).map((n) => n.neighborNodeIds)]
        .flat()
        .map((id) => [id, true])
    );

    const baseElements = elements.map((el) => {
      if (el.group === "nodes") {
        const n = el as GraphNode;
        return {
          ...el,
          data: {
            ...el.data,
            label: getElementLabel["nodes"](n),
            ...getElementColors(n),
            has_hidden_nodes: nodesHaveHiddenNodes[el.data.turing_id] || false,
          },
        };
      }
      const e = el as GraphEdge;
      return {
        ...el,
        data: {
          ...el.data,
          label: getElementLabel["edges"](e),
          ...getElementColors(e),
        },
      };
    });

    return baseElements;
  }, [vis, elements, getElementColors, getElementLabel]);

  previousElements.current = data;

  return data;
};

export function getFragment(cy: cytoscape.Core, nodeId: string) {
  let fragment = cy.$id(nodeId).closedNeighborhood();
  let previousSize = fragment.length;

  do {
    previousSize = fragment.length;
    fragment = fragment.closedNeighborhood();
  } while (previousSize !== fragment.length);

  return fragment;
}
