import { COLOR_MODES, DISCRETE_COLORS } from "./constants";
import { ColorSet, GraphEdgeData, GraphNodeData, GraphElement, GraphNode, GraphEdge } from "./types";

const colorGradient = (min: number, max: number, value: number) => {
  const extent = max - min;
  const r = (255 * (value - min)) / extent;
  const g = 255 - (255 * (value - min)) / extent;
  return `rgb(${r},${g},0)`;
};

const getColorMakers = (elementTypes: string[]) => ({
  [COLOR_MODES.None]: () => undefined,
  [COLOR_MODES.NodeType]: (el: GraphElement) => {
    const n = el as GraphNode
    const colorIndex =
      elementTypes.indexOf(n.data.node_type_name!) % DISCRETE_COLORS.length;
    const color = DISCRETE_COLORS[colorIndex];
    return color;
  },
  [COLOR_MODES.EdgeType]: (el: GraphElement) => {
    const e = el as GraphEdge;
    const colorIndex =
      elementTypes.indexOf(e.data.edge_type_name!) % DISCRETE_COLORS.length;
    const color = DISCRETE_COLORS[colorIndex];
    return color;
  },
  [COLOR_MODES.GradientProperty]: (el: GraphElement, colorSet: ColorSet) => {
    const data = colorSet.data;
    return colorGradient(data.min!, data.max!, parseInt(data.propValues![el.data.id]));
  },
  [COLOR_MODES.DiscreteProperty]: (el: GraphElement, colorSet: ColorSet) => {
    const colorIndex =
      colorSet.data.propValues!.indexOf(
        el.data.properties[colorSet.data.propName!]!
      ) % DISCRETE_COLORS.length;
    const color = DISCRETE_COLORS[colorIndex];
    return color;
  },
  [COLOR_MODES.Preset]: (_el: GraphElement, colorSet: ColorSet) => {
    return colorSet.data.color!;
  },
});

export const useElementColorMakers = (elements: GraphElement[]) => {
  const edges = elements.filter((el) => el.group === "edges").map((el) => el as GraphEdge);
  const nodes = elements.filter((el) => el.group === "nodes").map((el) => el as GraphNode);

  const edgeTypes = [...new Set(edges.map((e) => e.data.edge_type_name!))];
  const nodeTypes = [...new Set(nodes.map((e) => e.data.node_type_name!))];

  const nodeColorMakers = getColorMakers(nodeTypes);
  const edgeColorMakers = getColorMakers(edgeTypes);

  return { nodeColorMakers, edgeColorMakers };
};
