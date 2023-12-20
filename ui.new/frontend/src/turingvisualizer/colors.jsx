import { COLOR_MODES, DISCRETE_COLORS } from "./constants";

const colorGradient = (min, max, value) => {
  if (!value) return undefined;

  const extent = max - min;
  const r = parseInt((255 * (value - min)) / extent);
  const g = 255 - parseInt((255 * (value - min)) / extent);
  return `rgb(${r},${g},0)`;
};

const getColorMakers = (elementTypes) => ({
  [COLOR_MODES.None]: () => undefined,
  [COLOR_MODES.ElementType]: (elData) => {
    const colorIndex =
      elementTypes.indexOf(elData.node_type_name) % DISCRETE_COLORS.length;
    const color = DISCRETE_COLORS[colorIndex];
    return color;
  },
  [COLOR_MODES.NodeType]: (elData) => {
    const colorIndex =
      elementTypes.indexOf(elData.node_type_name) % DISCRETE_COLORS.length;
    const color = DISCRETE_COLORS[colorIndex];
    return color;
  },
  [COLOR_MODES.EdgeType]: (elData) => {
    const colorIndex =
      elementTypes.indexOf(elData.edge_type_name) % DISCRETE_COLORS.length;
    const color = DISCRETE_COLORS[colorIndex];
    return color;
  },
  [COLOR_MODES.GradientProperty]: (elData, colorSet) => {
    const data = colorSet.data;
    return colorGradient(data.min, data.max, data.propValues[elData.id]);
  },
  [COLOR_MODES.DiscreteProperty]: (elData, colorSet) => {
    const colorIndex =
      colorSet.data.propValues.indexOf(
        elData.properties[colorSet.data.propName]
      ) % DISCRETE_COLORS.length;
    const color = DISCRETE_COLORS[colorIndex];
    return color;
  },
  [COLOR_MODES.Preset]: (_elData, colorSet) => {
    return colorSet.data.color;
  },
});

export const useElementColorMakers = (elements) => {
  const edges = elements.filter((el) => el.group === "edges");
  const nodes = elements.filter((el) => el.group === "nodes");

  const edgeTypes = [...new Set(edges.map((e) => e.data.edge_type_name))];
  const nodeTypes = [...new Set(nodes.map((e) => e.data.node_type_name))];

  const nodeColorMakers = getColorMakers(nodeTypes);
  const edgeColorMakers = getColorMakers(edgeTypes);

  return { nodeColorMakers, edgeColorMakers };
};
