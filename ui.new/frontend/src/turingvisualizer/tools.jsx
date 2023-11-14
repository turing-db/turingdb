// Core
import React from "react";

//Turing
import * as queries from "./queries";
import * as colors from "./colors";
import { useVisualizerContext } from ".";

const useGetElementLabel = (state) => {
  const getElementLabel = React.useMemo(
    () => ({
      nodes: (el) => {
        if (state.nodeLabel === "None") return "";
        if (state.nodeLabel === "Node Type") return el.data.node_type_name;
        return el.data.properties[state.nodeLabel] || "";
      },
      edges: (el) => {
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

export const getDefaultCanvasTheme = () => ({
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
});

const useGetElementColors = (state, rawElements, theme) => {
  const { nodeColorMakers, edgeColorMakers } =
    colors.useElementColorMakers(rawElements);

  const getElementColors = React.useCallback(
    (el) => {
      if (el.group === "nodes") {
        const colorSetId = state.nodeColors.mapping[el.data.id] || 0;
        const colorSet = state.nodeColors.colorSets[colorSetId];
        const colorMaker = nodeColorMakers[colorSet.mode];

        if (el.data.type === "selected") {
          return {
            iconColor:
              colorMaker(el.data, colorSet) || theme.nodes.selected.icon,
            textColor:
              colorMaker(el.data, colorSet) || theme.nodes.selected.text,
          };
        } else {
          // neighbor
          return {
            iconColor:
              colorMaker(el.data, colorSet) || theme.nodes.neighbor.icon,
            textColor:
              colorMaker(el.data, colorSet) || theme.nodes.neighbor.text,
          };
        }
      } else {
        // edges
        const colorSetId = state.edgeColors.mapping[el.data.id] || 0;
        const colorSet = state.edgeColors.colorSets[colorSetId];
        const colorMaker = edgeColorMakers[colorSet.mode];

        if (el.data.type === "connecting")
          return {
            lineColor:
              colorMaker(el.data, colorSet) || theme.edges.connecting.line,
            textColor:
              colorMaker(el.data, colorSet) || theme.edges.connecting.text,
          };
        // neighbor
        else
          return {
            lineColor:
              colorMaker(el.data, colorSet) || theme.edges.neighbor.line,
            textColor:
              colorMaker(el.data, colorSet) || theme.edges.neighbor.text,
          };
      }
    },
    [nodeColorMakers, edgeColorMakers, state, theme]
  );

  return getElementColors;
};

export const useCytoscapeElements = (props) => {
  const themeMode = props.themeMode || "dark";
  const themeDef = props.canvasTheme || getDefaultCanvasTheme();
  const theme = themeDef[themeMode];
  const previousElements = React.useRef([]);

  const { data: rawElements } = props.devMode
    ? queries.useDevElements({
        ...props,
        hiddenNodeIds: Object.keys(props.hiddenNodes),
      })
    : queries.useElementsQuery({
        ...props,
        hiddenNodeIds: Object.keys(props.hiddenNodes),
      });

  const elements = React.useMemo(
    () => rawElements || previousElements.current,
    [rawElements]
  );

  const getElementLabel = useGetElementLabel(props);
  const getElementColors = useGetElementColors(props, elements, theme);

  const data = React.useMemo(() => {
    const baseElements = elements.map((el) => ({
      ...el,
      data: {
        ...el.data,
        label: getElementLabel[el.group](el),
        ...getElementColors(el),
      },
    }));

    return baseElements;
  }, [elements, getElementColors, getElementLabel]);

  previousElements.current = data;

  return data;
};

export const useCanvasTrigger = ({
  category,
  name,
  callback = () => {},
  core = false,
}) => {
  const vis = useVisualizerContext();

  React.useEffect(() => {
    core
      ? (vis.triggers()[category].secondary[name] = () => callback())
      : (vis.triggers()[category].core[name] = () => callback());
  }, [callback, name, category, vis, core]);
};
