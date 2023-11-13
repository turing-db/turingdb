import React from "react";
import * as utils from "@VisualizerOld/utils";

export const useItems = () => {
  const previousElements = React.useRef([]);
  const { data: rawElements } = utils.useCytoscapeElements();
  const elements = rawElements || previousElements.current;

  previousElements.current = elements;

  const items = React.useMemo(
    () => ({
      ...Object.fromEntries(
        elements
          .filter((e) => e.group === "nodes")
          .map((n) => [
            "node" + n.id,
            {
              data: {
                ...n.data,
                ...n.data.properties,
              },
              ...n.data,
              color:
                n.data.type === "selected"
                  ? "rgb(204,204,204)"
                  : "rgb(155,155,155)",
              size: 1,
              label: {
                text: n.data.label,
                backgroundColor: "rgba(0,0,0,0)",
                color:
                  n.data.type === "selected"
                    ? "rgb(204,204,204)"
                    : "rgb(155,155,155)",
              },
            },
          ])
      ),

      ...Object.fromEntries(
        elements
          .filter((e) => e.group === "edges")
          .map((e) => [
            "edge" + e.id,
            {
              data: e.data,
              ...e.data,
              id1: "node" + e.data.source,
              id2: "node" + e.data.target,
              color:
                e.data.type === "connecting" ? "rgb(0,230,0)" : "rgb(0,130,0)",
              end1: { arrow: true },
              width: 6,
              maxLength: 10,
              label: {
                text: e.data.label,
                backgroundColor: "rgba(0,0,0,0)",
                color:
                  e.data.type === "connecting"
                    ? "rgb(0,230,0)"
                    : "rgb(0,130,0)",
              },
            },
          ])
      ),
    }),
    [elements]
  );

  return items;
};
