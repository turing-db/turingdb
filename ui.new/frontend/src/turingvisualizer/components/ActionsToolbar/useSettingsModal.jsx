import React from "react";
import { Tooltip, Checkbox, Slider, FormGroup } from "@blueprintjs/core";
import LockMenu from "./LockMenu";

import { useVisualizerContext } from "../../";
import { useDialog } from "src/turingvisualizer/dialogs";

const LENGTH_RATIO = 0.1;

const useValue = ({ getCanvasValue, setCanvasValue }) => {
  const [released, setReleased] = React.useState(false);
  const [v, setV] = React.useState(getCanvasValue());

  React.useEffect(() => {
    if (!released) return;

    if (released) setReleased(false);

    if (v !== getCanvasValue()) {
      setCanvasValue(v);
    }
  }, [getCanvasValue, setCanvasValue, released, v]);

  return {
    value: v,
    onRelease: () => {
      setReleased(true);
    },
    onChange: (v) => {
      setV(v);
    },
  };
};

const useSettingsModal = () => {
  const vis = useVisualizerContext();
  const [filters, setFilters] = React.useState(vis.state().filters);

  React.useEffect(() => {
    Object.keys(filters).forEach((key) => {
      if (filters[key] !== vis.state().filters[key]) {
        vis.callbacks().setFilters({
          ...vis.state().filters,
          ...filters,
        });
        return;
      }
    });
  }, [vis, filters]);

  const centerOnDoubleClicked = useValue({
    getCanvasValue: () => vis.state().layouts.centerOnDoubleClicked,
    setCanvasValue: vis.callbacks().centerOnDoubleClicked,
  });

  const edgeLengthVal = useValue({
    getCanvasValue: () => vis.state().layouts.definitions[0].edgeLengthVal,
    setCanvasValue: (v) => {
      vis.callbacks().setDefaultCyLayout({
        ...vis.state().layouts.definitions[0],
        edgeLengthVal: v,
      });
    },
  });

  const nodeSpacing = useValue({
    getCanvasValue: () => vis.state().layouts.nodeSpacing,
    setCanvasValue: (v) => {
      vis.callbacks().setDefaultCyLayout({
        ...vis.state().layouts.definitions[0],
        nodeSpacing: v,
      });
    },
  });

  useDialog({
    name: "show-settings",
    title: "Graph settings",
    content: () => {
      return (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            padding: 20,
          }}>
          <FormGroup label="Node spacing">
            <Slider
              min={1}
              labelValues={[1, 50, 100]}
              max={100}
              initialValue={nodeSpacing.value}
              value={nodeSpacing.value}
              onRelease={nodeSpacing.onRelease}
              onChange={nodeSpacing.onChange}
            />
          </FormGroup>
          <FormGroup label="Edge length">
            <Slider
              min={1}
              labelValues={[1, 50, 100]}
              max={100}
              initialValue={edgeLengthVal.value * LENGTH_RATIO}
              value={edgeLengthVal.value * LENGTH_RATIO}
              onRelease={edgeLengthVal.onRelease}
              onChange={(v) => edgeLengthVal.onChange(v / LENGTH_RATIO)}
            />
          </FormGroup>
          <div style={{ display: "flex", flexDirection: "column" }}>
            <Tooltip content={"Hide publications"} usePortal={false}>
              <Checkbox
                label={"Hide publications"}
                checked={filters.hidePublications}
                onChange={(e) => {
                  setFilters({
                    ...filters,
                    hidePublications: e.target.checked,
                  });
                }}
              />
            </Tooltip>
            <Tooltip
              content={
                'Hide compartments such as "extracellular region" which can significantly complicate the visualization'
              }
              usePortal={false}>
              <Checkbox
                label={"Hide compartments"}
                checked={filters.hideCompartments}
                onChange={(e) => {
                  setFilters({
                    ...filters,
                    hideCompartments: e.target.checked,
                  });
                }}
              />
            </Tooltip>
            <Tooltip
              content={
                'Hide species nodes such as "Homo sapiens" which can significantly complicate the visualization'
              }
              usePortal={false}>
              <Checkbox
                label={"Hide species"}
                checked={filters.hideSpecies}
                onChange={(e) => {
                  setFilters({
                    ...filters,
                    hideSpecies: e.target.checked,
                  });
                }}
              />
            </Tooltip>
            <Tooltip
              content={
                'Hide database references'
              }
              usePortal={false}>
              <Checkbox
                label={"Hide database references"}
                checked={filters.hideDatabaseReferences}
                onChange={(e) => {
                  setFilters({
                    ...filters,
                    hideDatabaseReferences: e.target.checked,
                  });
                }}
              />
            </Tooltip>
            <Tooltip
              content={
                'Show Homo sapiens only nodes'
              }
              usePortal={false}>
              <Checkbox
                label={"Homo sapiens only"}
                checked={filters.showOnlyHomoSapiens}
                onChange={(e) => {
                  setFilters({
                    ...filters,
                    showOnlyHomoSapiens: e.target.checked,
                  });
                }}
              />
            </Tooltip>
          </div>
          <LockMenu />
          <Tooltip
            content={"Double clicking a node centers the canvas onto it"}
            usePortal={false}>
            <Checkbox
              label="Center on double click"
              checked={centerOnDoubleClicked.value}
              onChange={(e) => {
                centerOnDoubleClicked.onChange(e.target.checked);
                centerOnDoubleClicked.onRelease();
              }}
            />
          </Tooltip>
        </div>
      );
    },
  });
};

export default useSettingsModal;
