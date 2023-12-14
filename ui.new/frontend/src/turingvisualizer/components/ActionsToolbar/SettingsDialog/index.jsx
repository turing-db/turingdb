import React from "react";
import {
  Button,
  Dialog,
  Tooltip,
  Checkbox,
  Slider,
  FormGroup,
} from "@blueprintjs/core";
import LockMenu from "./LockMenu";
import { useVisualizerContext } from "../../../";
import { useDialog, dialogParams, ttParams } from "../tools";
import { useWidgetValue } from "./hooks";

const LENGTH_RATIO = 0.1;

function useFilters() {
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

  return [filters, setFilters];
}

export default function SettingsDialog() {
  const vis = useVisualizerContext();
  const [filters, setFilters] = useFilters();
  const dialog = useDialog();

  const centerOnDoubleClicked = useWidgetValue({
    getCanvasValue: () => vis.state().layouts.centerOnDoubleClicked,
    setCanvasValue: vis.callbacks().centerOnDoubleClicked,
  });

  const edgeLengthVal = useWidgetValue({
    getCanvasValue: () => vis.state().layouts.definitions[0].edgeLengthVal,
    setCanvasValue: (v) => {
      vis.callbacks().setDefaultCyLayout({
        ...vis.state().layouts.definitions[0],
        edgeLengthVal: v,
      });
    },
  });

  const nodeSpacing = useWidgetValue({
    getCanvasValue: () => vis.state().layouts.definitions[0].nodeSpacing,
    setCanvasValue: (v) => {
      vis.callbacks().setDefaultCyLayout({
        ...vis.state().layouts.definitions[0],
        nodeSpacing: v,
      });
    },
  });

  return (
    <>
      <Tooltip {...ttParams} content="Canvas settings">
        <Button text="Settings" icon="settings" onClick={dialog.open} />
      </Tooltip>

      <Dialog
        {...dialogParams}
        isOpen={dialog.isOpen}
        onClose={dialog.close}
        title="Canvas settings">
        <div className="flex flex-col p-10 overflow-auto h-[60vh]">
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

          <div className="flex flex-col">
            <Tooltip content="Hide publications">
              <Checkbox
                label="Hide publications"
                checked={filters.hidePublications}
                onChange={(e) => {
                  setFilters({
                    ...filters,
                    hidePublications: e.target.checked,
                  });
                }}
              />
            </Tooltip>

            <Tooltip content='Hide compartments such as "extracellular region"'>
              <Checkbox
                label="Hide compartments"
                checked={filters.hideCompartments}
                onChange={(e) => {
                  setFilters({
                    ...filters,
                    hideCompartments: e.target.checked,
                  });
                }}
              />
            </Tooltip>
            <Tooltip content='Hide species nodes such as "Homo sapiens"'>
              <Checkbox
                label="Hide species"
                checked={filters.hideSpecies}
                onChange={(e) => {
                  setFilters({
                    ...filters,
                    hideSpecies: e.target.checked,
                  });
                }}
              />
            </Tooltip>
            <Tooltip content="Hide database references">
              <Checkbox
                label="Hide database references"
                checked={filters.hideDatabaseReferences}
                onChange={(e) => {
                  setFilters({
                    ...filters,
                    hideDatabaseReferences: e.target.checked,
                  });
                }}
              />
            </Tooltip>
            <Tooltip content="Show Homo sapiens only nodes">
              <Checkbox
                label="Homo sapiens only"
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
          <Tooltip content="Double clicking a node centers the canvas onto it">
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
      </Dialog>
    </>
  );
}
