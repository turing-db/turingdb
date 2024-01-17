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
import { useVisualizerContext } from "../../";
import { useDialog, dialogParams, ttParams } from "../../tools";
import { useWidgetValue } from "./hooks";
import NodeFilters from "./NodeFilters";

const LENGTH_RATIO = 0.1;

export default function SettingsDialog() {
  const vis = useVisualizerContext();
  const dialog = useDialog();

  const centerOnDoubleClicked = useWidgetValue({
    getCanvasValue: () => vis.state()!.layouts.centerOnDoubleClicked,
    setCanvasValue: vis.callbacks()!.centerOnDoubleClicked,
  });

  const edgeLengthVal = useWidgetValue({
    getCanvasValue: () => vis.state()!.layouts.definitions[0]!.edgeLengthVal!,
    setCanvasValue: (v) => {
      vis.callbacks()!.setDefaultCyLayout({
        ...vis.state()!.layouts.definitions[0]!,
        edgeLengthVal: v,
      });
    },
  });

  const nodeSpacing = useWidgetValue({
    getCanvasValue: () => vis.state()!.layouts.definitions[0]!.nodeSpacing,
    setCanvasValue: (v) => {
      vis.callbacks()!.setDefaultCyLayout({
        ...vis.state()!.layouts.definitions[0]!,
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

          <NodeFilters/>
          <LockMenu />
          <Tooltip content="Double clicking a node centers the canvas onto it" className="w-max">
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
