import React from "react";
import {
  Button,
  Tooltip,
  Dialog,
  InputGroup,
  Spinner,
  Card,
  Tag,
} from "@blueprintjs/core";

import { ttParams, dialogParams, useDialog, useDefinedState } from "../tools";
import {
  getNodePropValue,
  getNodeSecondaryPropValue,
  useNodes,
  usePropertyTypes,
} from "./nodes";
import { useVisualizerContext } from "src/turingvisualizer";
import { SelectProperty } from "../SelectProperty";

export default function AddNodeDialog() {
  const vis = useVisualizerContext();
  const dialog = useDialog();
  const currentPropName = useDefinedState("");
  const [currentPropValue, setCurrentPropValue] = React.useState("");
  const properties = usePropertyTypes(vis.state()!.dbName);
  const selectedNodeIds = vis.state()!.selectedNodeIds;

  React.useEffect(() => {
    if (properties.includes("displayName")) {
      currentPropName.setOnce("displayName");
      return;
    }
    if (properties.includes("label")) {
      currentPropName.setOnce("label");
      return;
    }
  }, [properties, currentPropName]);

  const { loading, sortedNodes, query } = useNodes(
    vis.state()!.dbName,
    currentPropName.value,
    currentPropValue
  );

  return (
    <>
      <Tooltip {...ttParams} content="Add nodes from the database">
        <Button text="Add nodes" icon="add" onClick={dialog.open} />
      </Tooltip>
      <Dialog
        {...dialogParams}
        isOpen={dialog.isOpen}
        onClose={dialog.close}
        title="Add nodes from the database">
        <div className="h-[50vh]">
          <form
            onSubmit={(e) => {
              e.preventDefault();
              query();
            }}>
            <div className="flex space-x-2 items-center p-6 pt-4 pb-4">
              <div>Find by property</div>
              <SelectProperty
                currentProp={currentPropName.value}
                setCurrentProp={currentPropName.set}
                properties={properties}
              />
              <InputGroup
                className="flex-1"
                onChange={(e) => setCurrentPropValue(e.target.value)}
              />
              <Button type="submit" icon="search" />
            </div>
          </form>
          <div className="w-full h-full overflow-auto">
            {loading ? (
              <div className="flex h-full w-full items-center justify-center">
                <Spinner size={100} />
              </div>
            ) : (
              sortedNodes.map((n) => {
                const isSelected = selectedNodeIds.includes(n.id);

                return (
                  <Card
                    key={n.id}
                    interactive
                    compact
                    className="flex justify-between"
                    onClick={(e) => {
                      e.preventDefault();
                      vis.callbacks()?.setInspectedNode(n);
                      e.stopPropagation();
                    }}>
                    <div className="flex space-x-2">
                      <Tag className="w-20 text-center">{n.id}</Tag>
                      <div>
                        <div className={isSelected ? "primary-light" : ""}>
                          {getNodePropValue(n, currentPropName.value)}
                        </div>
                        <div className="pl-9 text-sm gray3">
                          {getNodeSecondaryPropValue(n, currentPropName.value)}
                        </div>
                      </div>
                    </div>
                    {isSelected ? (
                      <Button
                        icon="remove"
                        onClick={(
                          e: React.MouseEvent<HTMLElement, MouseEvent>
                        ) => {
                          e.preventDefault();
                          vis
                            .callbacks()!
                            .setSelectedNodeIds([
                              ...vis
                                .state()!
                                .selectedNodeIds.filter(
                                  (id) => n.turing_id !== id
                                ),
                            ]);
                          e.stopPropagation();
                        }}
                      />
                    ) : (
                      <Button
                        icon="add"
                        onClick={(
                          e: React.MouseEvent<HTMLElement, MouseEvent>
                        ) => {
                          e.preventDefault();
                          vis
                            .callbacks()!
                            .setSelectedNodeIds([
                              ...vis.state()!.selectedNodeIds,
                              n.turing_id,
                            ]);
                          e.stopPropagation();
                        }}
                      />
                    )}
                  </Card>
                );
              })
            )}
          </div>
        </div>
      </Dialog>
    </>
  );
}
