// Core
import React from "react";
import { useSelector, useDispatch } from "react-redux";

// @mui/material
import { IconButton } from "@mui/material";
import DeleteIcon from "@mui/icons-material/Delete";

// Turing
import BorderedContainer from "@Components/BorderedContainer";
import NodeChip from "@Components/NodeChip";
import NodeStack from "@Components/NodeStack";
import { BorderedContainerTitle } from "@Components/BorderedContainer";
import { clearSelectedNodes } from "@App/actions";

export default function SelectedNodesContainer() {
  const selectedNodes = useSelector((state) => state.selectedNodes);
  const dispatch = useDispatch();

  return (
    <BorderedContainer
      title={
        <BorderedContainerTitle title="Selected nodes">
          {selectedNodes.length !== 0 && (
            <IconButton
              onClick={() => {
                dispatch(clearSelectedNodes());
              }}>
              <DeleteIcon />
            </IconButton>
          )}
        </BorderedContainerTitle>
      }>
      <NodeStack>
        {Object.values(selectedNodes).map((node, i) => (
          <NodeChip node={node} key={i} nodeId={node.id} />
        ))}
      </NodeStack>
    </BorderedContainer>
  );
}
