// Core
import { useSelector, useDispatch } from "react-redux";

// @mui/material
import { IconButton } from "@mui/material";
import DeleteIcon from "@mui/icons-material/Delete";

// Turing
import BorderedContainer from "src/Components/BorderedContainer";
import NodeChip from "src/Components/NodeChip";
import NodeStack from "src/Components/NodeStack";
import { BorderedContainerTitle } from "src/Components/BorderedContainer";
import { clearSelectedNodes } from "src/App/actions";

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
