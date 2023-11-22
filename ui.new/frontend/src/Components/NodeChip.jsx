// Core
import { useDispatch, useSelector } from "react-redux";

// @mui/material
import { Grid, Chip } from "@mui/material";

// @mui/icons-material
import AddCircleSharpIcon from "@mui/icons-material/AddCircleSharp";
import CircleSharpIcon from "@mui/icons-material/CircleSharp";
import DeleteSharpIcon from "@mui/icons-material/DeleteSharp";

// Turing
import * as actions from "src/App/actions";
import * as thunks from "src/App/thunks";

const NodeChip = (props) => {
  const node = props.node;
  const selectedNodes = useSelector((state) => state.selectedNodes);
  const nodeLabel = useSelector((state) => state.nodeLabel);
  const dbName = useSelector((state) => state.dbName);
  const dispatch = useDispatch();

  const onClick = () => dispatch(thunks.inspectNode(dbName, props.nodeId));

  const onDelete = node
    ? selectedNodes[node.id]
      ? () => dispatch(actions.unselectNode(node))
      : () => dispatch(actions.selectNode(node))
    : () => {};

  const deleteIcon = node ? (
    selectedNodes[node.id] ? (
      <DeleteSharpIcon />
    ) : (
      <AddCircleSharpIcon />
    )
  ) : (
    <CircleSharpIcon />
  );

  const label = node?.properties[nodeLabel] || "Node " + props.nodeId;

  return (
    <Grid item p={0.5}>
      <Chip
        onClick={onClick}
        onDelete={onDelete}
        deleteIcon={deleteIcon}
        variant="outlined"
        label={label}
      />
    </Grid>
  );
};

export default NodeChip;
