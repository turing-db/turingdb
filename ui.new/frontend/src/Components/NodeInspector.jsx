// Core
import React from "react";
import axios from "axios";
import { useDispatch, useSelector } from "react-redux";
import { useTheme } from "@emotion/react";

// @mui/material
import {
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  Grid,
  Typography,
} from "@mui/material";

// Turing
import BorderedContainer, {
  BorderedContainerTitle,
} from "src/Components/BorderedContainer";
import NodeChip from "src/Components/NodeChip";
import NodeStack from "src/Components/NodeStack";
import { Secondary } from "src/Components/Span";
import * as actions from "src/App/actions";
import * as thunks from "src/App/thunks";
import { useQuery } from "src/App/queries";

const containerHeight = "120vh";
const edgeCountLim = 40;

const Edges = (props) => {
  const { edgeIds, type, edgeCount } = props;
  const theme = useTheme();
  const dbName = useSelector((state) => state.dbName);
  const dispatch = useDispatch();

  const { data, isFetching } = useQuery(
    ["fetch_edge_data", edgeIds],
    React.useCallback(async () => {
      const edges = await dispatch(thunks.getEdges(dbName, edgeIds))
        .then((res) => (res.error ? {} : res))
        .catch(() => {
          return {};
        });

      var rawIds = {};

      if (type === "in") {
        for (const e of Object.values(edges)) {
          if (e.edge_type_name in rawIds === false) {
            rawIds[e.edge_type_name] = [];
          }
          rawIds[e.edge_type_name].push(e.source_id);
        }
      } else {
        for (const e of Object.values(edges)) {
          if (e.edge_type_name in rawIds === false) {
            rawIds[e.edge_type_name] = [];
          }
          rawIds[e.edge_type_name].push(e.target_id);
        }
      }

      // Fetching nodes
      const ids = [...Object.values(rawIds)]
        .flat()
        .filter((id, i, arr) => arr.indexOf(id) === i);
      const rawNodes = await dispatch(
        thunks.getNodes(dbName, ids, {
          yield_edges: false,
        })
      ).then((res) => res);

      const nodes = Object.fromEntries(
        Object.entries(rawIds).map(([et, ids]) => [
          et,
          ids.map((id) => rawNodes[id]),
        ])
      );

      return nodes;
    }, [dbName, dispatch, edgeIds, type])
  );

  const nodes = data || {};

  return (
    <BorderedContainer
      sx={{ maxHeight: containerHeight }}
      title={
        <BorderedContainerTitle
          title={type === "in" ? "In edges" : "Out edges"}>
          {edgeCount > edgeCountLim ? (
            <Typography>
              Showing {edgeCountLim}/{edgeCount} edges
            </Typography>
          ) : (
            <Typography> {edgeCount} edges</Typography>
          )}
          <Button
            onClick={() =>
              dispatch(actions.selectNodes(Object.values(nodes).flat()))
            }>
            Select all
          </Button>
        </BorderedContainerTitle>
      }>
      <Grid container>
        {isFetching ? (
          <CircularProgress />
        ) : (
          <>
            {Object.keys(nodes).map((edge_type) => (
              <Grid
                item
                container
                xs={12}
                key={"global-edge-type-" + edge_type}>
                <Grid
                  item
                  xs={3.5}
                  key={edge_type}
                  color={theme.palette.secondary.main}
                  display="flex"
                  overflow="auto"
                  alignItems="center">
                  <Box
                    display="flex"
                    alignItems="center"
                    key={"edge-type-container-" + edge_type}>
                    {edge_type}
                  </Box>
                </Grid>
                <Grid
                  item
                  xs
                  key={"grid-item-" + edge_type}
                  maxHeight="50vh"
                  overflow="auto">
                  <NodeStack key={"node-stack-" + edge_type}>
                    {nodes[edge_type].map((n, i) => (
                      <NodeChip key={i} nodeId={n.id} node={n} />
                    ))}
                  </NodeStack>
                </Grid>
              </Grid>
            ))}
          </>
        )}
      </Grid>
    </BorderedContainer>
  );
};

export const Properties = (props) => {
  const { node, title = true, disablePadding = false } = props;
  const dbName = useSelector((state) => state.dbName);

  const listNodeProperties = React.useCallback(
    async () =>
      node &&
      (await axios
        .post("/api/list_node_properties", {
          db_name: dbName,
          id: node.id,
        })
        .then((res) => {
          return res.data;
        })
        .catch((err) => {
          console.log(err);
          return {};
        })),
    [dbName, node]
  );

  const queryKey = ["list_node_properties", node?.id];
  const { data, status } = useQuery(queryKey, listNodeProperties);

  const properties = data || {};

  return (
    <BorderedContainer
      title={title && <BorderedContainerTitle title="Node properties" />}
      sx={{ maxHeight: containerHeight }}
      disablePadding={disablePadding}>
      {node & (status === "loading") ? (
        <CircularProgress size={20} />
      ) : (
        Object.keys(properties).map((pName) => (
          <Box key={"box-key-" + pName} display="flex" alignItems="center">
            <Typography variant="body2" p={disablePadding ? 0 : 1}>
              <Secondary>{pName}</Secondary>:{" "}
              <span>{properties[pName].toString()}</span>
            </Typography>
          </Box>
        ))
      )}
    </BorderedContainer>
  );
};

export default function NodeInspector(props) {
  const { open, onClose } = props;
  const inspectedNode = useSelector((state) => state.inspectedNode);
  const selectedNodes = useSelector((state) => state.selectedNodes);
  const dispatch = useDispatch();

  const inCount = React.useMemo(
    () => (inspectedNode ? inspectedNode?.ins?.length : 0),
    [inspectedNode]
  );

  const outCount = React.useMemo(
    () => (inspectedNode ? inspectedNode?.outs?.length : 0),
    [inspectedNode]
  );

  const inEdgeIds = React.useMemo(
    () =>
      inspectedNode
        ? (() =>
            inspectedNode?.ins?.length > edgeCountLim
              ? inspectedNode?.ins?.slice(0, edgeCountLim)
              : inspectedNode?.ins)()
        : [],
    [inspectedNode]
  );

  const outEdgeIds = React.useMemo(
    () =>
      inspectedNode
        ? (() =>
            inspectedNode?.outs?.length > edgeCountLim
              ? inspectedNode?.outs?.slice(0, edgeCountLim)
              : inspectedNode?.outs)()
        : [],
    [inspectedNode]
  );

  if (!inspectedNode) return <></>;

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>Node id: {inspectedNode.id}</DialogTitle>
      <DialogContent dividers>
        <BorderedContainer
          title={
            <BorderedContainerTitle title={"Node type: "}>
              {inspectedNode.node_type}
            </BorderedContainerTitle>
          }></BorderedContainer>
        <Properties node={inspectedNode} />
        {<Edges edgeIds={inEdgeIds} edgeCount={inCount} type="in" />}
        {<Edges edgeIds={outEdgeIds} edgeCount={outCount} type="out" />}
        <Box display="flex" flexDirection="column" alignItems="center">
          {!(inspectedNode.id in selectedNodes) ? (
            <Button
              variant="contained"
              onClick={() => dispatch(actions.selectNode(inspectedNode))}>
              Add node to selection
            </Button>
          ) : (
            <Button
              variant="contained"
              onClick={() => dispatch(actions.unselectNode(inspectedNode))}>
              Remove node from selection
            </Button>
          )}
        </Box>
      </DialogContent>
    </Dialog>
  );
}
