import axios from "axios";
import * as actions from "./actions";

export const getNodes =
  (dbName, allIds, params = {}) =>
  async (dispatch, getState) => {
    if (!params.yield_edges) {
      const rawNodes = await dispatch(
        fetchNodes(dbName, { ids: allIds, ...params })
      );
      return Object.fromEntries(Object.values(rawNodes).map((n) => [n.id, n]));
    }

    const cache = getState().nodeCache;

    const ids = allIds.filter((id) => !(id in cache));

    if (ids.length !== 0) {
      await dispatch(fetchNodes(dbName, { ids, ...params }));
    }

    const newCache = getState().nodeCache;
    return Object.fromEntries(allIds.map((id) => [id, newCache[id]]));
  };

export const fetchNodes = (dbName, filters) => async (dispatch) => {
  return await axios
    .post("/api/list_nodes", {
      db_name: dbName,
      ...filters,
    })
    .then((res) => {
      if (res.data.error) {
        return res.data;
      }

      if (filters.yield_edges)
        // Store nodes only if the info is complete
        dispatch(actions.cacheNodes(res.data.nodes));

      return res.data.nodes;
    });
};

export const getEdges =
  (dbName, allIds, params) => async (dispatch, getState) => {
    const cache = getState().edgeCache;
    const ids = allIds.filter((id) => !(id in cache));

    if (ids.length !== 0) {
      await dispatch(fetchEdges(dbName, { ids, params }));
    }

    const newCache = getState().edgeCache;
    return Object.fromEntries(allIds.map((id) => [id, newCache[id]]));
  };

export const fetchEdges = (dbName, filters) => async (dispatch) => {
  return await axios
    .post("/api/list_edges", {
      db_name: dbName,
      ...filters,
    })
    .then((res) => {
      if (res.data.error) {
        console.log(res.data.error);
        return {};
      }

      dispatch(actions.cacheEdges(res.data));
      return res.data;
    });
};

export const inspectNode = (dbName, nodeId) => async (dispatch) => {
  if (nodeId === null) {
    dispatch(actions.inspectNode(null));
    return;
  }

  dispatch(getNodes(dbName, [nodeId], { yield_edges: true })).then((res) =>
    dispatch(actions.inspectNode(Object.values(res)[0]))
  );
};
