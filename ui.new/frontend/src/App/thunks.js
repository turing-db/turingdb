import axios from 'axios'
import * as actions from './actions'

export const getNodes = (dbName, allIds) => async (dispatch, getState) => {
    const cache = getState().nodeCache;
    const ids = allIds
        .filter(id => !(id in cache));

    if (ids.length !== 0) {
        await dispatch(fetchNodes(dbName, { ids }))
    }

    const newCache = getState().nodeCache;
    return Object.fromEntries(allIds.map(id => [id, newCache[id]]));
}

export const fetchNodes = (dbName, filters) => async (dispatch) => {
    return await axios
        .post("/api/list_nodes", {
            db_name: dbName,
            ...filters
        })
        .then(res => {
            if (res.data.error) {
                return []
            }

            dispatch(actions.cacheNodes(res.data));
            return res.data;
        });
}

export const getEdges = (dbName, allIds) => async (dispatch, getState) => {
    const cache = getState().edgeCache;
    const ids = allIds
        .filter(id => !(id in cache));

    if (ids.length !== 0) {
        await dispatch(fetchEdges(dbName, { ids }))
    }

    const newCache = getState().edgeCache;
    return Object.fromEntries(allIds.map(id => [id, newCache[id]]));
}

export const fetchEdges = (dbName, filters) => async (dispatch) => {
    return await axios
        .post("/api/list_edges", {
            db_name: dbName,
            ...filters
        })
        .then(res => {
            if (res.data.error) {
                console.log(res.data.error);
                return {}
            }

            dispatch(actions.cacheEdges(res.data));
            return res.data;
        });

}
