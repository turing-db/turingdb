import React from 'react';
import axios from 'axios';
import { useQuery as useNativeQuery } from '@tanstack/react-query';
import { useSelector } from 'react-redux';

export const useQuery = (key, fn, options = {}) =>
    useNativeQuery(key, fn, {
        immutableCheck: { warnAfter: 256 },
        serializableCheck: { warnAfter: 256 },
        refetchOnMount: false,
        refetchOnWindowFocus: false,
        staleTime: 100_000,
        ...options
    })

export const useNodePropertyTypesQuery = (options = {}) => {
    const dbName = useSelector(state => state.dbName);
    return useQuery(
        ["list_node_property_types", dbName],
        React.useCallback(() => axios
            .post("/api/list_node_property_types", { db_name: dbName })
            .then(res => res.data)
            .catch(err => { console.log(err); return []; })
            , [dbName]
        ),
        options
    );

};

export const useEdgePropertyTypesQuery = (options = {}) => {
    const dbName = useSelector(state => state.dbName);
    return useQuery(
        ["list_edge_property_types", dbName],
        React.useCallback(() => axios
            .post("/api/list_edge_property_types", { db_name: dbName })
            .then(res => res.data)
            .catch(err => { console.log(err); return []; })
            , [dbName]
        ),
        options
    );

};
