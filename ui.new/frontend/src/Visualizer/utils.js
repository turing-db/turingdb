import axios from 'axios';
import React from 'react';
import { useSelector } from 'react-redux';
import { useQuery } from '../App/queries';
import * as actions from './actions';
import { EDGE_COLOR_MODES } from './constants'

const colorGradient = (min, max, value) => {
    const extent = max - min;
    const r = parseInt(255 * (value - min) / extent);
    const g = 255 - parseInt(255 * (value - min) / extent);
    return `rgb(${r},${g},0)`;
}

export const useCytoscapeElements = () => {
    const dbName = useSelector(state => state.dbName);
    const selectedNodes = useSelector(state => state.selectedNodes);
    const displayedNodeProperty = useSelector(state => state.displayedNodeProperty);
    const edgeLabel = useSelector(state => state.visualizer.edgeLabel);
    const hiddenNodeIds = useSelector(state => state.visualizer.hiddenNodeIds);
    const filters = useSelector(state => state.visualizer.filters);
    const edgeColorMode = useSelector(state => state.visualizer.edgeColorMode);
    const nodeIds = Object.keys(selectedNodes);

    const res = useQuery(
        ["cytoscape_elements", dbName, nodeIds, hiddenNodeIds, filters],
        React.useCallback(async () => {
            const rawElements = await axios
                .post("/api/viewer/init", {
                    db_name: dbName,
                    node_ids: nodeIds,
                    hidden_node_ids: hiddenNodeIds,
                    max_edge_count: 30,
                    node_property_filter_out: [
                        ...(filters.hideCompartments ? [
                            ["schemaClass", "Compartment"]
                        ] : []),

                        ...(filters.hideSpecies ? [
                            ["schemaClass", "Species"]
                        ] : []),

                        ...(filters.hidePublications ? [
                            ["schemaClass", "InstanceEdit"],
                            ["schemaClass", "ReviewStatus"],
                            ["schemaClass", "LiteratureReference"]
                        ] : []),

                        ...(filters.hideDatabaseReferences ? [
                            ["schemaClass", "ReferenceGeneProduct"],
                            ["schemaClass", "ReferenceDatabase"]
                        ] : []),
                    ],
                    node_property_filter_in: [
                        ...(filters.showOnlyHomoSapiens ? [
                            ["speciesName", "Homo sapiens"]
                        ] : []),
                    ],
                })
                .then(res => res.data);
            return rawElements;
        }, [dbName, nodeIds, hiddenNodeIds, filters]))

    const edges = res.data ? res.data.filter(el => el.group === "edges") : [];
    const edgeTypes = edges
        .map(e => e.data.edge_type_name)
        .filter((et, i, arr) => arr.indexOf(et) === i);
    const gradientColorPropertyValues = edgeColorMode.mode === EDGE_COLOR_MODES.GradientProperty
        ? (() => {
            const propertiesAreFloats = edges
                .every(e => !isNaN(parseFloat(e.data.properties[edgeColorMode.data.propTypeName])));
            if (!propertiesAreFloats) return [];

            const propertyValues = Object.fromEntries(
                edges.map(e => [e.data.id, e.data.properties[edgeColorMode.data.propTypeName]])
            );

            const min = Math.min.apply(Math, Object.values(propertyValues));
            const max = Math.max.apply(Math, Object.values(propertyValues));
            return Object.fromEntries(
                Object.entries(propertyValues)
                    .map(([id, v]) => [id, colorGradient(min, max, v)])
            );
        })()
        : {};

    const uniquePropertyValues = edgeColorMode.mode === EDGE_COLOR_MODES.QuantitativeProperty
        ? edges
            .map(e => e.data.properties[edgeColorMode.data.propTypeName])
            .filter((pt, i, arr) => arr.indexOf(pt) === i)
        : [];

    const edgeColorMakers = {
        [EDGE_COLOR_MODES.None]: () => undefined,
        [EDGE_COLOR_MODES.EdgeType]: (elData) => {
            const colorIndex = edgeTypes.indexOf(elData.edge_type_name)
                % quantitativeColors.length;
            const color = quantitativeColors[colorIndex];
            return color;
        },
        [EDGE_COLOR_MODES.GradientProperty]: (elData) => {
            return gradientColorPropertyValues[elData.id];
        },
        [EDGE_COLOR_MODES.QuantitativeProperty]: (elData) => {
            const colorIndex = uniquePropertyValues.indexOf(
                elData.properties[edgeColorMode.data.propTypeName]
            ) % quantitativeColors.length;
            const color = quantitativeColors[colorIndex];
            return color;
        }
    }

    const getElementLabel = el => {
        if (el.group === "nodes") {
            if (displayedNodeProperty === "None") return "";
            if (displayedNodeProperty === "NodeType") return el.data.node_type_name;
            return el.data.properties[displayedNodeProperty] || "";
        } else { // edges
            if (edgeLabel === "None") return "";
            if (edgeLabel === "EdgeType") return el.data.edge_type_name + "\n\u2060";
            return el.data.properties[edgeLabel] + "\n\u2060" || "";
        }
    };

    const getElementColors = el => {
        if (el.group === "nodes") {
            if (el.data.type === "selected")
                return { "backgroundColor": "rgb(204,204,204)", color: "rgb(150,150,150)" };
            else // neighbor
                return { "backgroundColor": "rgb(155,155,155)", color: "rgb(90, 90, 90)" };
        } else { // edges
            if (el.data.type === "connecting")
                return { "lineColor": edgeColorMakers[edgeColorMode.mode](el.data) || "rgb(0,153,0)" };
            else // connecting
                return { "lineColor": edgeColorMakers[edgeColorMode.mode](el.data) || "rgb(0,80,0)" };
        }
    }

    const data = res.data &&
        res.data.map(el => {
            return {
                ...el,
                data: {
                    ...el.data,
                    label: getElementLabel(el),
                    ...getElementColors(el),
                }
            };
        });

    return { ...res, data };
}

export const applyLayouts = (cy, layouts, cyLayout) => {
    // Applying default layout
    cy.defaultLayoutRef.current = cy
        .filter(e => layouts.mapping[e.id()] === undefined)
        .layout(cyLayout)
    cy.defaultLayoutRef.current.run();

    // Applying layouts except the default one
    Object
        .entries(layouts.definitions)
        .forEach(([layoutId, layout]) => {
            cy
                .filter(e => layouts.mapping[e.id()] === parseInt(layoutId))
                .layout(layout)
                .run();
        });
}

export const showCellCellInteraction = (dispatch, cy, fitRequired) => {
    const defaultLayout = cy.defaultLayoutRef.current;
    if (defaultLayout) defaultLayout.stop();

    const nodes = cy.nodes();
    if (nodes.length === 0) return;

    const uniqueTitleValues = nodes
        .map(n => n.data().properties.title)
        .filter((title, i, arr) => arr.indexOf(title) === i);

    const filteredNodes = uniqueTitleValues
        .map(title => nodes
            .filter(n => n.data().properties.title === title));

    fitRequired.current = true;

    const nLines = uniqueTitleValues.length;
    const maxLength = Math.max.apply(Math, filteredNodes.map(arr => arr.length));
    const yStretch = 35.0;
    const aspectRatio = cy.width() / cy.height();
    const canvasWidth = maxLength * yStretch * aspectRatio;
    const xStretch = canvasWidth / nLines;

    uniqueTitleValues.forEach((_title, x) => {
        const currentNodes = filteredNodes[x];
        const xPos = x * xStretch;
        const yShift = (maxLength - currentNodes.length) / 2

        dispatch(actions.addLayout(
            {
                name: 'preset',
                positions: {
                    ...Object.fromEntries(currentNodes.map((n, y) =>
                        [n.id(), { x: xPos, y: (y + yShift) * yStretch }]
                    ))
                }
            },
            currentNodes.map(n => n.id())
        ));
    })
};

export const useCyStyleQuery = () => useQuery(
    ["get_cystyle"],
    () => fetch('/cy-style.json')
        .then(res => res.json())
        .catch(err => console.log(err))
);

export const quantitativeColors = [
    "#29A634",
    "#D1980B",
    "#D33D17",
    "#9D3F9D",
    "#00A396",
    "#DB2C6F",
    "#8EB125",
    "#946638",
    "#7961DB"
];


