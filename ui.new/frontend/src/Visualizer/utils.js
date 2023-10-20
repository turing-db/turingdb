import React from 'react';
import { useSelector } from 'react-redux';
import { useQuery } from '../App/queries';
import * as queries from './queries'
import * as colors from './colors'
import * as actions from './actions';
import { useTheme } from '@emotion/react';

export const useCytoscapeElements = () => {
    const displayedNodeProperty = useSelector(state => state.displayedNodeProperty);
    const edgeLabel = useSelector(state => state.visualizer.edgeLabel);
    const edgeColors = useSelector(state => state.visualizer.edgeColors);
    const nodeColors = useSelector(state => state.visualizer.nodeColors);
    const rawElements = queries.useElementsQuery();
    const theme = useTheme();
    const { nodeColorMakers, edgeColorMakers } = colors.useElementColorMakers(rawElements);

    const getElementLabel = React.useCallback(el => {
        if (el.group === "nodes") {
            if (displayedNodeProperty === "None") return "";
            if (displayedNodeProperty === "NodeType") return el.data.node_type_name;
            return el.data.properties[displayedNodeProperty] || "";
        } else { // edges
            if (edgeLabel === "None") return "";
            if (edgeLabel === "EdgeType") return el.data.edge_type_name + "\n\u2060";
            return el.data.properties[edgeLabel] + "\n\u2060" || "";
        }
    }, [displayedNodeProperty, edgeLabel]);

    const getElementColors = React.useCallback(el => {
        if (el.group === "nodes") {
            const colorSetId = nodeColors.mapping[el.data.id] || 0;
            const colorSet = nodeColors.colorSets[colorSetId];
            const colorMaker = nodeColorMakers[colorSet.mode];

            if (el.data.type === "selected") {
                return {
                    iconColor: colorMaker(el.data, colorSet) || theme.nodes.selected.icon,
                    textColor: colorMaker(el.data, colorSet) || theme.nodes.selected.text
                };
            }
            else { // neighbor
                return {
                    iconColor: colorMaker(el.data, colorSet) || theme.nodes.neighbor.icon,
                    textColor: colorMaker(el.data, colorSet) || theme.nodes.neighbor.text
                };
            }

        } else { // edges
            const colorSetId = edgeColors.mapping[el.data.id] || 0;
            const colorSet = edgeColors.colorSets[colorSetId];
            const colorMaker = edgeColorMakers[colorSet.mode];

            if (el.data.type === "connecting")
                return {
                    lineColor: colorMaker(el.data, colorSet) || theme.edges.connecting.line,
                    textColor: colorMaker(el.data, colorSet) || theme.edges.connecting.text
                };
            else // neighbor
                return {
                    lineColor: colorMaker(el.data, colorSet) || theme.edges.neighbor.line,
                    textColor: colorMaker(el.data, colorSet) || theme.edges.neighbor.text
                };
        }
    }, [edgeColorMakers, edgeColors, nodeColorMakers, nodeColors, theme]);

    const data = React.useMemo(() => rawElements.data?.map(el => ({
        ...el,
        data: {
            ...el.data,
            label: getElementLabel(el),
            ...getElementColors(el),
        }
    })), [getElementColors, getElementLabel, rawElements.data]);

    return { ...rawElements, data };
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


