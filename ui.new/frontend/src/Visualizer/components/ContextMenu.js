import React from 'react';
import classNames from "classnames";

import { ContextMenu as BPContextMenu, Menu, MenuItem } from '@blueprintjs/core';
import { useDispatch, useSelector } from "react-redux";
import { Icon } from "@blueprintjs/core";

import * as actions from "../actions";
import * as appActions from '../../App/actions';
import * as thunks from "../../App/thunks";
import { EDGE_COLOR_MODES } from '../constants';

const NodeContextMenu = (props) => {
    const dispatch = useDispatch();
    const selectedNodes = useSelector(state => state.selectedNodes);
    const dbName = useSelector(state => state.dbName);
    const layouts = useSelector(state => state.visualizer.layouts);
    const isSelected = selectedNodes[props.data.id] !== undefined;
    const nodeProps = props.data.properties;

    const hideNodes = React.useCallback((cy) => {
        const nodes = cy.nodes().filter(e => e.selected());
        const selectedNodes = nodes.filter(e => e.data().type === "selected");
        const neighborNodes = nodes.filter(e => e.data().type === "neighbor");
        const selectedNodeIds = selectedNodes.map(n => n.id());
        const neighborNodeIds = neighborNodes.map(n => n.id());

        selectedNodeIds.length !== 0 && dispatch(thunks.getNodes(dbName, selectedNodeIds))
            .then(res => Object.values(res).forEach(n => dispatch(appActions.unselectNode(n))));
        neighborNodeIds.length !== 0 && dispatch(actions.hideNodes(neighborNodeIds));

    }, [dbName, dispatch]);

    const VerticalLineIcon = () => <Icon
        icon="layout-linear"
        style={{
            rotate: "90deg"
        }}
    />;

    return (
        <Menu>
            <MenuItem
                icon="layout"
                text="Set layout..."
            >
                <MenuItem
                    icon=<VerticalLineIcon />
                    text="Vertical line"
                    onClick={() => {
                        const eles = props.cy.current.filter(e => e.selected());
                        if (eles.length === 0) return;

                        dispatch(actions.addLayout(
                            {
                                name: 'preset',
                                positions: {
                                    ...Object.fromEntries(eles.map((e, i) =>
                                        [e.id(), { x: layouts.layoutCount * 50.0, y: i * 40.0 }]
                                    ))
                                }
                            },
                            eles.map(e => e.id())
                        ));
                    }}
                />
                <MenuItem
                    icon="layout-auto"
                    text="Auto"
                    onClick={() => {
                        const eles = props.cy.current.filter(e => e.selected());
                        if (eles.length === 0) return;
                        dispatch(actions.setDefaultLayout(eles.map(e => e.id())));
                    }}
                />
            </MenuItem>
            <MenuItem
                text="Select all..."
                icon="select"
            >
                <MenuItem icon="property" text="by same property...">
                    {Object.keys(nodeProps).map(propName =>
                        <MenuItem key={propName} icon="label" text={propName}
                            onClick={() =>
                                props.cy.current
                                    .filter(e =>
                                        e.data().properties[propName] &&
                                        e.data().properties[propName] === nodeProps[propName])
                                    .forEach(e => e.select())
                            }
                        />
                    )}
                </MenuItem>
                <MenuItem
                    icon="property"
                    text="by same layout"
                    onClick={() =>
                        props.cy.current
                            .filter(e => layouts.mapping[e.id()] === layouts.mapping[props.data.id])
                            .forEach(e => e.select())
                    }
                />
            </MenuItem>

            {isSelected
                // Selected nodes only
                ? <>
                    <MenuItem
                        text="Hide"
                        icon="graph-remove"
                        onClick={() => hideNodes(props.cy.current)}
                    />
                    <MenuItem
                        text="Collapse neighbors"
                        icon="collapse-all"
                        onClick={() => {
                            const node = props.cy.current.$id(props.data.id);
                            const nodeIds = node
                                .neighborhood()
                                .nodes()
                                .map(n => n.id())
                                .filter(id => !selectedNodes[id]);
                            dispatch(actions.hideNodes(nodeIds))
                        }}
                    />
                </>

                // Neighbors nodes only
                : <>
                    <MenuItem
                        text="Add to selection"
                        icon="add"
                        onClick={() => {
                            dispatch(thunks.getNodes(dbName, [props.data.id]))
                                .then(res => dispatch(appActions.selectNode(res[props.data.id])))
                        }}
                    />
                    <MenuItem
                        text="Hide"
                        icon="graph-remove"
                        onClick={() => hideNodes(props.cy.current)}
                    />
                </>}
        </Menu>
    );
};

const EdgeColorContextMenu = (props) => {
    const dispatch = useDispatch();

    return (
        <Menu small>
            <MenuItem
                text="Set edge colors..."
                icon="tint"
            >
                <MenuItem
                    text="None"
                    icon="cross"
                    onClick={() => { dispatch(actions.setEdgeColorMode(EDGE_COLOR_MODES.None)) }}
                />
                <MenuItem
                    text="Gradient"
                    icon="text-highlight"
                >
                    <MenuItem
                        text="Based on property..."
                        icon="property"
                    >
                        {props.edgePropertyTypes.map(propName =>
                            <MenuItem key={propName} icon="label" text={propName}
                                onClick={() =>
                                    dispatch(actions.setEdgeColorMode(
                                        EDGE_COLOR_MODES.GradientProperty,
                                        { propTypeName: propName }
                                    ))
                                }
                            />
                        )}
                    </MenuItem>
                </MenuItem>

                <MenuItem
                    text="Quantitative"
                    icon="list"
                >
                    <MenuItem
                        text="Based on property..."
                        icon="property"
                    >
                        {props.edgePropertyTypes.map(propName =>
                            <MenuItem key={propName} icon="label" text={propName}
                                onClick={() =>
                                    dispatch(actions.setEdgeColorMode(
                                        EDGE_COLOR_MODES.QuantitativeProperty,
                                        { propTypeName: propName }
                                    ))
                                }
                            />
                        )}
                    </MenuItem>
                    <MenuItem
                        text="Based on edge type"
                        icon="one-to-one"
                        onClick={() => { dispatch(actions.setEdgeColorMode(EDGE_COLOR_MODES.EdgeType)) }}
                    />
                </MenuItem>
            </MenuItem>
        </Menu>
    );
}

const EdgeContextMenu = () => {
    return <></>
};

const BackgroundContextMenu = (props) => {
    const dispatch = useDispatch();
    const dbName = useSelector(state => state.dbName);
    const edgePropertyTypes = props.cy.current
        .edges()
        .map(e => Object.keys(e.data().properties))
        .flat()
        .filter((p, i, arr) => arr.indexOf(p) === i);

    const publicationNodeIds = (() => {
        switch (dbName) {
            case "reactome":
                return props.elements
                    .filter(e => e.group() === "nodes" && e.data().type === "neighbor")
                    .map(e => e.data())
                    .filter(e => e.properties.schemaClass === "InstanceEdit")
                    .map(e => e.id);

            default:
                return [];
        }
    })();

    const nonHomoSapensNodeIds = (() => {
        switch (dbName) {
            case "reactome":
                return props.elements
                    .filter(e => e.group() === "nodes" && e.data().type === "neighbor")
                    .map(e => e.data())
                    .filter(e => e.properties.speciesName && e.properties.speciesName !== "Homo sapiens")
                    .map(e => e.id);
            default:
                return [];
        }

    })();

    return <Menu>
        <EdgeColorContextMenu edgePropertyTypes={edgePropertyTypes} cy={props.cy} />
        {publicationNodeIds.length !== 0 &&
            <MenuItem text="Hide publications" onClick={() =>
                dispatch(actions.hideNodes(publicationNodeIds))} />}
        {nonHomoSapensNodeIds.length !== 0 &&
            <MenuItem text="Hide non Homo sapiens" onClick={() =>
                dispatch(actions.hideNodes(nonHomoSapensNodeIds))} />}
    </Menu>;
}

const ContextMenu = React.forwardRef((props, ref) => {
    const BaseContextMenu = () => {
        const [data, setData] = React.useState({});
        props.setMenuData.current = setData;

        return <BPContextMenu
            popoverProps={{
                modifiers: {
                    computeStyle: {
                        gpuAcceleration: false,
                    }
                }
            }}
            content={() => {
                switch (data.group) {
                    case "nodes":
                        return <NodeContextMenu cy={props.cy} data={data.data} />;
                    case "edges":
                        return <EdgeContextMenu cy={props.cy} data={data.data} />;
                    default:
                        return <BackgroundContextMenu cy={props.cy} elements={data.data} />;
                }
            }}
        >
            {(ctxMenuProps) => <div
                className={classNames("my-context-menu-target", ctxMenuProps.className)}
                onContextMenu={ctxMenuProps.onContextMenu}
                ref={ref}
            >
                {ctxMenuProps.popover}
                {ctxMenuProps.children}
            </div>}
        </BPContextMenu>;
    }

    switch (props.data) {
        default: return <BaseContextMenu>
        </BaseContextMenu>;
    }
});

export default ContextMenu;

