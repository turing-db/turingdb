import React from 'react';
import classNames from "classnames";

import { ContextMenu as BPContextMenu, Menu, MenuDivider, MenuItem } from '@blueprintjs/core';
import { useDispatch, useSelector } from "react-redux";
import { Icon } from "@blueprintjs/core";

import * as actions from "../actions";
import * as appActions from '../../App/actions';
import * as thunks from "../../App/thunks";
import { colorMenuItems } from '../colors';

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
        style={{ rotate: "90deg" }}
    />;

    return (
        <Menu>
            <NodeColorContextMenu nodePropertyTypes={props.nodePropertyTypes} cy={props.cy} />
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
            <MenuItem text="Select all..." icon="select">
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
                        intent="danger"
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
                        text="Add to network"
                        intent="success"
                        icon="add"
                        onClick={() => {
                            dispatch(thunks.getNodes(dbName, [props.data.id]))
                                .then(res => dispatch(appActions.selectNode(res[props.data.id])))
                        }}
                    />
                    <MenuItem
                        text="Hide"
                        intent="danger"
                        icon="graph-remove"
                        onClick={() => hideNodes(props.cy.current)}
                    />
                </>}
        </Menu>
    );
};

const EdgeColorContextMenu = (props) => {
    return (
        <Menu>
            <MenuItem text="Set edge colors..." icon="one-to-one">

                <MenuDivider title="General" />
                <colorMenuItems.EdgeColorContextMenu_None
                    cy={props.cy.current} />

                <colorMenuItems.EdgeColorContextMenu_Preset
                    cy={props.cy.current} />

                <MenuDivider title="Gradient" />
                <colorMenuItems.EdgeColorContextMenu_GradientProperty
                    cy={props.cy.current}
                    propertyTypes={props.edgePropertyTypes} />

                <MenuDivider title="Discrete" />
                <colorMenuItems.EdgeColorContextMenu_DiscreteProperty
                    cy={props.cy.current}
                    propertyTypes={props.edgePropertyTypes} />
                <colorMenuItems.EdgeColorContextMenu_EdgeType
                    cy={props.cy.current} />

            </MenuItem>
        </Menu>
    );
}

const NodeColorContextMenu = (props) => {
    return (
        <Menu>
            <MenuItem text="Set node colors..." icon="graph">

                <MenuDivider title="General" />
                <colorMenuItems.NodeColorContextMenu_None
                    cy={props.cy.current} />

                <colorMenuItems.NodeColorContextMenu_Preset
                    cy={props.cy.current} />

                <MenuDivider title="Gradient" />
                <colorMenuItems.NodeColorContextMenu_GradientProperty
                    propertyTypes={props.nodePropertyTypes}
                    cy={props.cy.current} />

                <MenuDivider title="Discrete" />
                <colorMenuItems.NodeColorContextMenu_DiscreteProperty
                    propertyTypes={props.nodePropertyTypes}
                    cy={props.cy.current} />
                <colorMenuItems.NodeColorContextMenu_NodeType
                    cy={props.cy.current} />

            </MenuItem>
        </Menu>
    );
}

const EdgeContextMenu = (props) => {
    return (
        <Menu>
            <EdgeColorContextMenu edgePropertyTypes={props.edgePropertyTypes} cy={props.cy} />
        </Menu>
    );

};

const getPropertyTypesFromElements = (elements) => elements
    .map(e => Object.keys(e.data().properties))
    .flat()
    .filter((p, i, arr) => arr.indexOf(p) === i);

const BackgroundContextMenu = (props) => {
    return <Menu>
        <MenuDivider title="colors" />
        <EdgeColorContextMenu edgePropertyTypes={props.edgePropertyTypes} cy={props.cy} />
        <NodeColorContextMenu nodePropertyTypes={props.nodePropertyTypes} cy={props.cy} />
    </Menu>;
}

const ContextMenu = React.forwardRef((props, ref) => {
    const BaseContextMenu = () => {
        const [data, setData] = React.useState({});
        props.setMenuData.current = setData;
        const edgePropertyTypes = getPropertyTypesFromElements(props.cy.current?.edges() || []);
        const nodePropertyTypes = getPropertyTypesFromElements(props.cy.current?.nodes() || []);

        return <BPContextMenu
            style={{zIndex: 9999999999}}
            popoverProps={{
               // modifiers: {
               //     computeStyle: {
               //         gpuAcceleration: true, // can cause bugs with icon rendering on chrome
               //     }
               // }
            }}
            content={() => {
                switch (data.group) {
                    case "nodes":
                        return <NodeContextMenu
                            cy={props.cy}
                            data={data.data}
                            nodePropertyTypes={nodePropertyTypes}
                        />;
                    case "edges":
                        return <EdgeContextMenu
                            cy={props.cy}
                            data={data.data}
                            edgePropertyTypes={edgePropertyTypes}
                        />;
                    default:
                        return <BackgroundContextMenu
                            cy={props.cy}
                            elements={data.data}
                            nodePropertyTypes={nodePropertyTypes}
                            edgePropertyTypes={edgePropertyTypes}
                        />;
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

