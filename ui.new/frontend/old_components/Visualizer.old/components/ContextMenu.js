import React from 'react';
import classNames from "classnames";

import { ContextMenu as BPContextMenu, Menu, MenuDivider, MenuItem } from '@blueprintjs/core';
import { useDispatch, useSelector } from "react-redux";
import { Icon } from "@blueprintjs/core";

import * as appActions from '../../App/actions';
import * as thunks from "../../App/thunks";
import { colorMenuItems } from '../colors';
import { VisualizerContext } from '../context';

const NodeContextMenu = (props) => {
    const dispatch = useDispatch();
    const selectedNodes = useSelector(state => state.selectedNodes);
    const dbName = useSelector(state => state.dbName);
    const visualizer = React.useContext(VisualizerContext);
    const isSelected = selectedNodes[props.data.turingId] !== undefined;
    const nodeProps = props.data.properties;

    const hideNodes = React.useCallback((cy) => {
        const nodes = cy.nodes().filter(e => e.selected());
        const selectedNodes = nodes.filter(e => e.data().type === "selected");
        const neighborNodes = nodes.filter(e => e.data().type === "neighbor");
        const selectedNodeIds = selectedNodes.map(n => n.id());
        const neighborNodeIds = neighborNodes.map(n => n.id());

        selectedNodeIds.length !== 0 && dispatch(thunks.getNodes(dbName, selectedNodeIds))
            .then(res => Object.values(res).forEach(n => dispatch(appActions.unselectNode(n))));
        neighborNodeIds.length !== 0 && visualizer.state.hiddenNodeIds.hideNodes(neighborNodeIds);

    }, [dbName, dispatch, visualizer.state.hiddenNodeIds]);

    const VerticalLineIcon = () => <Icon
        icon="layout-linear"
        style={{ rotate: "90deg" }}
    />;

    return (
        <Menu>
            <NodeColorContextMenu nodePropertyTypes={props.nodePropertyTypes} cy={visualizer.cy()} />
            <MenuItem
                icon="layout"
                text="Set layout..."
            >
                <MenuItem
                    icon=<VerticalLineIcon />
                    text="Vertical line"
                    onClick={() => {
                        const eles = visualizer.cy().filter(e => e.selected());
                        if (eles.length === 0) return;

                        visualizer.state.layouts.addLayout(
                            {
                                name: 'preset',
                                positions: {
                                    ...Object.fromEntries(eles.map((e, i) =>
                                        [e.id(), { x: visualizer.state.layouts.layouts.layoutCount * 50.0, y: i * 40.0 }]
                                    ))
                                }
                            },
                            eles.map(e => e.id())
                        );
                    }}
                />
                <MenuItem
                    icon="layout-auto"
                    text="Auto"
                    onClick={() => {
                        const eles = visualizer.cy().filter(e => e.selected());
                        if (eles.length === 0) return;
                        visualizer.state.layouts.setDefaultLayout(eles.map(e => e.id()));
                    }}
                />
            </MenuItem>
            <MenuItem text="Select all..." icon="select">
                <MenuItem icon="property" text="by same property...">
                    {Object.keys(nodeProps).map(propName =>
                        <MenuItem key={propName} icon="label" text={propName}
                            onClick={() =>
                                visualizer.cy()
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
                        visualizer.cy()
                            .filter(e =>
                                visualizer.state.layouts.layouts.mapping[e.id()]
                                === visualizer.state.layouts.layouts.mapping[props.data.id])
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
                        onClick={() => hideNodes(visualizer.cy())}
                    />
                    <MenuItem
                        text="Collapse neighbors"
                        icon="collapse-all"
                        onClick={() => {
                            const node = visualizer.cy().$id(props.data.id);
                            const nodeIds = node
                                .neighborhood()
                                .nodes()
                                .map(n => n.id())
                                .filter(id => !selectedNodes[id]);
                            visualizer.state.hiddenNodeIds.hideNodes(nodeIds)
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
                        onClick={() => hideNodes(visualizer.cy())}
                    />
                </>}
        </Menu>
    );
};

const EdgeColorContextMenu = (props) => {
    const visualizer = React.useContext(VisualizerContext);

    return (
        <Menu>
            <MenuItem text="Set edge colors..." icon="one-to-one">

                <MenuDivider title="General" />
                <colorMenuItems.EdgeColorContextMenu_None
                    cy={visualizer.cy()} />

                <colorMenuItems.EdgeColorContextMenu_Preset
                    cy={visualizer.cy()} />

                <MenuDivider title="Gradient" />
                <colorMenuItems.EdgeColorContextMenu_GradientProperty
                    cy={visualizer.cy()}
                    propertyTypes={props.edgePropertyTypes} />

                <MenuDivider title="Discrete" />
                <colorMenuItems.EdgeColorContextMenu_DiscreteProperty
                    cy={visualizer.cy()}
                    propertyTypes={props.edgePropertyTypes} />
                <colorMenuItems.EdgeColorContextMenu_EdgeType
                    cy={visualizer.cy()} />

            </MenuItem>
        </Menu>
    );
}

const NodeColorContextMenu = (props) => {
    const visualizer = React.useContext(VisualizerContext);

    return (
        <Menu>
            <MenuItem text="Set node colors..." icon="graph">

                <MenuDivider title="General" />
                <colorMenuItems.NodeColorContextMenu_None
                    cy={visualizer.cy()} />

                <colorMenuItems.NodeColorContextMenu_Preset
                    cy={visualizer.cy()} />

                <MenuDivider title="Gradient" />
                <colorMenuItems.NodeColorContextMenu_GradientProperty
                    propertyTypes={props.nodePropertyTypes}
                    cy={visualizer.cy()} />

                <MenuDivider title="Discrete" />
                <colorMenuItems.NodeColorContextMenu_DiscreteProperty
                    propertyTypes={props.nodePropertyTypes}
                    cy={visualizer.cy()} />
                <colorMenuItems.NodeColorContextMenu_NodeType
                    cy={visualizer.cy()} />

            </MenuItem>
        </Menu>
    );
}

const EdgeContextMenu = (props) => {
    const visualizer = React.useContext(VisualizerContext);

    return (
        <Menu>
            <EdgeColorContextMenu edgePropertyTypes={props.edgePropertyTypes} cy={visualizer.cy()} />
        </Menu>
    );

};

const getPropertyTypesFromElements = (elements) => elements
    .map(e => Object.keys(e.data().properties))
    .flat()
    .filter((p, i, arr) => arr.indexOf(p) === i);

const BackgroundContextMenu = (props) => {
    const visualizer = React.useContext(VisualizerContext);

    return <Menu>
        <MenuDivider title="colors" />
        <EdgeColorContextMenu edgePropertyTypes={props.edgePropertyTypes} cy={visualizer.cy()} />
        <NodeColorContextMenu nodePropertyTypes={props.nodePropertyTypes} cy={visualizer.cy()} />
    </Menu>;
}

const ContextMenu = React.forwardRef((props, ref) => {
    const visualizer = React.useContext(VisualizerContext);

    const BaseContextMenu = () => {
        const [data, setData] = React.useState({});
        visualizer.setMenuData.current = setData;
        const edgePropertyTypes = getPropertyTypesFromElements(visualizer.cy()?.edges() || []);
        const nodePropertyTypes = getPropertyTypesFromElements(visualizer.cy()?.nodes() || []);

        return <BPContextMenu
            style={{ zIndex: 9999999999 }}
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
                            cy={visualizer.cy()}
                            data={data.data}
                            nodePropertyTypes={nodePropertyTypes}
                        />;
                    case "edges":
                        return <EdgeContextMenu
                            cy={visualizer.cy()}
                            data={data.data}
                            edgePropertyTypes={edgePropertyTypes}
                        />;
                    default:
                        return <BackgroundContextMenu
                            cy={visualizer.cy()}
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

