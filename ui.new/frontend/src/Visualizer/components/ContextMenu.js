import React from 'react';
import classNames from "classnames";

import { ContextMenu as BPContextMenu, Menu, MenuItem } from '@blueprintjs/core';
import { useDispatch, useSelector } from "react-redux";
import { Icon } from "@blueprintjs/core";

import * as actions from "../actions";
import * as appActions from '../../App/actions';
import * as thunks from "../../App/thunks"

const NodeContextMenu = (props) => {
    const dispatch = useDispatch();
    const selectedNodes = useSelector(state => state.selectedNodes);
    const dbName = useSelector(state => state.dbName);
    const layouts = useSelector(state => state.visualizer.layouts);
    const isSelected = selectedNodes[props.data.id] !== undefined;
    const nodeProps = props.data.properties;

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
                ? <MenuItem
                    text="Hide"
                    icon="graph-remove"
                    onClick={() =>
                        dispatch(thunks.getNodes(dbName, [props.data.id]))
                            .then(res =>
                                dispatch(appActions.unselectNode(res[props.data.id])))}
                />

                // Neighbors nodes only
                : <MenuItem text="Hide"
                    onClick={() => dispatch(actions.hideNode(props.data.id))}
                />}
        </Menu>
    );
};

const BackgroundContextMenu = (props) => {
    const dispatch = useDispatch();
    const dbName = useSelector(state => state.dbName);
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

        return <BPContextMenu content={() => {
            switch (data.group) {
                case "nodes":
                    return <NodeContextMenu cy={props.cy} data={data.data} />;
                case "edges":
                    return <></>;
                default:
                    return <BackgroundContextMenu cy={props.cy} elements={data.data} />;
            }
        }}>
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

