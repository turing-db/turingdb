import React from 'react';
import classNames from "classnames";

import { ContextMenu, Menu, MenuItem } from '@blueprintjs/core';
import { useDispatch, useSelector } from "react-redux";

import * as actions from "../App/actions";
import * as thunks from "../App/thunks"

const NodeContextMenu = (props) => {
    const dispatch = useDispatch();
    const selectedNodes = useSelector((state) => state.selectedNodes);
    const dbName = useSelector((state) => state.dbName);
    const isSelected = selectedNodes[props.data.id] !== undefined;

    if (isSelected)
        return <Menu>
            <MenuItem text="Delete"
                onClick={() =>
                    dispatch(thunks.getNodes(dbName, [props.data.id]))
                        .then(res =>
                            dispatch(actions.unselectNode(res[props.data.id])))}
            />
        </Menu>;

    // Else, it is a neighbor
    return <Menu>
        <MenuItem text="Hide"
            onClick={() => dispatch(actions.hideNode(props.data.id))}
        />
    </Menu>;
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

const CytoscapeContextMenu = React.forwardRef((props, ref) => {
    const BaseContextMenu = () => {
        const [data, setData] = React.useState({});
        props.setMenuData.current = setData;

        return <ContextMenu content={() => {
            switch (data.group) {
                case "nodes":
                    return <NodeContextMenu data={data.data} />;
                case "edges":
                    return <></>;
                default:
                    return <BackgroundContextMenu elements={data.data} />;
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
        </ContextMenu>;
    }

    switch (props.data) {
        default: return <BaseContextMenu>
        </BaseContextMenu>;
    }
});

export default CytoscapeContextMenu;

