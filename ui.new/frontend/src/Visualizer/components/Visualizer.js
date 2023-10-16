import React from 'react';
import CytoscapeCanvas from './CytoscapeCanvas';
import ContextMenu from './ContextMenu';
import * as utils from '../utils';
import * as actions from '../actions';
import { useDispatch, useSelector } from 'react-redux';

const Visualizer = (props) => {
    const dispatch = useDispatch();
    const layouts = useSelector(state => state.visualizer.layouts);
    const cyLayout = useSelector(state => state.visualizer.cyLayout);

    const layout = React.useRef();
    const cy = React.useRef();
    const portalRef = React.useRef();
    const portalData = React.useRef({});
    const setMenuData = React.useRef(() => { });

    const {
        currentElements,
        interactionDisabled,
        setInteractionDisabled,
        fitRequired,
    } = props;

    const visualizer = {
        callbacks: {
            showCellCellInteraction: () => utils.showCellCellInteraction(dispatch, cy.current, fitRequired),
            applyLayouts: () => utils.applyLayouts(cy.current, layouts, cyLayout),
            fit: () => cy.current.fit(),
            clearHiddenNodes: () => dispatch(actions.clearHiddenNodes()),
            setEdgeLabel: (v) => v && dispatch(actions.setEdgeLabel(v))
        },
    }
    // Settings the callbacks reference to be able to call them from parents
    props.visualizer.current = visualizer;

    return <>
        <CytoscapeCanvas
            {...{
                cy,
                layout,
                portalData,
                portalRef,
                currentElements,
                interactionDisabled,
                setInteractionDisabled,
                fitRequired,
                setMenuData,
            }}
        />
        <ContextMenu
            ref={portalRef}
            data={portalData}
            cy={cy}
            setMenuData={setMenuData}
        />
    </>;
};

export default Visualizer;

