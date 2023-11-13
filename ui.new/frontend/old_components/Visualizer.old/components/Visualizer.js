import React from 'react';
import CytoscapeCanvas from './CytoscapeCanvas';
import ContextMenu from './ContextMenu';
import { VisualizerContext } from '../context';


const Visualizer = (props) => {
    const visualizer = React.useContext(VisualizerContext);
    const layout = React.useRef();
    const portalData = React.useRef({});

    const {
        currentElements,
        interactionDisabled,
        setInteractionDisabled,
        fitRequired,
    } = props;

    return (<>
        <CytoscapeCanvas
            {...{
                layout,
                portalData,
                currentElements,
                interactionDisabled,
                setInteractionDisabled,
                fitRequired,
            }}
        />
        <ContextMenu
            ref={visualizer.portalRef}
            data={portalData}
        />
    </>
    );
};

export default Visualizer;

