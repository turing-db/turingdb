# Turing visualizer component

## Setup

In order to work, the visualizer component needs

- React
- blueprintjs
- react-query
- cytoscape
- cytoscape-cola

For example, this boilerplate code renders the TestApp

```JSX
// React
import React from 'react';
import ReactDOM from 'react-dom/client';

// Blueprintjs dependencies
import 'normalize.css/normalize.css'
import '@fontsource/public-sans';
import '@blueprintjs/icons/lib/css/blueprint-icons.css'
import '@blueprintjs/core/lib/css/blueprint.css'

// The visualizer needs ReactQuery
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Example application content
import { TestApp } from 'turingvisualizer/components'

const queryClient = new QueryClient();
const root = ReactDOM.createRoot(document.getElementById('root'));

root.render(
    <QueryClientProvider client={queryClient}>
        <TestApp />
    </QueryClientProvider>
);
```

## Inside a custom application

In order to render the canvas,
you need to wrap your custom component with a `VisualizerContextProvider`,
with four parameters: `themeMode`, `dbName` (that you can set to `"reactome"` for the moment),
a `containerId`.
The containerId is the unique identifier for the html container that
cytoscape uses to render the canvas (each canvas should have a unique id).

In the example below, the `<AppContent />` component is the wrapper around the visualizer, and thus, controls its lifetime. The visualizer stays alive as long as `<AppContent />` stays mounted. In your application, you might have different tabs `<VisualizerTab />` with a unique canvas in each one, in which case, you need to keep the tab alive when hidden if you want to restore the state of the canvas when switching tabs.

```JSX
<div>
    <VisualizerContextProviders
        themeMode="light"
        dbName="pole"
        containerId="cy1"
    >
        <div style={{
            display: "flex",
            flexDirection: "column",
            height: "100vh",
        }}>
            <AppContent />
        </div>
    </VisualizerContextProvider>
</div>
```

In your component (`<AppContent/>` in this example), render the visualizer with:

```JSX
<Visualizer
    canvas={<Canvas />}
    contextMenu={<TuringContextMenu/>}
    cyStyle={style}
>
    {/* children rendered on top of the canvas (such as a toolbar) */}
</Visualizer>
```

You can replace `<TuringContextMenu/>` with your own context menu. Your component has access the visualizer state using the `useVisualizerContext` hook:

```JSX
const vis = useVisualizerContext();
```

## Trigger actions on Canvas state change

If you need to react to state changes in the canvas, you can register triggers. For example, when the JSON 'elements' object (that cytoscape uses to render the graph) changes, you might trigger changes to your component using the `useCanvasTrigger`. For example, say that we want to increase a count everytime the elements array changes:

```JSX
const MyComponent = () => {
    const vis = useVisualizerContext();
    const [count, setCount] = React.useState(0);

    useCanvasTrigger({
        // the callback will be executed when the cytoscape elements change
        category: "elements",
        name: "testapp-onElementChanged" // unique key to register your callback

        callback: () => {
            setCount(count + 1);
        }
    })

    return (
        <p>Elements changed {count} times</p>
        <Visualizer
            canvas={<Canvas />} // Mandatory
            contextMenu={<TuringContextMenu/>} // or your own context menu
            cyStyle={style} // style is imported from @turingvisualizer
        >
        </Visualizer>
    );
}
```

Available trigger categories:

- `elements`: The JSON dict that cytoscape uses to render stuff
- `selectedNodeIds`: IDs of the selected nodes (the main nodes, which don't include their neighbors)
- `inspectedNode`: Node currently inspected in the canvas
- `filters`: The node filters
- `layouts`: The layouts object (using right click -> setLayout will execute callbacks stored here).
- `nodeLabel`: The node label displayed on the canvas
- `edgeLabel`: The edge label displayed on the canvas
- `hiddenNodes`: The nodes hidden on the canvas

## Modify the canvas state (does not re-render the canvas)

It is possible to trigger state changes on the canvas using callbacks accessible using `vis.callbacks()`.
For now, here are the possible actions:

```JSX
vis.callbacks().setSelectedNodeIds(ids)
// Sets the selected nodes using an array of turing ids


vis.callbacks().setInspectedNode(inspectedNode: GraphNodeData);
// sets the node currently inspected

vis.callbacks().setDefaultCyLayout(cyLayout)
// Modifies the default cytoscape layout

vis.callbacks().addLayout(definition, nodeIds)
// Adds a new layout (defined by 'definition') and apply
// it to the node given by nodeIds array

vis.callbacks().updateLayout(layoutId, patch)
// modifies the layout

vis.callbacks().resetLayout(nodeIds)
// Sets the default layout to the give nodes

vis.callbacks().setNodeColorMode(mode, elementIds, data)
vis.callbacks().setEdgeColorMode(mode, elementIds, data)
// allows to set node and edge colors. The API is quite complex for now
// Check the TuringContextMenu example for how to use it

vis.callbacks().setNodeLabel(label)
vis.callbacks().setEdgeLabel(label)
// Modifies the labels displayed on the canvas. For example,
// vis.callbacks().setNodeLabel("displayName") uses the "displayName"
// property to set the label

vis.callbacks().hideNode(node) // node = n.data
vis.callbacks().hideNodes(nodes) // nodes = { n.data.turing_id, n.data }
vis.callbacks().showNodes(nodeIds) // nodeIds = [ n.data.turing_id ]
// hides nodes on the canvas

vis.callbacks().requestLayoutRun(request)
// runs all layouts

vis.callbacks().requestLayoutFit(request)
// fit the canvas to the window

vis.callbacks().setFilters(filters)
// sets the filters (see below)

vis.callbacks().centerOnDoubleClicked(value: boolean)
// Sets wether to center the canvas onto the double clicked node or not
```

For example, set filters with:

```JSX
vis.callbacks().setFilters({
    ...vis.state().filters,
    hidePublications: true
});

The available filters are:
- hidePublications
- hideCompartments
- hideSpecies
- hideDatabaseReferences
- showOnlyHomoSapiens
```

## ContextMenu

The context menu can entirely be changed if need, just change `<TuringContextMenu/>` in visualizer to use your own component. Your component should be defined like so:

```JSX
import { ContextMenu } from '@turingvisualizer';

const MyOwnCustomMenu = () => {
    useContextMenuData(); // !important
    const vis = useVisualizerContext();

    // Access context menu data (such as the data of the clicked node or edge)
    const menuData = vis.contextMenuData();

    return (
        <ContextMenu>
            {/* Your Menu items here */}
        </ContextMenu>
    );
}

const MyComponent = () => {
    return (
        <Visualizer
            canvas={<Canvas />}
            contextMenu={<MyOwnCustomMenu/>}
            cyStyle={style}
        >
        </Visualizer>
    );
}
```

## Interact with cytoscape

Interact with cytoscape at all time using the `cy` reference:

```JSX
const vis = useVisualizerContext();
const cy = vis.cy();

const selectedElements = cy.filter(e => e.selected());
const selectedNodes = selectedElements.filter(e => e.group() === "nodes")
const edgeIds = cy.edges().map(e => e.id()); // Cytoscape ids
const turingEdgeIds = cy.edges().map(e => e.data().turing_id) // Turing database ids;
```

## Avoid re-rendering

Sometimes, the component that holds the visualizer might use some external state
and re-render upon changes. For example in our UI prototype, with have a global state (redux) to
store the selectedNodes that we access with:

```JSX
const selectedNodes = useSelector(state => state.selectedNodes);
```

In order to avoid re-render, we can use it as a reference with a custom hook:

```JSX
const useSelectorRef = (property) => {
    const ref = React.useRef();

    useSelector(state => {
        ref.current = state[prop];
        return true; // Avoid re-render when state[prop] changes
    })

    return ref;
}

const selectedNodesRef = useSelectorRef("selectedNodes");

// Example usage
const onClick = (e) => console.log(selectedNodesRef.current);
```

## Custom style

The canvas style is customizable by modifying the cytoscape json stylesheet:

```JSX
import style from 'turingvisualizer/style';

const MyComponent = () => {
    const cyStyle = [
        ...style,
        {
            "selector": "node",
            "style": {
                "font-size": "19px",
            }
        }
    ];

    return (
        <Visualizer
            canvas={<Canvas />}
            contextMenu={<MyOwnCustomMenu/>}
            cyStyle={cyStyle}
        >
        </Visualizer>
    );
}
```

Also, some parameters are definied programmatically using `data()` directives.
You can modify them like so:

```JSX
import { getDefaultCanvasTheme } from 'turingvisualizer/tools';

...
    const theme = getDefaultCanvasTheme();
    theme.dark.nodes.selected.icon = "#FF0000";

    return (
        <Visualizer
            ...
            canvasTheme={theme}
        >
        </Visualizer>
    );

```

## Rest API

It is possible to fetch for nodes and edges outside of the visualizer by using the
fake REST API.

### List nodes

For example using react-query:

```JSX
// import
import { endpoints } from 'turingvisualizer/queries';

const { data, isFetching } = useQuery(

    // Query keys
    ["dev_list_nodes", propertyValue],

    // Actual query
    () => {
        return endpoints["list_nodes"]({
            dbName,

            // The usual filters
            filters: {
                showOnlyHomoSapiens: true,
                hidePublications: true,
                hideCompartments: true,
                hideSpecies: true,
                hideDatabaseReferences: true,
            },

            additionalPropFilterIn: [
                // only nodes that have "APOE-4" in their displayName will
                // be retrieved
                ["displayName", "APOE-4"],
            ],

            additionalPropFilterOut: [
                // For example
                // ["displayName", "cytosol"]
                // nodes that have "cytosol" in their displayName will
                // be excluded
            ]
        });
    });
```

### List edges

```JSX
endpoints["list_edges"]({
        dbName,

        nodeId: 1930248, // List the edges of the node 1930248

        nodeFilters: { // Filter edges according to the other node
            showOnlyHomoSapiens: true,
            hidePublications: true,
            hideCompartments: true,
            hideSpecies: true,
            hideDatabaseReferences: true,
        },

        edgeTypeNames: ["input"], // Show only edges of type "input"

        additionalNodePropFilterIn: [ // Filter edges according to the other node
            ["prop1", "prop1value"],
            ["prop2", "prop2value"],
            ["prop3", "prop3value"],
        ],

        additionalNodePropFilterOut: [
            ["prop1", "prop1value"],
            ["prop2", "prop2value"],
            ["prop3", "prop3value"],
        ],

        edgePropFilterOut: [
            ["prop1", "prop1value"],
            ["prop2", "prop2value"],
            ["prop3", "prop3value"],
        ],

        edgePropFilterIn: [
            []
        ],
    })
```

### Get nodes

This returns the nodes of given ids

```JSX
endpoints["get_nodes"]({
    dbName,
    nodeIds: [1930248, 1930250]
})
```

### Get edges

This returns the edges of given ids

```JSX
endpoints["get_edges"]({
    dbName,
    edgeIds: [1930248, 1930250]
})
```

### List pathways related to a node

```JSX
endpoints["list_pathways"]({
    dbName,
    nodeId: 1930248,
})
```

### Get all the nodes of a pathway

```JSX
endpoints["get_pathway"]({
    dbName,
    nodeId: pathwayNodeID, // One of the node ids returned by
                           // the 'list_pathways' endpoint

    filters: {
        showOnlyHomoSapiens: true,
        hidePublications: true,
        hideCompartments: true,
        hideSpecies: true,
        hideDatabaseReferences: true,
    },
    additionalPropFilterIn: [],
    additionalPropFilterOut: []
})
```

## Cytoscape events

It is possible to hook onto some cytoscape events by modifying the callbacks
in vis.refs.events (don't forget to call the original event at the end of your
callback if your need to retain the original effect):

```JSX
import * as cyEvents from '@turingvisualizer/events';

vis.refs.events.current.onetap = (vis, e) => {
    console.log(e);
    cyEvents.onetap(vis, e);
};

vis.refs.events.current.cxttap = (vis, e) => {
    console.log(e);
    cyEvents.cxttap(vis, e);
};

vis.refs.events.current.dragfreeon = (vis, e) => {
    console.log(e);
    cyEvents.dragfreeon(vis, e);
};

vis.refs.events.current.dbltap = (vis, e) => {
    console.log(e);
    cyEvents.dbltap(vis, e);
};
```

Checkout the `turingvisualizer/events.js` file for examples on how to write event callbacks
