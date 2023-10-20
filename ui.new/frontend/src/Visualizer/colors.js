import { useDispatch } from 'react-redux';
import { COLOR_MODES, DISCRETE_COLORS } from './constants'
import { MenuItem } from '@blueprintjs/core';
import * as actions from './actions'
import { Icon } from "@blueprintjs/core";

const colorGradient = (min, max, value) => {
    if (!value) return undefined;

    const extent = max - min;
    const r = parseInt(255 * (value - min) / extent);
    const g = 255 - parseInt(255 * (value - min) / extent);
    return `rgb(${r},${g},0)`;
}

const getColorMakers = (
    elementTypes
) => ({
    [COLOR_MODES.None]: () => undefined,
    [COLOR_MODES.ElementType]: (elData) => {
        const colorIndex = elementTypes.indexOf(elData.node_type_name)
            % DISCRETE_COLORS.length;
        const color = DISCRETE_COLORS[colorIndex];
        return color;
    },
    [COLOR_MODES.NodeType]: (elData) => {
        const colorIndex = elementTypes.indexOf(elData.node_type_name)
            % DISCRETE_COLORS.length;
        const color = DISCRETE_COLORS[colorIndex];
        return color;
    },
    [COLOR_MODES.EdgeType]: (elData) => {
        const colorIndex = elementTypes.indexOf(elData.edge_type_name)
            % DISCRETE_COLORS.length;
        const color = DISCRETE_COLORS[colorIndex];
        return color;
    },
    [COLOR_MODES.GradientProperty]: (elData, colorSet) => {
        const data = colorSet.data;
        return colorGradient(data.min, data.max, data.propValues[elData.id]);
    },
    [COLOR_MODES.DiscreteProperty]: (elData, colorSet) => {
        const colorIndex = colorSet.data.propValues.indexOf(
            elData.properties[colorSet.data.propName]
        ) % DISCRETE_COLORS.length;
        const color = DISCRETE_COLORS[colorIndex];
        return color;
    },
    [COLOR_MODES.Preset]: (_elData, colorSet) => {
        return colorSet.data.color;
    }
});

export const useElementColorMakers = (rawElements) => {
    const edges = rawElements.data ? rawElements.data.filter(el => el.group === "edges") : [];
    const nodes = rawElements.data ? rawElements.data.filter(el => el.group === "nodes") : [];

    const edgeTypes = [...new Set(edges.map(e => e.data.edge_type_name))];
    const nodeTypes = [...new Set(nodes.map(e => e.data.node_type_name))];

    const nodeColorMakers = getColorMakers(nodeTypes);
    const edgeColorMakers = getColorMakers(edgeTypes);

    return { nodeColorMakers, edgeColorMakers };
}

const ColorMenuItem = ({
    cy,
    text, // text that appears on the menu item
    icon, // icon that appears on the menu item
    elGroup, // "nodes" | "edges"
    colorMode, // ex: COLOR_MODES.NodeType
    colorData, // Data required for the color mode, ex { propName, propValues }
}) => {
    const dispatch = useDispatch();
    return <MenuItem text={text} icon={icon}
        onClick={() => {
            const selectedElements = cy[elGroup]().filter(n => n.selected());
            const elements = selectedElements.length === 0
                ? cy[elGroup]()
                : selectedElements;

            switch (colorMode) {
                case COLOR_MODES.DiscreteProperty: {
                    colorData = {
                        ...colorData,
                        propValues: [...new Set(cy[elGroup]().map(e => e.data().properties[colorData.propName]))]
                    }
                    break;
                }

                case COLOR_MODES.GradientProperty: {
                    colorData = {
                        ...colorData,
                        propValues: Object.fromEntries(elements
                            .filter(e => !isNaN(parseFloat(e.data().properties[colorData.propName])))
                            .map(e => [e.id(), parseFloat(e.data().properties[colorData.propName])])
                        )
                    };
                    colorData.min = Math.min.apply(Math, Object.values(colorData.propValues));
                    colorData.max = Math.max.apply(Math, Object.values(colorData.propValues));
                    break;
                }

                default: break;
            }

            const setColorMode = elGroup === "nodes"
                ? actions.setNodeColorMode
                : actions.setEdgeColorMode;

            dispatch(setColorMode(colorMode, selectedElements.map(n => n.id()), colorData));
        }}
    />;
}

export const colorMenuItems = {
    NodeColorContextMenu_None: ({ cy }) =>
        <ColorMenuItem
            cy={cy}
            text="None"
            icon="cross"
            elGroup="nodes"
            colorMode={COLOR_MODES.None}
            colorData={{}}
            key="node-none"
        />,
    EdgeColorContextMenu_None: ({ cy }) =>
        <ColorMenuItem
            cy={cy}
            text="None"
            icon="cross"
            elGroup="edges"
            colorMode={COLOR_MODES.None}
            colorData={{}}
            key="edge-none"
        />,
    NodeColorContextMenu_Preset: ({ cy }) =>
        <MenuItem
            text="Preset"
            icon="edit"
        >
            {DISCRETE_COLORS.map(col =>
                <ColorMenuItem
                    cy={cy}
                    text={col}
                    icon={<Icon icon="symbol-square" color={col} />}
                    elGroup="nodes"
                    colorMode={COLOR_MODES.Preset}
                    colorData={{ color: col }}
                    key={"node-preset-" + col}
                />
            )}
        </MenuItem>,
    EdgeColorContextMenu_Preset: ({ cy }) =>
        <MenuItem
            text="Preset"
            icon="edit"
        >
            {DISCRETE_COLORS.map(col =>
                <ColorMenuItem
                    cy={cy}
                    text={col}
                    icon={<Icon icon="symbol-square" color={col} />}
                    elGroup="edges"
                    colorMode={COLOR_MODES.Preset}
                    colorData={{ color: col }}
                    key={"edge-preset-" + col}
                />
            )}
        </MenuItem>,
    NodeColorContextMenu_NodeType: ({ cy }) =>
        <ColorMenuItem
            cy={cy}
            text="Based on node type"
            icon="graph"
            elGroup="nodes"
            colorMode={COLOR_MODES.NodeType}
            colorData={{}}
            key="node-type"
        />,
    EdgeColorContextMenu_EdgeType: ({ cy }) =>
        <ColorMenuItem
            cy={cy}
            text="Based on edge type"
            icon="one-to-one"
            elGroup="edges"
            colorMode={COLOR_MODES.EdgeType}
            colorData={{}}
            key="edge-type"
        />,
    NodeColorContextMenu_GradientProperty: ({ cy, propertyTypes }) =>
        <MenuItem text="Based on property..." icon="property" key="node-gradient-property">
            {propertyTypes.map(propName =>
                <ColorMenuItem
                    cy={cy}
                    text={propName}
                    icon="label"
                    elGroup="nodes"
                    colorMode={COLOR_MODES.GradientProperty}
                    colorData={{ propName }}
                    key={"node-gradient-" + propName}
                />
            )}
        </MenuItem>,
    EdgeColorContextMenu_GradientProperty: ({ cy, propertyTypes }) =>
        <MenuItem text="Based on property..." icon="property" key="edge-gradient-property">
            {propertyTypes.map(propName =>
                <ColorMenuItem
                    cy={cy}
                    text={propName}
                    icon="label"
                    elGroup="edges"
                    colorMode={COLOR_MODES.GradientProperty}
                    colorData={{ propName }}
                    key={"edge-gradient-" + propName}
                />
            )}
        </MenuItem>,
    NodeColorContextMenu_DiscreteProperty: ({ cy, propertyTypes }) =>
        <MenuItem text="Based on property..." icon="property" key="node-discrete-property">
            {propertyTypes.map(propName =>
                <ColorMenuItem
                    cy={cy}
                    text={propName}
                    icon="label"
                    elGroup="nodes"
                    colorMode={COLOR_MODES.DiscreteProperty}
                    colorData={{ propName }}
                    key={"node-discrete-" + propName}
                />
            )}
        </MenuItem>,
    EdgeColorContextMenu_DiscreteProperty: ({ cy, propertyTypes }) =>
        <MenuItem text="Based on property..." icon="property" key="edge-discrete-property">
            {propertyTypes.map(propName =>
                <ColorMenuItem
                    cy={cy}
                    text={propName}
                    icon="label"
                    elGroup="edges"
                    colorMode={COLOR_MODES.DiscreteProperty}
                    colorData={{ propName }}
                    key={"edge-discrete-" + propName}
                />
            )}
        </MenuItem>,

}
