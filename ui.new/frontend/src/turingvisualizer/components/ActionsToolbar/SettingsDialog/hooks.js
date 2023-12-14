import React from "react";
export function useWidgetValue(props) {
    const [released, setReleased] = React.useState(false);
    const [v, setV] = React.useState(props.getCanvasValue());
    React.useEffect(() => {
        if (!released)
            return;
        if (released)
            setReleased(false);
        if (v !== props.getCanvasValue())
            props.setCanvasValue(v);
    }, [props, released, v]);
    return {
        value: v,
        onRelease: () => setReleased(true),
        onChange: (v) => setV(v),
    };
}
