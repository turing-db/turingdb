import React from "react";

export interface useValueProps<T> {
  getCanvasValue: () => T;
  setCanvasValue: (v: T) => void;
}

export function useWidgetValue<T>(props: useValueProps<T>) {
  const [released, setReleased] = React.useState<boolean>(false);
  const [v, setV] = React.useState<T>(props.getCanvasValue());

  React.useEffect(() => {
    if (!released) return;
    if (released) setReleased(false);
    if (v !== props.getCanvasValue()) props.setCanvasValue(v);
  }, [props, released, v]);

  return {
    value: v,
    onRelease: () => setReleased(true),
    onChange: (v: T) => setV(v),
  };
}
