//Core
import React from "react";

// Turing
import { useVisualizerContext } from ".";
import { Alert, Dialog } from "@blueprintjs/core";

export const useDialog = ({
  name,
  content,
  title,
  isAlert = false,
  alertProps,
}) => {
  const vis = useVisualizerContext();
  const [open, setOpen] = React.useState(false);
  const dataRef = React.useRef(null);

  const data = () => dataRef.current;

  const toggle = React.useCallback(
    (data) => {
      dataRef.current = data;
      setOpen(!open);
    },
    [open]
  );

  const Content = React.useCallback(
    () =>
      isAlert ? (
        <Alert
          key={name + "-alert"}
          {...alertProps}
          className={vis.state().themeMode === "dark" ? "bp5-dark" : "bp5"}
          usePortal={false}
          onConfirm={() => alertProps.onConfirm({ data, toggle, open })}
          onCancel={() => alertProps.onCancel({ data, toggle, open })}
          isOpen={open}>
          {content(toggle, data, open)}
        </Alert>
      ) : (
        <Dialog
          key={name + "-dialog"}
          isOpen={open}
          title={title}
          usePortal={false}
          onClose={toggle}
          canOutsideClickCancel={true}
          className={vis.state().themeMode === "light" ? "" : "bp5-dark"}
          style={{
            position: "relative",
          }}>
          {content(toggle, data, open)}
        </Dialog>
      ),
    [vis, title, isAlert, alertProps, content, open, toggle, name]
  );

  vis.dialogs()[name] = { toggle, Content, open };

  return { open, setOpen, toggle, data };
};
