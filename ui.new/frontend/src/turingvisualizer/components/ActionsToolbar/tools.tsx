import { DialogProps } from "@blueprintjs/core";
import React from "react";

export const dialogParams: Partial<DialogProps> = {
  canOutsideClickClose: true,
  usePortal: true,
  canEscapeKeyClose: true,
  className: "min-w-[80%]",
};

export const ttParams = {
  hoverCloseDelay: 40,
  hoverOpenDelay: 400,
  compact: true,
  openOnTargetFocus: false,
};

export function useDialog() {
  const [isOpen, setIsOpen] = React.useState<boolean>(false);
  const toggle = () => setIsOpen(!isOpen);
  const open = () => setIsOpen(true);
  const close = () => setIsOpen(false);

  return {
    isOpen,
    setIsOpen,

    toggle,
    open,
    close,
  };
}

export type DialogInfos = ReturnType<typeof useDialog>;

export function useDefinedState<T>(initValue: T) {
  const [value, set] = React.useState<T>(initValue);
  const ready = React.useRef<boolean>(false);

  return {
    value, // The actual value
    set, // Sets the value
    ready, // Holds true if value was initialized
    setOnce: (v: T) => {
      // Sets the value only the first time it is called
      if (!ready.current) {
        ready.current = true;
        set(v);
      }
    },
  };
}
