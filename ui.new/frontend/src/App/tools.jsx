import React from "react";
import { useSelector } from "react-redux";

export const useSelectorRef = (prop) => {
  const ref = React.useRef(null);

  useSelector((state) => {
    ref.current = state[prop];
    return true;
  });

  return ref;
};
