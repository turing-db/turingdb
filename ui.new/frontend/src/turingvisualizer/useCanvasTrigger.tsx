import React from "react";
import { useVisualizerContext } from "./context";

interface UseCanvasTriggerArgs {
  category: string;
  name: string;
  callback: () => void;
  core?: boolean;
}
export const useCanvasTrigger = ({
  category,
  name,
  callback = () => {},
  core = false,
}: UseCanvasTriggerArgs) => {
  const vis = useVisualizerContext();

  React.useEffect(() => {
    core
      ? (vis.triggers()![category].secondary[name] = () => callback())
      : (vis.triggers()![category].core[name] = () => callback());
  }, [callback, name, category, vis, core]);
};
