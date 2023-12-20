import { SearchNodesDialog } from "src/turingvisualizer/components/ActionsToolbar/SearchNodesDialog";
import { useVisualizerContext } from "../context";

export default function DialogContainer() {
  const vis = useVisualizerContext();
  return (
    <>
      <SearchNodesDialog dialog={vis.searchNodesDialog} />
    </>
  );
}
