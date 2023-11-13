// Core
import React from "react";
import { useDispatch, useSelector } from "react-redux";

// @mui/material
import { Box, Button } from "@mui/material";

// Turing
import NodeTypeFilterContainer from "@Components/NodeTypeFilterContainer";
import PropertyFilterContainer from "@Components/PropertyFilterContainer";
import NodeFilterContainer from "@Components/NodeFilterContainer";
import SelectedNodesContainer from "@Components/SelectedNodesContainer";
import BorderedContainer from "@Components/BorderedContainer";
import NodeStack from "@Components/NodeStack";
import { BorderedContainerTitle } from "@Components/BorderedContainer";
import * as actions from "@App/actions";
import * as thunks from "@App/thunks";

export const CommonNode = ({ nodeId, nodeName }) => {
  const dbName = useSelector((state) => state.dbName);
  const dispatch = useDispatch();

  return (
    <Button
      variant="outlined"
      onClick={() =>
        dispatch(thunks.getNodes(dbName, [nodeId], { yield_edges: true })).then(
          (res) => dispatch(actions.selectNode(Object.values(res)[0]))
        )
      }
      style={{ textTransform: "none" }}>
      {nodeName}
    </Button>
  );
};

export default function DBInspector() {
  const [selectedNodeType, setSelectedNodeType] = React.useState(null);
  const [propertyValue, setPropertyValue] = React.useState("");
  const dbName = useSelector((state) => state.dbName);

  return (
    <Box>
      <NodeTypeFilterContainer
        selected={selectedNodeType}
        setSelected={setSelectedNodeType}
      />

      <PropertyFilterContainer
        propertyValue={propertyValue}
        setPropertyValue={setPropertyValue}
      />

      <NodeFilterContainer
        selectedNodeType={selectedNodeType}
        propertyValue={propertyValue}
      />

      {dbName === "reactome" && (
        <BorderedContainer
          title={<BorderedContainerTitle title={"Common nodes"} />}>
          <NodeStack>
            <CommonNode
              nodeId={1930248}
              nodeName="APOE-4 [extracellular region]"
            />
            <CommonNode
              nodeId={653686}
              nodeName="APOE [extracellular region]"
            />
            <CommonNode nodeId={97787} nodeName="EGFR [plasma membrane]" />
            <CommonNode nodeId={451497} nodeName="STAT3 [cytosol]" />
            <CommonNode nodeId={119705} nodeName="AKT1 [plasma membrane]" />
            <CommonNode nodeId={120092} nodeName="AKT1 [cytosol]" />
            <CommonNode nodeId={641078} nodeName="VIM [cytosol]" />
            <CommonNode
              nodeId={583785}
              nodeName="BCL2 [mitochondrial outer membrane]"
            />
            <CommonNode nodeId={595625} nodeName="BCL2 gene [nucleoplasm]" />
            <CommonNode nodeId={1565} nodeName="HSPA8 [cytosol]" />
          </NodeStack>
        </BorderedContainer>
      )}

      <SelectedNodesContainer />
    </Box>
  );
}
