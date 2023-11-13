// Core
import React from "react";

// Turing
import { Chart } from "regraph";

import { useCytoscapeElements } from "@turingvisualizer/tools";
import { Button } from "@blueprintjs/core";
import { useSelector } from "react-redux";

const labelStyle = {
  backgroundColor: "rgba(255,255,255,0)",
  color: "rgb(255,255,255)",
  margin: 0,
  padding: [0, 20, 0, 20],
};

const nodeProps = {
  border: {
    radius: 14,
    width: 8,
  },
};

const Overlay = (props) => {
  return (
    <div
      style={{
        position: "absolute",
        width: "100%",
        height: "100%",
        padding: 10,
        pointerEvents: "none",
      }}>
      {React.Children.map(props.children, (child) =>
        React.cloneElement(child, {
          style: {
            ...child.props.style,
            pointerEvents: "auto",
          },
        })
      )}
    </div>
  );
};

const useItems = (dbName, selectedNodeIds) => {
  const elements = useCytoscapeElements({
    themeMode: "dark",
    dbName: dbName,
    hiddenNodes: {},
    devMode: false,
    selectedNodeIds: selectedNodeIds,
    filters: {},
    nodeLabel: "label",
    edgeLabel: "Edge Type",
    nodeColors: {
      colorSets: {
        0: { mode: "None", data: {} }, // Global
      },
      mapping: {},
    },
    edgeColors: {
      colorSets: {
        0: { mode: "None", data: {} }, // Global
      },
      mapping: {},
    },
  });

  const items = React.useMemo(() => {
    const itms = Object.fromEntries(
      elements.map((e) => {
        if (e.group === "nodes") {
          return [
            "n" + e.data.turing_id,
            {
              turing_id: e.data.turing_id,
              data: { ...e.data.properties },
              shape: { width: "auto", height: "auto" },
              label: { text: e.data.label, ...labelStyle },
              color: e.data.type === "selected" ? "#357" : "#334",
              ...nodeProps,
            },
          ];
        }

        return [
          "e" + e.data.turing_id,
          {
            turing_id: e.data.turing_id,
            id1: "n" + e.data.turing_source_id,
            id2: "n" + e.data.turing_target_id,
            label: { text: e.data.label, ...labelStyle },
            width: 4,
          },
        ];
      })
    );

    //Object.values(itms).forEach((el) => {
    //  if (el.id1 === undefined) return; // Nodes

    //  // Else edges
    //  const n1 = itms[el.id1];
    //  const n2 = itms[el.id2];
    //  const l1 = n1.label.text;
    //  const l2 = n2.label.text;

    //  const n1IsEthnicity = l1 === "White" || l1 === "Black";
    //  const n2IsEthnicity = l2 === "White" || l2 === "Black";
    //  const n1IsPatient = l1.includes("pat");
    //  const n2IsPatient = l2.includes("pat");

    //  if (n1IsEthnicity && n2IsPatient) {
    //    itms[el.id2].data.ethnicity = l1;
    //    return;
    //  }

    //  if (n2IsEthnicity && n1IsPatient) {
    //    itms[el.id1].data.ethnicity = l2;
    //    return;
    //  }

    //  if (n1IsPatient && itms[el.id1].data.ethnicity === undefined) {
    //    itms[el.id1].data.ethnicity = "Unknown";
    //    return;
    //  }

    //  if (n2IsPatient && itms[el.id2].data.ethnicity === undefined) {
    //    itms[el.id2].data.ethnicity = "Unknown";
    //    return;
    //  }

    //});

    Object.values(itms).forEach((el) => {
      if (el.id1 === undefined) return; // Nodes

      // Else edges
      const n1 = itms[el.id1];
      const n2 = itms[el.id2];
      const l1 = n1.label.text;
      const l2 = n2.label.text;

      const n1IsHospital = l1.includes("hosp_");
      const n2IsHospital = l2.includes("hosp_");
      const n1IsPatient = l1.includes("patient_");
      const n2IsPatient = l2.includes("patient_");

      if (n1IsHospital && n2IsPatient) {
        itms[el.id2].data.hospital = l1;
        return;
      }

      if (n2IsHospital && n1IsPatient) {
        itms[el.id1].data.hospital = l2;
        return;
      }

      if (n1IsPatient && itms[el.id1].data.hospital === undefined) {
        itms[el.id1].data.hospital = "Unknown";
        return;
      }

      if (n2IsPatient && itms[el.id2].data.hospital === undefined) {
        itms[el.id2].data.hospital = "Unknown";
        return;
      }
    });

    const itms2 = Object.fromEntries(
      Object.entries(itms)
        .filter(([elId, el]) => {
          const labelsToRemove = [
            "White",
            "Black",
            "Other",
            "Hispanic",
            "Asian or Pacific Islander",
          ];

          if (el.id1 === undefined) {
            // Node
            if (el.data.label.includes("hosp_")) return false;
            if (labelsToRemove.some((l) => el.data.label === l)) return false;
            return true;
          }

          // Edges
          const n1 = itms[el.id1];
          const n2 = itms[el.id2];

          if (n1.data.label.includes("hosp_")) return false;
          if (n2.data.label.includes("hosp_")) return false;
          if (
            labelsToRemove.some((l) => {
              if (n1.data.label === l) return true;
              if (n2.data.label === l) return true;
              return false;
            })
          ) {
            return false;
          }

          return true;
        })
        .map(([elId, el]) => [elId, el])
    );

    return itms2;
  }, [elements]);

  return {
    items,
  };
};

const getChartSettings = () => {
  return {
    backgroundColor: "#111122",
    links: {
      inlineLabels: true,
    },
    labels: {
      maxLength: 30,
    },
    minZoom: 0.001,
  };
};

const AdminPageContent = (props) => {
  const [selectedNodeIds, setSelectedNodeIds] = React.useState(
    props.selectedNodeIds
  );
  const { items } = useItems(props.dbName, selectedNodeIds);
  const chartSettings = getChartSettings();

  const onDoubleClick = React.useCallback(
    (e) => {
      const item = items[e.id];
      if (item === undefined) return;
      if (item.id1 !== undefined) return; // Edge
      if (selectedNodeIds.includes(item.turing_id)) return;

      const newNodeIds = Object.keys(
        Object.fromEntries(
          [...selectedNodeIds, item.turing_id].map((id) => [id, true])
        )
      );

      if (newNodeIds.length === selectedNodeIds.length) return;

      setSelectedNodeIds(newNodeIds);
    },
    [items, selectedNodeIds]
  );

  const selectAll = React.useCallback(() => {
    const newNodeIds = Object.values(items)
      .filter((e) => {
        if (e.id1 !== undefined) return false;
        return true;
      })
      .map((n) => n.turing_id);

    const ids = Object.keys(
      Object.fromEntries(
        [...selectedNodeIds, ...newNodeIds].map((id) => [id, true])
      )
    );

    setSelectedNodeIds(ids);
  }, [items, selectedNodeIds]);

  const onCombineNodes = React.useCallback(({ setStyle, combo }) => {
    const colors = {
      White: "#fbb",
      Hispanic: "#b77",
      "Asian or Pacific Islander": "#eca",
      Black: "#433",
      Other: "#3e3",
    };

    setStyle({
      color: colors[combo.ethnicity],
      label: { fontSize: 66, text: combo.ethnicity },
    });
  }, []);

  return (
    <div
      style={{
        position: "relative",
        width: "100%",
        height: "100%",
      }}>
      <div
        style={{
          position: "absolute",
          width: "100%",
          height: "100%",
        }}>
        <Chart
          options={chartSettings}
          items={items}
          combine={{
            properties: ["ethnicity", "hospital"],
            level: 4,
            shape: "rectangle",
            number: 10,
          }}
          layout={{
            name: "organic",
          }}
          animation={{ animate: true, time: 1550 }}
          onCombineNodes={onCombineNodes}
          onDoubleClick={onDoubleClick}
        />
      </div>

      <Overlay>
        <Button
          text="Select all neighbors"
          icon="multi-select"
          onClick={selectAll}
        />
      </Overlay>
    </div>
  );
};

export default function AdminPage() {
  const dbName = useSelector((state) => state.dbName);
  const selectedNodes = useSelector((state) => state.selectedNodes);

  return (
    <AdminPageContent
      selectedNodeIds={Object.keys(selectedNodes)}
      dbName={dbName}
    />
  );
}
