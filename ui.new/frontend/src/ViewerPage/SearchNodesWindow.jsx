// Core
import React from "react";

// @blueprintjs
import { Card, CardList, Icon, InputGroup, Tag } from "@blueprintjs/core";

// Turing
import { useVisualizerContext } from "@turingvisualizer";

const SearchNodesWindow = () => {
  const vis = useVisualizerContext();
  const [label, setLabel] = React.useState("");

  const nodes = React.useMemo(() => {
    const nl = vis.state().nodeLabel;
    return vis
      .cy()
      .nodes()
      .filter((n) => {
        const v = String(
          (() => {
            if (nl === "None") return n.data().turing_id;
            if (nl === "Node Type") return n.data().node_type_name;
            return n.data().properties[nl] || "";
          })()
        );

        return v.includes(label);
      });
  }, [vis, label]);

  return (
    <div
      style={{
        minWidth: "20px",
        maxWidth: "50vw",
      }}>
      <InputGroup
        leftElement={<Icon icon="search" />}
        onChange={(e) => setLabel(e.target.value)}
        placeholder="Find nodes"
        rightElement={<Tag minimal>{nodes.length}</Tag>}
        value={label}
      />
      <CardList
        style={{
          maxHeight: "50vh",
          overflow: "auto",
        }}>
        {nodes.length !== 0 ? (
          nodes.map((n) => (
            <Card
              key={"node-" + n.id()}
              interactive={true}
              elevation={2}
              style={{
                display: "flex",
                flexDirection: "column",
                alignItems: "start",
              }}
              onClick={() => {
                vis.cy().elements().unselect();
                n.select();
                vis.cy().animate(
                  {
                    fit: {
                      eles: n,
                      padding: 300,
                    },
                  },
                  {
                    duration: 600,
                    easing: "ease-in-out-sine",
                  }
                );
              }}>
              <Tag minimal style={{ width: "min-content" }}>
                id [{n.data().turing_id}]
              </Tag>
              <div>
                {vis.state().nodeLabel === "Node Type"
                  ? n.data().node_type_name
                  : n.data().properties[vis.state().nodeLabel] ||
                    "Unnamed node"}
              </div>
            </Card>
          ))
        ) : (
          <div>No results</div>
        )}
      </CardList>
    </div>
  );
};

export default SearchNodesWindow;
