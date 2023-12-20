import { CardList, Card, Tag, Text } from "@blueprintjs/core";
import { useVisualizerContext } from "../../../context";

export default function NodeContainer(props) {
  const vis = useVisualizerContext();

  return (
    <CardList className="overflow-auto flex-1">
      {props.nodes.length !== 0 ? (
        props.nodes.map((n) => (
          <Card
            key={"node-" + n.id()}
            interactive={true}
            onClick={() => {
              vis.searchNodesDialog.close();
              vis.cy().elements().unselect();
              vis.cy().animate(
                {
                  center: {
                    eles: n,
                  },
                },
                {
                  duration: 600,
                  easing: "ease-in-out-sine",
                }
              );
              n.select();
            }}>
            <div className="flex flex-col items-start">
              <Tag
                intent={n.selected() ? "primary" : ""}
                minimal
                style={{ width: "min-content" }}>
                id [{n.data().turing_id}]
              </Tag>
              <Text className={n.selected() ? "primary" : ""}>
                {props.propName === "Node Type"
                  ? n.data().node_type_name
                  : n.data().properties[props.propName] || "Unnamed node"}
              </Text>
            </div>
          </Card>
        ))
      ) : (
        <div>No results</div>
      )}
    </CardList>
  );
}
