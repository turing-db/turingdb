// Core
import React from "react";

// @blueprintjs
import { Card, CardList, Dialog, Tag, Text } from "@blueprintjs/core";

// Turing
import { useVisualizerContext } from "../../context";
import { dialogParams } from "../../tools";
import { endpoints } from "../../queries";
import { GraphNode } from "../../types";
import { lockBehaviours } from "../../reducers/layouts";

export function ShowInPathwayDialog() {
  const vis = useVisualizerContext();
  const [loading, setLoading] = React.useState(true);
  const [pathways, setPathways] = React.useState<GraphNode[]>([]);
  const data = vis.contextMenuData()?.data;

  React.useEffect(() => {
    if (!vis.showInPathwayDialog.isOpen && !loading) {
      setLoading(true);
      return;
    }
    if (!vis.showInPathwayDialog.isOpen || !data) return;

    if (loading) {
      endpoints["list_pathways"]({
        dbName: vis.state()?.dbName,
        nodeId: data.turing_id as string,
      }).then((res) => {
        setPathways(res);
        setLoading(false);
      });
    }
  }, [vis.showInPathwayDialog.isOpen]);

  return (
    <>
      <Dialog
        {...dialogParams}
        isOpen={vis.showInPathwayDialog.isOpen}
        onClose={vis.showInPathwayDialog.close}
        className={dialogParams.className + " h-[70vh]"}
        title="Show node in pathway">
        <CardList className="overflow-auto flex-1">
          {pathways.length !== 0 ? (
            pathways.map((p) => (
              <Card
                key={"p-" + p.data.turing_id}
                interactive={true}
                onClick={() => {
                  const lockBehaviour =
                    vis.state()!.layouts.definitions[0]?.lockBehaviour!;

                  vis.callbacks()!.setDefaultCyLayout({
                    ...vis.state()!.layouts.definitions[0]!,
                    lockBehaviour: lockBehaviours.DO_NOT_LOCK,
                  });

                  vis.showInPathwayDialog.close();
                  const cy = vis.cy()!;
                  endpoints["get_pathway"]({
                    nodeId: p.data.turing_id,
                    dbName: vis.state()!.dbName,
                  }).then((res) => {
                      if (res.failed) {
                        console.log(res.error)
                        return;
                      }
                      const pathwayNodes = res.nodes;
                    vis
                      .callbacks()!
                      .setSelectedNodeIds(
                        pathwayNodes!.map((n) => n.data.turing_id)
                      );

                    cy.one("layoutstop", () => {
                      const n = cy.$id(data.id);
                      n.select();
                      cy.animate(
                        {
                          fit: { eles: cy.elements(), padding: 100 },
                          center: { eles: n },
                        },
                        {
                          duration: 600,
                          easing: "ease-in-out-sine",
                          queue: false,
                        }
                      );
                      vis.callbacks()!.setDefaultCyLayout({
                        ...vis.state()!.layouts.definitions[0]!,
                        lockBehaviour: lockBehaviour,
                      });
                    });
                  });
                }}>
                <div className="flex flex-col items-start">
                  <Tag minimal style={{ width: "min-content" }}>
                    id [{p.data.turing_id}]
                  </Tag>
                  <Text>{p.data.properties["displayName"]}</Text>
                </div>
              </Card>
            ))
          ) : (
            <div>No results</div>
          )}
        </CardList>
      </Dialog>
    </>
  );
}
