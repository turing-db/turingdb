// Turing
import { useVisualizerContext } from "../context";
import { useCanvasTrigger } from "../useCanvasTrigger";
import { useCytoscapeInstance } from "../cytoscape/instance";
import { getFitViewport } from "../cytoscape/tools";
import { lockBehaviours } from "../reducers/layouts";

const Canvas = () => {
  const vis = useVisualizerContext();
  const cy = useCytoscapeInstance();

  useCanvasTrigger({
    category: "elements",
    name: "core-setElements",
    core: true,
    callback: () => {
      if (!cy.current) return;
      const c = cy.current;

      const elements = vis.state().elements;
      const layouts = vis.state().layouts;
      const idsBefore = Object.fromEntries(
        c.elements().map((e) => [e.id(), true])
      );
      const mappedElements = Object.fromEntries(
        elements.map((e) => [e.data.id, e])
      );
      const toAdd = elements.filter((e) => idsBefore[e.data.id] === undefined);
      const toKeep = c.collection(
        Object.keys(idsBefore)
          .filter((id) => mappedElements[id])
          .map((id) => c.$id(id))
      );
      const toRm = c.collection(
        Object.keys(idsBefore)
          .filter((id) => !mappedElements[id])
          .map((id) => c.$id(id))
      );
      const patchedElements = {};

      if (toAdd.length !== 0 || toRm.length !== 0) c.elements().unselect();
      toRm.forEach((e) => c.remove(e));
      toAdd.forEach((e) => c.add(e));

      // Patching exiting elements
      if (toRm.length === 0 && toAdd.length === 0) {
        c.batch(() => {
          toKeep.forEach((e1) => {
            const e2 = mappedElements[e1.id()];
            const data1 = e1.data();
            const data2 = e2.data;

            // data keys
            const keys = [
              "type",
              "iconColor",
              "textColor",
              "lineColor",
              "label",
              "has_hidden_nodes",
            ];
            for (const key of keys) {
              if (data1[key] !== data2[key]) {
                e1.data(data2);
                patchedElements[e1.id()] = true;
                break;
              }
            }
          });
        });
      }

      // If nodes were added or layouts were modified, apply layouts
      const nodesAdded = toAdd.length !== 0;

      const runCallbacks = {
        cola: (it, lId, lDef) => {
          const allElements = c.elements();
          const nonColaEles = c.filter((e) => {
            const l1 = parseInt(layouts.mapping[e.id()] || 0);
            const l2 = parseInt(lId);
            return l1 !== l2;
          });

          const rawAddedElements = Object.fromEntries(
            toAdd.map((n) => [n.id, n])
          );
          const addedElements = c.filter(
            (n) => rawAddedElements[n.id()] !== undefined
          );

          const addedNodes = addedElements.nodes();
          c.edges().forEach((e) => {
            const n1 = e.source();
            const n2 = e.target();
            let originNode = undefined;
            let newNode = undefined;
            if (
              toKeep.$id(n1.id()).length !== 0 &&
              addedNodes.$id(n2.id()).length !== 0
            ) {
              originNode = n1;
              newNode = n2;
            } else if (
              toKeep.$id(n2.id()).length !== 0 &&
              addedNodes.$id(n1.id()).length !== 0
            ) {
              originNode = n2;
              newNode = n1;
            }
            if (!originNode) return;

            const neighbors = originNode
              .neighborhood()
              .nodes()
              .filter(
                (n) => n.id() !== newNode.id() && n.data().type === "selected"
              );

            if (neighbors.length === 0) return;

            const totalPosition = neighbors
              .map((n) => n.position())
              .reduce(
                (pos_a, pos_b) => ({
                  x: pos_a.x + pos_b.x,
                  y: pos_a.y + pos_b.y,
                }),
                { x: 0, y: 0 }
              );

            const origin = originNode.position();
            const vector = {
              x: originNode.position().x - totalPosition.x / neighbors.length,
              y: originNode.position().y - totalPosition.y / neighbors.length,
            };
            const norm = Math.sqrt(vector.x * vector.x + vector.y * vector.y);
            const unitVector = {
              x: vector.x / norm,
              y: vector.y / norm,
            };
            const newPosition = {
              x: origin.x + unitVector.x * lDef.edgeLengthVal,
              y: origin.y + unitVector.y * lDef.edgeLengthVal,
            };
            newNode.position(newPosition);
          });

          // If no elements to add or remove, it means the graph did not have
          // any element prior to this operation. Thus, we can center the camera
          const firstRender = toKeep.length === 0 && toRm.length === 0;

          const selectedNodes = c
            .nodes()
            .filter((n) => n.data().type === "selected");

          if (toKeep.length !== c.elements().length) {
            if (!firstRender) {
              switch (lDef.lockBehaviour) {
                case lockBehaviours.ALL_SELECTED: {
                  selectedNodes.lock();
                  break;
                }
                case lockBehaviours.INTERACTED: {
                  selectedNodes.filter((n) => patchedElements[n.id()]).lock();
                  break;
                }
                default: {
                  break;
                }
              }
            }
          }

          nonColaEles.lock();
          addedElements.style({
            opacity: 0.0,
          });

          const nextLayoutId = it.next().value;
          const nextLayout = layouts.definitions[nextLayoutId];
          const nextLayoutName = nextLayout?.name || "end";

          const stop = () => {
            c.elements().unlock();
            runCallbacks[nextLayoutName](it, nextLayoutId, nextLayout);
          };

          const ready = () => {
            if (firstRender) {
              setTimeout(() => {
                c.fit(allElements, 200);
              }, 50);
            }
          };

          const layout = allElements.layout({
            ...lDef,
            centerGraph: firstRender,
            //fit: firstRender,
            //padding: firstRender ? 130 : 0,
            stop,
            ready,
          });

          addedElements.animate({
            style: { opacity: 1.0 },
            duration: 1000,
            easing: "ease-in-out-expo",
          });

          layout.run();
        },

        preset: (it, lId, lDef) => {
          const eles = c.filter((e) => {
            const l1 = parseInt(layouts.mapping[e.id()] || 0);
            const l2 = parseInt(lId);
            return l1 === l2;
          });
          const nextLayoutId = it.next().value;
          const nextLayout = layouts.definitions[nextLayoutId];
          const nextLayoutName = nextLayout?.name || "end";
          const stop = () => {
            runCallbacks[nextLayoutName](it, nextLayoutId, nextLayout);
          };

          eles.layout({ ...lDef, stop }).run();
        },

        random: (it, lId, lDef) => {
          const eles = c.filter((e) => {
            const l1 = parseInt(layouts.mapping[e.id()] || 0);
            const l2 = parseInt(lId);
            return l1 === l2;
          });
          const nextLayoutId = it.next().value;
          const nextLayout = layouts.definitions[nextLayoutId];
          const nextLayoutName = nextLayout?.name || "end";
          const stop = () =>
            runCallbacks[nextLayoutName](it, nextLayoutId, nextLayout);

          eles.layout({ ...lDef, stop }).run();
        },

        end: () => {
          if (layouts.fitRequested) {
            const viewport = getFitViewport(vis.cy(), vis.cy().elements(), 100);
            const zoom = viewport?.zoom;

            if (!zoom) return;

            vis.cy().minZoom(zoom);
            vis.cy().maxZoom(6);

            vis.cy().animate(
              {
                fit: { padding: 100 },
              },
              {
                duration: 600,
                easing: "ease-in-out-sine",
                queue: true,
              }
            );
            vis.callbacks().requestLayoutFit(false);
          }
        },
      };

      if (nodesAdded || layouts.runRequested) {
        const firstLayout = layouts.definitions[0];
        const it = Object.keys(layouts.definitions)[Symbol.iterator]();
        const firstLayoutId = it.next().value;

        runCallbacks[firstLayout.name](it, firstLayoutId, firstLayout);
        vis.callbacks().requestLayoutRun(false);
      }
    },
  });

  return (
    <div
      id={vis.containerId}
      style={{
        position: "absolute",
        width: "100%",
        height: "100%",
      }}></div>
  );
};

export default Canvas;
