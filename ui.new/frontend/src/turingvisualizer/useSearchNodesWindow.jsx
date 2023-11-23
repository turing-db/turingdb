// Core
import React from "react";

// @blueprintjs
import {
  Colors,
  Text,
  MenuItem,
  FormGroup,
  Button,
  Card,
  CardList,
  Icon,
  Menu,
  InputGroup,
  Tag,
} from "@blueprintjs/core";
import { Select } from "@blueprintjs/select";

// Turing
import { useVisualizerContext } from "./";
import { useDialog } from "./dialogs";

const useSearchNodesWindow = () => {
  const vis = useVisualizerContext();
  const [propName, setPropName] = React.useState(vis.state()?.nodeLabel || "");
  const [propValue, setPropValue] = React.useState("");
  const previousNodes = React.useRef([]);
  const previousPropNames = React.useRef([]);
  const primaryColor =
    vis.state().themeMode === "dark" ? Colors.BLUE5 : Colors.BLUE1;

  const filterProp = (query, prop, _index, exactMatch) => {
    const normalizedProp = prop.toLowerCase();
    const normalizedQuery = query.toLowerCase();

    if (exactMatch) {
      return normalizedProp === normalizedQuery;
    }

    return normalizedProp.indexOf(normalizedQuery) >= 0;
  };

  const renderProp = (prop, { handleClick, handleFocus, modifiers }) => {
    if (!modifiers.matchesPredicate) {
      return null;
    }

    return (
      <MenuItem
        active={modifiers.active}
        disabled={modifiers.disabled}
        key={"item-" + prop}
        onClick={handleClick}
        onFocus={handleFocus}
        roleStructure="listoption"
        text={prop}
      />
    );
  };

  useDialog({
    name: "search-nodes",
    title: "Search nodes in graph",
    content: () => {
      const nodes = vis.dialogs()["search-nodes"]?.open
        ? vis
            .cy()
            ?.nodes()
            .filter((n) => {
              const v = String(
                (() => {
                  if (propName === "None") return n.data().turing_id;
                  if (propName === "Node Type") return n.data().node_type_name;
                  return n.data().properties[propName] || "";
                })()
              );

              return v.includes(propValue);
            })
        : previousNodes.current;
      previousNodes.current = nodes;

      const propNames = vis.dialogs()["search-nodes"]?.open
        ? [
            ...new Set(
              vis
                .cy()
                ?.nodes()
                .map((n) => Object.keys(n.data().properties))
                .flat()
            ),
            "Node Type",
          ]
        : previousPropNames.current;
      previousPropNames.current = propNames;

      const renderList = ({ items, renderItem, itemsParentRef }) => {
        const renderedItems = items
          .map(renderItem)
          .filter((item) => item != null);

        return (
          <Menu
            role="listbox"
            ulRef={itemsParentRef}
            style={{
              maxHeight: "30vh",
              overflow: "auto",
            }}>
            {renderedItems}
          </Menu>
        );
      };
      return (
        <div
          style={{
            width: "inherit",
            height: "60vh",
          }}>
          <FormGroup
            label="Find by property"
            inline
            style={{ padding: "10px 20px 0px 20px" }}>
            <Select
              items={propNames}
              itemRenderer={renderProp}
              itemListRenderer={renderList}
              itemPredicate={filterProp}
              noResults={
                <MenuItem
                  disabled={true}
                  text="No results."
                  roleStructure="listoption"
                />
              }
              onItemSelect={setPropName}>
              <Button
                text={propName}
                rightIcon="double-caret-vertical"
                placeholder="Select property"
              />
            </Select>
          </FormGroup>
          <div style={{ padding: 20, paddingTop: 0 }}>
            <InputGroup
              leftElement={<Icon icon="search" />}
              onChange={(e) => setPropValue(e.target.value)}
              placeholder="Find nodes"
              rightElement={<Tag minimal>{nodes.length}</Tag>}
              value={propValue}
            />
          </div>
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
                    vis.dialogs()["search-nodes"].toggle();
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
                  <Tag
                    intent={n.selected() ? "primary" : ""}
                    minimal
                    style={{ width: "min-content" }}>
                    id [{n.data().turing_id}]
                  </Tag>
                  <Text style={{ color: n.selected() ? primaryColor : "" }}>
                    {propName === "Node Type"
                      ? n.data().node_type_name
                      : n.data().properties[propName] || "Unnamed node"}
                  </Text>
                </Card>
              ))
            ) : (
              <div>No results</div>
            )}
          </CardList>
        </div>
      );
    },
  });
};
export default useSearchNodesWindow;
