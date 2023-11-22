// @blueprintjs
import { Menu } from "@blueprintjs/core";

// Turing
import * as items from "../ContextMenu/items";
import { useMenuActions } from "../ContextMenu/hooks";

const SelectNodesMenu = () => {
  const actions = useMenuActions();

  return (
    <Menu>
      <items.ItemSelectAllBySameNodeTypeNoData actions={actions} />
      <items.ItemSelectAllBySamePropertyNoData actions={actions} />
    </Menu>
  );
};

export default SelectNodesMenu;
