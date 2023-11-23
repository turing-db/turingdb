import { Select } from "@blueprintjs/select";
import { Menu, MenuItem } from "@blueprintjs/core";

export const SelectMenu = (props) => {
  const predicate = (query, value, _index, exactMatch) => {
    const normalizedProp = String(value).toLowerCase();
    const normalizedQuery = query.toLowerCase();

    if (exactMatch) {
      return normalizedProp === normalizedQuery;
    }

    return normalizedProp.indexOf(normalizedQuery) >= 0;
  };

  const renderMenu = ({ items, renderItem, itemsParentRef }) => {
    const renderedItems = items.map(renderItem).filter((item) => item != null);

    return (
      <Menu
        role="listbox"
        ulRef={itemsParentRef}
        {...props.menuProps}
        style={{
          maxHeight: "30vh",
          overflow: "auto",
          ...props.menuProps?.style,
        }}>
        {renderedItems}
      </Menu>
    );
  };

  const renderChild = (value, { modifiers }) => {
    if (!modifiers.matchesPredicate) {
      return null;
    }
    const v = String(value);

    return props.renderChild(v);
  };

  return (
    <Select
      items={props.items}
      itemRenderer={renderChild}
      itemPredicate={predicate}
      itemListRenderer={renderMenu}
      popoverProps={{
        placement: `${props.placement || "right"}-start`,
      }}
      noResults={
        <MenuItem
          key="select-menu-no-result"
          disabled={true}
          text="No results."
          roleStructure="listoption"
        />
      }>
      {props.Parent}
    </Select>
  );
};
