import { Menu, Button, MenuItem } from "@blueprintjs/core";
import {
  ItemListRenderer,
  ItemPredicate,
  ItemRenderer,
  Select,
} from "@blueprintjs/select";

export function SelectProperty(props: {
  properties: string[];
  currentProp: string | undefined;
  setCurrentProp: React.Dispatch<React.SetStateAction<string>>;
}) {
  const predicate: ItemPredicate<string> = (
    query,
    text: string,
    _index,
    exactMatch
  ) => {
    const normalizedTitle = text.toLowerCase();
    const normalizedQuery = query.toLowerCase();

    if (exactMatch) {
      return normalizedTitle === normalizedQuery;
    } else {
      return text.indexOf(normalizedQuery) >= 0;
    }
  };

  const renderer: ItemRenderer<string> = (
    property,
    { handleClick, handleFocus, modifiers }
  ) => {
    if (!modifiers.matchesPredicate) {
      return null;
    }
    return (
      <MenuItem
        active={modifiers.active}
        disabled={modifiers.disabled}
        key={property}
        onClick={handleClick}
        onFocus={handleFocus}
        roleStructure="listoption"
        text={property}
      />
    );
  };

  const listRenderer: ItemListRenderer<string> = ({
    items,
    itemsParentRef,
    query,
    renderItem,
    menuProps,
  }) => {
    const renderedItems = items.map(renderItem).filter((item) => item != null);
    return (
      <Menu
        className="max-h-[60vh] overflow-auto"
        role="listbox"
        ulRef={itemsParentRef}
        {...menuProps}>
        <MenuItem
          disabled={true}
          text={`Found ${renderedItems.length} items matching "${query}"`}
          roleStructure="listoption"
        />
        {renderedItems}
      </Menu>
    );
  };

  return (
    <Select<string>
      items={props.properties}
      itemPredicate={predicate}
      itemRenderer={renderer}
      itemListRenderer={listRenderer}
      noResults={
        <MenuItem
          disabled={true}
          text="No results."
          roleStructure="listoption"
        />
      }
      onItemSelect={props.setCurrentProp}>
      <Button
        text={props.currentProp || "Select a property"}
        className="w-[150px] overflow-hidden"
        rightIcon="double-caret-vertical"
        placeholder="Select a film"
      />
    </Select>
  );
}
