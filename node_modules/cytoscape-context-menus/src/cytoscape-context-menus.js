import * as utils from './utils.js';
import { DEFAULT_OPTS, DIVIDER_CSS_CLASS, INDICATOR_CSS_CLASS } from './constants.js';
import { MenuItem, ContextMenu, MenuItemList } from './context-menu.js';

export function contextMenus(opts) {
  let cy = this;

  // Initilize scratch pad
  if (!cy.scratch('cycontextmenus')) {
    cy.scratch('cycontextmenus', {});
  }

  let getScratchProp = (propname) =>
    cy.scratch('cycontextmenus')[propname];

  let setScratchProp = (propname, value) =>
    cy.scratch('cycontextmenus')[propname] = value;

  let hasScratchProp = (propname) =>
    typeof cy.scratch('cycontextmenus')[propname] !== 'undefined';

  let options = getScratchProp('options');
  /** @type { ContextMenu } */
  let cxtMenu = getScratchProp('cxtMenu');

  /**
   * Right click event
   */
  let bindOnCxttap = () => {

    // TODO: move this to ContextMenu, just do the binding here
    let onCxttap = (event) => {
      setScratchProp('currentCyEvent', event);
      adjustCxtMenu(event); // adjust the position of context menu

      let target = event.target || event.cyTarget;

      // Check for each menuItem, if show is true, show the menuItem
      for (let menuItem of cxtMenu.children) {
          if (menuItem instanceof MenuItem) {
              let shouldDisplay = (target === cy) ?
                  // If user clicked in cy area then show core items
                  menuItem.coreAsWell :
                  // If selector of the item matches then show
                  target.is(menuItem.selector);
              // User clicked on empty area and menuItem is core
              if (shouldDisplay && menuItem.show) {
                cxtMenu.display();

                // anyVisibleChild indicates if there is any visible child of context menu if not do not show the context menu
                setScratchProp('anyVisibleChild', true); // there is visible child
                menuItem.display();
              }
          }
      }

      // Remove trailing divider from last visible menu item if it has it.
      // For other visible items, add divider if the associated menu item
      // should have divider, i.e, it was last item at some point and
      // the divider was removed but it should be there when the item
      // is not last

      const visibleItems = Array.from(cxtMenu.children).filter(item => {
        if (item instanceof MenuItem)
          return item.isVisible();
      });
      const length = visibleItems.length;
      visibleItems.forEach((item, index) => {
        if (!(item instanceof MenuItem))
          return;

        if (index < length - 1 && item.getHasTrailingDivider()) {
          item.classList.add(DIVIDER_CSS_CLASS);
        }
        else if (item.getHasTrailingDivider()) {
          item.classList.remove(DIVIDER_CSS_CLASS);
        };
      });

      if (!getScratchProp('anyVisibleChild') && utils.isElementVisible(cxtMenu)) {
        cxtMenu.hide();
      }
    };

    cy.on(options.evtType, onCxttap);
    setScratchProp('onCxttap', onCxttap);
  };

  let bindCyEvents = () => {

    let eventCyTapStart = event => {
      if (cxtMenu.contains(event.originalEvent.target)) {
        return false;
      }
      cxtMenu.hide();
      setScratchProp('cxtMenuPosition', undefined);
      setScratchProp('currentCyEvent', undefined);
    };

    cy.on('tapstart', eventCyTapStart);
    setScratchProp('eventCyTapStart', eventCyTapStart);

    let eventCyViewport = () => {
      cxtMenu.hide();
    };

    cy.on('viewport', eventCyViewport);
    setScratchProp('onViewport', eventCyViewport);
  };

  // Hide callbacks outside the cytoscape canvas
  let bindHideCallbacks = () => {
    let onClick = (event) => {
      let cyContainer = cy.container();
      // Hide only if click is outside of the Cytoscape area and the context menu
      if (!cyContainer.contains(event.target) && !cxtMenu.contains(event.target)) {
        cxtMenu.hide();
        setScratchProp('cxtMenuPosition', undefined);
      }
    };

    document.addEventListener('mouseup', onClick);
    setScratchProp('hideOnNonCyClick', onClick);
  };

  // Adjusts context menu if necessary
  let adjustCxtMenu = (event) => {
    const container = cy.container();
    let currentCxtMenuPosition = getScratchProp('cxtMenuPosition');
    let cyPos = event.position || event.cyPosition;

    if (currentCxtMenuPosition != cyPos) {
      cxtMenu.hideMenuItems();
      setScratchProp('anyVisibleChild', false);// we hide all children there is no visible child remaining
      setScratchProp('cxtMenuPosition', cyPos);

      let containerPos = utils.getOffset(container);
      let renderedPos = event.renderedPosition || event.cyRenderedPosition;

      let borderWidth = getComputedStyle(container)['border-width'];
      let borderThickness = parseInt(borderWidth.replace("px","")) || 0;
      if (borderThickness > 0) {
        containerPos.top += borderThickness;
        containerPos.left += borderThickness;
      }
      let containerHeight = container.clientHeight;
      let containerWidth = container.clientWidth;

      let horizontalSplit = containerHeight / 2;
      let verticalSplit = containerWidth / 2;

      //When user clicks on bottom-left part of window
      if (renderedPos.y > horizontalSplit && renderedPos.x <= verticalSplit) {
        cxtMenu.style.left = renderedPos.x + 'px';
        cxtMenu.style.bottom = (containerHeight - renderedPos.y) + 'px';
        cxtMenu.style.right = "auto";
        cxtMenu.style.top = "auto";
      } else if (renderedPos.y > horizontalSplit && renderedPos.x > verticalSplit) {
        cxtMenu.style.right = (containerWidth - renderedPos.x) + 'px';
        cxtMenu.style.bottom = (containerHeight - renderedPos.y) + 'px';
        cxtMenu.style.left = "auto";
        cxtMenu.style.top = "auto";
      } else if (renderedPos.y <= horizontalSplit && renderedPos.x <= verticalSplit) {
        cxtMenu.style.left = renderedPos.x + 'px';
        cxtMenu.style.top = renderedPos.y + 'px';
        cxtMenu.style.right = "auto";
        cxtMenu.style.bottom = "auto";
      } else {
        cxtMenu.style.right = (containerWidth - renderedPos.x) + 'px';
        cxtMenu.style.top = renderedPos.y + 'px';
        cxtMenu.style.left = "auto";
        cxtMenu.style.bottom = "auto";
      }
    }
  };

  let createAndAppendMenuItemComponent = (opts, parentID = undefined) => {
    // Create and append menu item
    let menuItemComponent = createMenuItemComponent(opts);

    if (typeof parentID !== 'undefined') {
      let parent = asMenuItem(parentID);

      cxtMenu.insertMenuItem(menuItemComponent, { parent });
    } else {
      cxtMenu.insertMenuItem(menuItemComponent);
    }
  };//insertComponentBeforeExistingItem(component, existingItemID)

  let createAndAppendMenuItemComponents = (optionsArr, parentID = undefined) => {
    for (let i = 0; i < optionsArr.length; i++) {
      createAndAppendMenuItemComponent(optionsArr[i], parentID);
    }
  };

  // Creates a menu item as an html component
  let createMenuItemComponent = (opts) => {
    let scratchpad = cy.scratch('cycontextmenus');
    return new MenuItem(opts, cxtMenu.onMenuItemClick, scratchpad);
  };

  let destroyCxtMenu = () => {
    if(!getScratchProp('active')) {
      return;
    }

    cxtMenu.removeAllMenuItems();

    cy.off('tapstart', getScratchProp('eventCyTapStart'));
    cy.off(options.evtType, getScratchProp('onCxttap'));
    cy.off('viewport', getScratchProp('onViewport'));
    document.removeEventListener('mouseup', getScratchProp('hideOnNonCyClick'));

    cxtMenu.parentNode.removeChild(cxtMenu);
    cxtMenu = undefined;

    setScratchProp('cxtMenu', undefined);
    setScratchProp('active', false);
    setScratchProp('anyVisibleChild', false);
    setScratchProp('onCxttap', undefined);
    setScratchProp('onViewport', undefined);
    setScratchProp('hideOnNonCyClick', undefined);
  };

  let makeSubmenuIndicator = (props) => {
    let elem = document.createElement('img');
    elem.src = props.src;
    elem.width = props.width;
    elem.height = props.height;
    elem.classList.add(INDICATOR_CSS_CLASS);

    return elem;
  };

  /**
   * @param { string } menuItemID
   */
  let asMenuItem = (menuItemID) => {
    let menuItem = document.getElementById(menuItemID);
    if (menuItem instanceof MenuItem) {
      return menuItem;
    } else {
      throw new Error(`The item with id=${menuItemID} is not a menu item`);
    }
  };

  // Get an extension instance to enable users to access extension methods
  let getInstance = (cy) => {
    let instance = {
      // Returns whether the extension is active
      isActive: function() {
        return getScratchProp('active');
      },
      // Appends given menu item to the menu items list.
      appendMenuItem: function(item, parentID = undefined) {
        createAndAppendMenuItemComponent(item, parentID);
        return cy;
      },
      // Appends menu items in the given list to the menu items list.
      appendMenuItems: function(items, parentID = undefined) {
        createAndAppendMenuItemComponents(items, parentID);
        return cy;
      },
      // Removes the menu item with given ID.
      removeMenuItem: function(itemID) {
        let item = asMenuItem(itemID);

        cxtMenu.removeMenuItem(item);
        return cy;
      },
      // Sets whether the menuItem with given ID will have a following divider.
      setTrailingDivider: function(itemID, status) {
        let menuItem = asMenuItem(itemID);
        menuItem.setHasTrailingDivider(status);
        
        if (status) {
          menuItem.classList.add(DIVIDER_CSS_CLASS);
        } else {
          menuItem.classList.remove(DIVIDER_CSS_CLASS);
        }
        return cy;
      },
      // Inserts given item before the existingitem.
      insertBeforeMenuItem: function(item, existingItemID) {
        let menuItemComponent = createMenuItemComponent(item);
        let existingItem = asMenuItem(existingItemID);

        cxtMenu.insertMenuItem(menuItemComponent, { before: existingItem });
        return cy;
      },
      // Moves the item to the submenu of the parent with the given ID
      moveToSubmenu: function(itemID, options = null) {
        let item = asMenuItem(itemID);

        if (options === null) {
          cxtMenu.moveToSubmenu(item);
        } else if (typeof options === 'string') {
          // options is parentID
          let parent = asMenuItem(options.toString());
          cxtMenu.moveToSubmenu(item, parent);
        } else if (typeof options.coreAsWell !== 'undefined' || typeof options.selector !== 'undefined') {
          cxtMenu.moveToSubmenu(item, null, options);
        } else {
          console.warn('options neither has coreAsWell nor selector property but it is an object. Are you sure that this is what you want to do?');
        }

        return cy;
      },
      // Moves the item with given ID before the existingitem.
      moveBeforeOtherMenuItem: function(itemID, existingItemID) {
        let item = asMenuItem(itemID);
        let before = asMenuItem(existingItemID);

        cxtMenu.moveBefore(item, before);
        return cy;
      },
      // Disables the menu item with given ID.
      disableMenuItem: function(itemID) {
        let menuItem = asMenuItem(itemID);

        menuItem.disable();
        return cy;
      },
      // Enables the menu item with given ID.
      enableMenuItem: function(itemID) {
        let menuItem = asMenuItem(itemID);

        menuItem.enable();
        return cy;
      },
      // Disables the menu item with given ID.
      hideMenuItem: function(itemID) {
        let menuItem = asMenuItem(itemID);

        menuItem.hide();
        return cy;
      },
      // Enables the menu item with given ID.
      showMenuItem: function(itemID) {
        let menuItem = asMenuItem(itemID);

        menuItem.display();
        return cy;
      },
      // Destroys the extension instance
      destroy: function() {
        destroyCxtMenu();
        return cy;
      }
    };

    return instance;
  };

  if ( opts !== 'get' ) {
    // https://developer.mozilla.org/en-US/docs/Web/Web_Components/Using_custom_elements
    MenuItem.define();
    MenuItemList.define();
    ContextMenu.define();

    // merge the options with default ones
    options = utils.extend(DEFAULT_OPTS, opts);
    setScratchProp('options', options);

    // Clear old context menu if needed
    if (getScratchProp('active')) {
      destroyCxtMenu();
    }

    setScratchProp('active', true);

    setScratchProp('submenuIndicatorGen', makeSubmenuIndicator.bind(undefined, options.submenuIndicator));

    // Create cxtMenu and append it to body
    let cxtMenuClasses = utils.getClassStr(options.contextMenuClasses);
    setScratchProp('cxtMenuClasses', cxtMenuClasses);

    let onMenuItemClick =
      () => setScratchProp('cxtMenuPosition', undefined);

    let scratchpad = cy.scratch('cycontextmenus');
    cxtMenu = new ContextMenu(onMenuItemClick, scratchpad);

    setScratchProp('cxtMenu', cxtMenu);
    //document.body.appendChild(cxtMenu);
    cy.container().appendChild(cxtMenu);

    setScratchProp('cxtMenuItemClasses', utils.getClassStr(options.menuItemClasses));
    let menuItems = options.menuItems;
    createAndAppendMenuItemComponents(menuItems);

    bindOnCxttap();
    bindCyEvents();
    bindHideCallbacks();

    utils.preventDefaultContextTap();
  }

  return getInstance(this);
}
