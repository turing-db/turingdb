export const CXT_MENU_CSS_CLASS = 'cy-context-menus-cxt-menu';
export const MENUITEM_CSS_CLASS = 'cy-context-menus-cxt-menuitem';
export const DIVIDER_CSS_CLASS = 'cy-context-menus-divider';
export const INDICATOR_CSS_CLASS = 'cy-context-menus-submenu-indicator';

export const DEFAULT_OPTS = {
    // Customize event to bring up the context menu
    // Possible options https://js.cytoscape.org/#events/user-input-device-events
    evtType: 'cxttap',
    // List of initial menu items
    menuItems: [
      /*
      {
        id: 'remove',
        content: 'remove',
        tooltipText: 'remove',
        selector: 'node, edge',
        onClickFunction: function () {
          console.log('remove element');
        },
        hasTrailingDivider: true
      },
      {
        id: 'hide',
        content: 'hide',
        tooltipText: 'remove',
        selector: 'node, edge',
        onClickFunction: function () {
          console.log('hide element');
        },
        disabled: true
      }*/
    ],
    // css classes that menu items will have
    menuItemClasses: [
      MENUITEM_CSS_CLASS,
    ],
    // css classes that context menu will have
    contextMenuClasses: [
      CXT_MENU_CSS_CLASS,
    ],
    submenuIndicator: { src: 'assets/submenu-indicator-default.svg', width: 12, height: 12 }
};
