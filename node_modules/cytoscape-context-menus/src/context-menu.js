import { setBooleanAttribute, getClassStr, isIn, getDimensionsHidden, defineCustomElement } from './utils';
import { DIVIDER_CSS_CLASS } from './constants';

// TODO: add submenu property

function stopEvent(event) {
  event.preventDefault();
  event.stopPropagation();
}

export class MenuItem extends HTMLButtonElement {
    /**
     * @param {{
     *      id: string;
     *      tooltipText?: string;
     *      disabled?: boolean;
     *      image?: {
     *          src: string;
     *          width: number;
     *          height: number;
     *          y: string;
     *          x: string;
     *      };
     *      content: string;
     *      selector: string;
     *      show?: boolean;
     *      submenu?: Array;
     *      coreAsWell?: boolean;
     *      onClickFunction?: any;
     *      hasTrailingDivider?: boolean;
     * }} params
     * @param { * } onMenuItemClick
     * passed so that submenu items can have this
     * called when the menu item is clicked
     */
    constructor(
        params,
        onMenuItemClick,
        scratchpad
    ) {
        super();

        super.setAttribute('id', params.id);

        let className = this._getMenuItemClassStr(scratchpad['cxtMenuItemClasses'], params.hasTrailingDivider);

        super.setAttribute('class', className);

        super.setAttribute('title', params.tooltipText ?? "");

        if (params.disabled) {
            setBooleanAttribute(this, 'disabled', true);
        }

        if (params.image) {
            let img = document.createElement('img');
            img.src = params.image.src;
            img.width = params.image.width;
            img.height = params.image.height;
            img.style.position = 'absolute';
            img.style.top = params.image.y + 'px';
            img.style.left = params.image.x + 'px';

            super.appendChild(img);
        }

        this.innerHTML += params.content;

        this.onMenuItemClick = onMenuItemClick;

        this.data = {};
        this.clickFns = [];
        this.selector = params.selector;
        this.hasTrailingDivider = params.hasTrailingDivider;
        this.show = (typeof params.show === 'undefined') || params.show;
        this.coreAsWell = params.coreAsWell || false;
        this.scratchpad = scratchpad;

        if (typeof params.onClickFunction === 'undefined' &&
            typeof params.submenu === 'undefined') {

            throw new Error("A menu item must either have click function or a submenu or both");
        }

        this.onClickFunction = params.onClickFunction;

        // Create the submenu if neccessary
        if (params.submenu instanceof Array) {
            this._createSubmenu(params.submenu);
        }

        super.addEventListener('mousedown', stopEvent);
        super.addEventListener('mouseup', stopEvent);
        super.addEventListener('touchstart', stopEvent);
        super.addEventListener('touchend', stopEvent);
    }

    bindOnClickFunction(onClickFn) {
        this.clickFns.push(onClickFn);

        super.addEventListener('click', onClickFn);
    }

    unbindOnClickFunctions() {
        for (let onClickFn of this.clickFns) {
            super.removeEventListener('click', onClickFn);
        }
        this.clickFns = [];
    }

    enable() {
        setBooleanAttribute(this, 'disabled', false);

        if (this.hasSubmenu()) {
            this.addEventListener('mouseenter', this.mouseEnterHandler);
        }
    }

    disable() {
        setBooleanAttribute(this, 'disabled', true);

        if (this.hasSubmenu()) {
            this.removeEventListener('mouseenter', this.mouseEnterHandler);
        }
    }

    hide() {
        this.show = false;
        this.style.display = 'none';
    }

    getHasTrailingDivider() {
        // may be undefined so use this way
        return this.hasTrailingDivider ? true : false;
    }
    /**
     * @param {boolean} status 
     */
    setHasTrailingDivider(status) {
        this.hasTrailingDivider = status;
    }

    hasSubmenu() {
        return this.submenu instanceof MenuItemList;
    }

    appendSubmenuItem(menuItem, before = undefined) {
        if (!this.hasSubmenu()) {
            this._createSubmenu();
        }
        this.submenu.appendMenuItem(menuItem, before)
    }

    isClickable() {
        return this.onClickFunction !== undefined;
    }

    display() {
        this.show = true;
        this.style.display = 'block';
    }

    /**
     * Returns true if this menu item is currently visible
     */
    isVisible() {
        return this.show === true && this.style.display !== 'none';
    }

    /**
     * Removes the submenu if exists
     */
    removeSubmenu() {
        if (this.hasSubmenu()) {
            this.submenu.removeAllMenuItems();
            this.detachSubmenu();
        }
    }

    detachSubmenu() {
        if (this.hasSubmenu()) {
            this.removeChild(this.submenu);
            this.removeChild(this.indicator);
            this.removeEventListener('mouseenter', this.mouseEnterHandler);
            this.removeEventListener('mouseleave', this.mouseLeaveHandler);
            this.submenu = undefined;
            this.indicator = undefined;
        }
    }

    _onMouseEnter(_event) {
        let rect = this.getBoundingClientRect();
        let submenuRect = getDimensionsHidden(this.submenu);

        let exceedsRight = (rect.right + submenuRect.width) > window.innerWidth;
        let exceedsBottom = (rect.top + submenuRect.height) > window.innerHeight;

        // Adjusts the position of the submenu
        if (!exceedsRight && !exceedsBottom) {
            this.submenu.style.left = this.clientWidth + "px";
            this.submenu.style.top = "0px";
            this.submenu.style.right = "auto";
            this.submenu.style.bottom = "auto";
        } else if (exceedsRight && !exceedsBottom) {
            this.submenu.style.right = this.clientWidth + "px";
            this.submenu.style.top = "0px";
            this.submenu.style.left = "auto";
            this.submenu.style.bottom = "auto";
        } else if (exceedsRight && exceedsBottom) {
            this.submenu.style.right = this.clientWidth + "px";
            this.submenu.style.bottom = "0px";
            this.submenu.style.top = "auto";
            this.submenu.style.left = "auto";
        } else {
            this.submenu.style.left = this.clientWidth + "px";
            this.submenu.style.bottom = "0px";
            this.submenu.style.right = "auto";
            this.submenu.style.top = "auto";
        }

        this.submenu.display();

        // Remove trailing divider from last visible menu item if it has it.
        // For other visible items, add divider if the associated menu item
        // should have divider, i.e, it was last item at some point and
        // the divider was removed but it should be there when the item
        // is not last

        const visibleItems = Array.from(this.submenu.children).filter(item => {
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
    }

    _onMouseLeave(event) {
        let pos = { x: event.clientX, y: event.clientY };

        // Hide if mouse is not passed to the submenu
        if (!isIn(pos, this.submenu)) {
            this.submenu.hide();
        }
    }

    _createSubmenu(items = []) {
        // We generate another indicator for each
        this.indicator = this.scratchpad['submenuIndicatorGen']();
        this.submenu = new MenuItemList(this.onMenuItemClick, this.scratchpad);

        this.appendChild(this.indicator);
        this.appendChild(this.submenu);

        for (let item of items) {
            let menuItem = new MenuItem(item, this.onMenuItemClick, this.scratchpad);
            this.submenu.appendMenuItem(menuItem);
        }

        this.mouseEnterHandler = this._onMouseEnter.bind(this);
        this.mouseLeaveHandler = this._onMouseLeave.bind(this);

        // submenu should be visible when mouse is over
        this.addEventListener('mouseenter', this.mouseEnterHandler);

        this.addEventListener('mouseleave', this.mouseLeaveHandler);
    }

    // TODO: can be static
    _getMenuItemClassStr(classStr, hasTrailingDivider) {
        return hasTrailingDivider ?
            classStr + ' ' + DIVIDER_CSS_CLASS :
            classStr;
    };

    static define() {
        defineCustomElement('ctx-menu-item', MenuItem, 'button');
    }
}

export class MenuItemList extends HTMLDivElement {
    constructor(onMenuItemClick, scratchpad) {
        super();

        super.setAttribute('class', scratchpad['cxtMenuClasses']);

        this.style.position = 'absolute';

        this.onMenuItemClick = onMenuItemClick;
        this.scratchpad = scratchpad;
    }

    hide() {
        if (this.isVisible()) {
            this.hideSubmenus();
            this.style.display = 'none';
        }
    }

    display() {
        this.style.display = 'block';
    }

    isVisible() {
        return this.style.display !== 'none';
    }

    /**
     * Hides all menu items
     */
    hideMenuItems(){
        for (let item of this.children) {
            if (item instanceof HTMLElement) {
                item.style.display = 'none';
            } else {
                console.warn(`${item} is not a HTMLElement`);
            }
        }
    }

    hideSubmenus() {
        for (let menuItem of this.children) {
            if (menuItem instanceof MenuItem) {
                if (menuItem.submenu) {
                    menuItem.submenu.hide();
                }
            }
        }
    }

    /**
     * @param { MenuItem } menuItem
     * @param { Element? } before
     * If before is specified menuItem is inserted before this element instead of at the end \
     * By default appends at the end of the this
     */
    appendMenuItem(menuItem, before = undefined) {
        if (typeof before !== 'undefined') {
            if (before.parentNode === this) {
                this.insertBefore(menuItem, before);
            } else {
                throw new Error(`The item with id='${before.id}' is not a child of the context menu`);
            }
        } else {
            this.appendChild(menuItem);
        }

        if (menuItem.isClickable()) {
            this._performBindings(menuItem);
        }
    }

    /**
     * Removes any menuItem that is any children of the context menu
     * Returns true if child is found and removed, false otherwise
     * @param { MenuItem } menuItem
     */
    /* removeMenuItem(menuItem) {
        if (this._removeImmediateMenuItem(menuItem)) {
            return true;
        } else {
            for (let child of this.children) {
                if (child instanceof MenuItem && child.hasSubmenu()) {
                    if (child.submenu.removeMenuItem(menuItem)) {
                        return true;
                    }
                }
            }
            // throw new Error(`The item with id='${menuItem.id}' is not a child of the context menu`);
            return false;
        }
    } */

    /**
     * Moves a menuItem before another
     * @param { MenuItem } menuItem
     * @param { MenuItem } before
     */
    moveBefore(menuItem, before) {
        if (menuItem.parentNode !== this) {
            throw new Error(`The item with id='${menuItem.id}' is not a child of context menu`);
        }
        if (before.parentNode !== this) {
            throw new Error(`The item with id='${before.id}' is not a child of context menu`);
        }

        this.removeChild(menuItem);
        this.insertBefore(menuItem, before);
    }

    removeAllMenuItems() {
        // https://stackoverflow.com/a/3955238/12045421
        while (this.firstChild) {
            let child = this.lastChild;
            if (child instanceof MenuItem) {
                this._removeImmediateMenuItem(child);
            } else {
                console.warn("Found non menu item in the context menu: ", child);
                // Remove it as well
                this.removeChild(child);
            }
        }
    }

    /**
     * Removes if the `menuItem` is direct child of the parent
     * @param { MenuItem } menuItem
     */
    _removeImmediateMenuItem(menuItem) {
        if (this._detachImmediateMenuItem(menuItem)) {
            menuItem.detachSubmenu();
            menuItem.unbindOnClickFunctions();
        } else {
            throw new Error(`menu item(id=${menuItem.id}) is not in the context menu`);
        }
    }

    /**
     * Detaches `menuItem` from `this` doesn't destroy it
     * @param { MenuItem } menuItem
     * @returns { boolean }
     */
    _detachImmediateMenuItem(menuItem) {
        if (menuItem.parentNode === this) {
            this.removeChild(menuItem);

            if (this.children.length <= 0) {
                let parent = this.parentNode;
                if (parent instanceof MenuItem) {
                    parent.detachSubmenu();
                }
            }

            return true;
        } else {
            return false;
        }
    }

    /**
     * @param { MenuItem } menuItem
     */
    _performBindings(menuItem) {
        let callback = this._bindOnClick(menuItem.onClickFunction);
        menuItem.bindOnClickFunction(callback);
        menuItem.bindOnClickFunction(this.onMenuItemClick);
    }

    _bindOnClick(onClickFn) {
        return () => {
            let event = this.scratchpad['currentCyEvent'];
            onClickFn(event);
        };
    }

    static define() {
        defineCustomElement('menu-item-list', MenuItemList, 'div');
    }
}

export class ContextMenu extends MenuItemList {

    constructor(onMenuItemClick, scratchpad) {
        super(onMenuItemClick, scratchpad);

        // Called when a menu item is clicked
        this.onMenuItemClick = (event) => {
            // So that parent menuItems won't be clicked
            stopEvent(event);
            this.hide();
            onMenuItemClick();
        };

        /* this.addEventListener('mouseleave', (_event) => {
            this.hideMenuItemSubmenus();
        }); */
    }

    /**
     * @param { MenuItem } menuItem
     */
    removeMenuItem(menuItem) {
        let parent = menuItem.parentElement;

        if (parent instanceof MenuItemList && this.contains(parent)) {
            parent._removeImmediateMenuItem(menuItem);
        }
    }

    /**
     * @param { MenuItem } menuItem
     * @param { Element? } before
     */
    appendMenuItem(menuItem, before = undefined) {
        this.ensureDoesntContain(menuItem.id);

        super.appendMenuItem(menuItem, before);
    }

    /**
     * Inserts the menu item to the context menu \
     * If before is specified, item is inserted before the 'before' inside the same submenu \
     * The parent argument is ignored if before is specified because parent can be inferred from the before argument \
     * If parent is specified, item is inserted into the submenu of specified parent
     * @param { MenuItem } menuItem
     * @param {{ before?: MenuItem, parent?: MenuItem }} param1
     */
    insertMenuItem(menuItem, { before, parent } = {}) {
        this.ensureDoesntContain(menuItem.id);

        if (typeof before !== 'undefined') {
            if (this.contains(before)) {
                let parent = before.parentNode;
                if (parent instanceof MenuItemList) {
                    parent.appendMenuItem(menuItem, before);
                } else {
                    throw new Error(`Parent of before(id=${before.id}) is not a submenu`);
                }
            } else {
                throw new Error(`before(id=${before.id}) is not in the context menu`);
            }
        } else if (typeof parent !== 'undefined') {
            if (this.contains(parent)) {
                parent.appendSubmenuItem(menuItem);
            } else {
                throw new Error(`parent(id=${parent.id}) is not a descendant of the context menu`);
            }
        } else {
            this.appendMenuItem(menuItem);
        }
    }

    /**
     * @param { MenuItem } menuItem
     * @param { MenuItem } before
     */
    moveBefore(menuItem, before) {
        let parent = menuItem.parentElement;
        if (this.contains(parent)) {
            if (this.contains(before)) {
                parent.removeChild(menuItem);
                this.insertMenuItem(menuItem, { before });
            } else {
                throw new Error(`before(id=${before.id}) is not in the context menu`);
            }
        } else {
            throw new Error(`parent(id=${parent.id}) is not in the contex menu`);
        }
    }

    /**
     * @param { MenuItem } menuItem
     * @param { MenuItem } parent
     * @param { { selector?: string, coreAsWell: boolean } } options
     */
    moveToSubmenu(menuItem, parent = null, options = null) {
        let oldParent = menuItem.parentElement;
        if (oldParent instanceof MenuItemList) {
            if (this.contains(oldParent)) {
                // Assuming parameters are always correct since this is an internal function
                if (parent !== null) {
                    if (this.contains(parent)) {
                        oldParent._detachImmediateMenuItem(menuItem);
                        parent.appendSubmenuItem(menuItem);
                    } else {
                        throw new Error(`parent(id=${parent.id}) is not in the context menu`);
                    }
                } else {
                    if (options !== null) {
                        menuItem.selector = options.selector;
                        menuItem.coreAsWell = options.coreAsWell;
                    }

                    oldParent._detachImmediateMenuItem(menuItem);
                    this.appendMenuItem(menuItem);
                }
            } else {
                throw new Error(`parent of the menu item(id=${oldParent.id}) is not in the context menu`);
            }
        } else {
            throw new Error(`current parent(id=${oldParent.id}) is not a submenu`);
        }

    }

    ensureDoesntContain(id) {
        let elem = document.getElementById(id);
        if (typeof elem !== 'undefined' && this.contains(elem)) {
            throw new Error(`There is already an element with id=${id} in the context menu`);
        }
    }

    static define() {
        defineCustomElement('ctx-menu', ContextMenu, 'div');
    }
}
