// Utility functions that are not directly related with the extension
export function getOffset(el) {
    let rect = el.getBoundingClientRect();

    return {
        top: rect.top,
        left: rect.left, 
    };
}

export function matches(el, selector) {
    return (el.matches || 
        el.matchesSelector || 
        el.msMatchesSelector || 
        el.mozMatchesSelector || 
        el.webkitMatchesSelector || 
        el.oMatchesSelector).call(el, selector);
}

export function isElementHidden(elem) {
    return elem.offsetWidth <= 0 && elem.offsetHeight <= 0 ||
        ((elem.style && elem.style.display) || getComputedStyle(elem)['display']);
}

export function isElementVisible(elem) {
    return !isElementHidden(elem);
}
// Merge default options with the ones coming from parameter
export function extend(defaults, options) {
    let obj = {};

    for (let i in defaults) {
      obj[i] = defaults[i];
    }

    for (let i in options) {
      // Arrays should be merged
      if (obj[i] instanceof Array) {
        obj[i] = obj[i].concat(options[i]);
      } else {
        obj[i] = options[i];
      }
    }

    return obj;
}

// Get string representation of css classes
export function getClassStr(classes) {
    let str = '';

    for (let i = 0; i < classes.length; i++) {
      let className = classes[i];
      str += className;
      if (i !== classes.length - 1) {
        str += ' ';
      }
    }

    return str;
}

export function preventDefaultContextTap() {
    let contextMenuAreas = document.getElementsByClassName('cy-context-menus-cxt-menu');

    for (const cxtMenuArea of contextMenuAreas) {
      cxtMenuArea.addEventListener('contextmenu', e => e.preventDefault());
    }
}

/**
 * https://stackoverflow.com/a/38057647/12045421
 * 
 * @param { Element } element 
 * @param { string } attribute
 * @param { boolean } boolValue 
 */
export function setBooleanAttribute(element, attribute, boolValue) {
  if (boolValue) {
    element.setAttribute(attribute, '');
  } else {
    element.removeAttribute(attribute);
  }
}

/**
 * Returns true if the first parameter is inside the element
 * @param {*} param0 
 * @param { HTMLElement } element 
 */
export function isIn({ x, y }, element) {
  let rect = element.getBoundingClientRect();

  return x >= rect.left && 
        x <= rect.right &&
        y >= rect.top &&
        y <= rect.bottom;
}

/**
 * Get the dimensions from a hidden element
 * @param { HTMLElement } element
 */
export function getDimensionsHidden(element) {
  // Temporarily show the element
  element.style.opacity = "0";
  element.style.display = "block";

  let rect = element.getBoundingClientRect();

  // Hide back after getting the dimensions
  element.style.opacity = "1";
  element.style.display = "none";

  return rect;
}

/**
 * Defines a new custom html element
 * https://developer.mozilla.org/en-US/docs/Web/Web_Components/Using_custom_elements
 * @param { string } name 
 * @param { * } klass
 * @param { string } extendsType 
 */
export function defineCustomElement(name, klass, extendsType) {
  // We have to check otherwise it throws an exception if already added
  if (typeof customElements.get(name) === 'undefined') {
    customElements.define(name, klass, { extends: extendsType })
  }
}