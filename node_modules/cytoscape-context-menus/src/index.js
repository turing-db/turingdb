// We have to use CommonJS here https://stackoverflow.com/questions/33505992/babel-6-changes-how-it-exports-default
let { contextMenus } = require('./cytoscape-context-menus.js');

let register = function(cytoscape) {
    if (!cytoscape) { 
        return; 
    } // can't register if cytoscape unspecified

    cytoscape('core', 'contextMenus', contextMenus);
};

// @ts-ignore
if (typeof cytoscape !== 'undefined') {
    // Register for plain javascript
    // @ts-ignore
    register(cytoscape);
}

module.exports = register;