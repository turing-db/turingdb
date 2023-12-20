import cytoscape from "cytoscape";

export const getFitViewport = (
  cy: cytoscape.Core,
  elements: cytoscape.Collection,
  padding = 0
) => {
  const bb = elements.boundingBox();

  let w = cy.width();
  let h = cy.height();
  let zoom: number;

  zoom = Math.min((w - 2 * padding) / bb.w, (h - 2 * padding) / bb.h);

  // crop zoom
  zoom = zoom > cy.maxZoom() ? cy.maxZoom() : zoom;
  zoom = zoom < cy.minZoom() ? cy.minZoom() : zoom;

  let pan = {
    // now pan to middle
    x: (w - zoom * (bb.x1 + bb.x2)) / 2,
    y: (h - zoom * (bb.y1 + bb.y2)) / 2,
  };

  return {
    zoom: zoom,
    pan: pan,
  };
};

export const fit = (
  cy: cytoscape.Core,
  elements: cytoscape.Collection,
  padding = 0
) => {
  const viewport = getFitViewport(cy, elements, padding);
  viewport.zoom *= 0.9;
  cy.viewport(viewport);
};
