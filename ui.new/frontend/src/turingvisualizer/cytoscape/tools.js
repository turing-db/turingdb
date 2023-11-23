export const getFitViewport = (cy, elements, padding = 0) => {
  const bb = elements.boundingBox();
  const vp = cy.viewport();

  let w = vp.width();
  let h = vp.height();
  let zoom;

  zoom = Math.min((w - 2 * padding) / bb.w, (h - 2 * padding) / bb.h);

  // crop zoom
  zoom = zoom > vp.maxZoom ? vp.maxZoom : zoom;
  zoom = zoom < vp.minZoom ? vp.minZoom : zoom;

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

export const fit = (cy, elements, padding = 0) => {
  const viewport = getFitViewport(cy, elements, padding);
  viewport.zoom *= 0.9;
  cy.viewport(viewport);
};
