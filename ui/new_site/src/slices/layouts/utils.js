/**
 * Changes the body attribute
 */
 const changeHTMLAttribute = (attribute, value) => {
    if (document.documentElement) document.documentElement.setAttribute(attribute, value);
    return true;
}
export { changeHTMLAttribute };