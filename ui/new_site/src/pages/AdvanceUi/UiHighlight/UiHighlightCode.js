import PrismCode from "../../../Components/Common/Prism";

// HTML Highlight

const htmlHighlightCode =
    `
<!DOCTYPE html>
<html>
    <head>
        <title>Velzon - Responsive Admin Dashboard Template</title>
    </head>
    <body>
        <div>
            <h1>This is a Heading 1</h1>
            <h2>This is a Heading 2</h2>
            <h3>This is a Heading 3</h3>
            <h4>This is a Heading 4</h4>
        </div>
        <!-- end div content -->
    </body>
</html>
`;

const HtmlHighlightExample = () => (
    <PrismCode
        code={htmlHighlightCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// CSS Highlight

const cssHighlightCode =
`body {
    color: #212529; 
    background-color: #f3f3f9;
    font-family: "Poppins",sans-serif;
}

.example {
    margin: 0;
    color: #74788d; 
}
`;

const CssHighlightExample = () => (
    <PrismCode
        code={cssHighlightCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Javascript Highlight

const javaScriptCode =
`function myFunction() {
    var divElement = document.getElementById("myDIV");
    if (divElement.style.display === "none") {
      divElement.style.display = "block";
    } else {
      divElement.style.display = "none";
    }
}
`;

const JavaScriptExample = () => (
    <PrismCode
        code={javaScriptCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { HtmlHighlightExample, CssHighlightExample, JavaScriptExample };