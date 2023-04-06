import PrismCode from "../../../Components/Common/Prism";

// Default Placeholder

const defaultPlaceholderCode =
    `
<!-- Base Examples -->
<Card>
<img src={img1} className="card-img-top" alt="card img" />
    <CardBody>
        <h5 className="card-title">Card title</h5>
        <p className="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
        <Link to="#" className="btn btn-primary">Go somewhere</Link>
    </CardBody>
</Card>

<Card>
    <img src={img2} className="card-img-top" alt="card dummy img" />
    <CardBody>
        <h5 className="card-title placeholder-glow">
            <span className="placeholder col-6"></span>
        </h5>
        <p className="card-text placeholder-glow">
            <span className="placeholder col-7"></span>
            <span className="placeholder col-4"></span>
            <span className="placeholder col-4"></span>
            <span className="placeholder col-6"></span>
        </p>
        <Link to="#" tabIndex="-1" className="btn btn-primary disabled placeholder col-6"></Link>
    </CardBody>
</Card>

`;

const DefaultPlaceholderExample = () => (
    <PrismCode
        code={defaultPlaceholderCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Width

const widthCode =
    `
<!-- Width Sizing-->
<div className="live-preview">
    <span className="placeholder col-6"></span>
    <span className="placeholder w-75"></span>
    <span className="placeholder" style={{width: "25%"}}></span>
</div>
`;

const WidthExample = () => (
    <PrismCode
        code={widthCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Sizing

const sizingCode =
    `
<!-- Sizing -->
<span className="placeholder col-12 placeholder-lg"></span>

<span className="placeholder col-12"></span>

<span className="placeholder col-12 placeholder-sm"></span>

<span className="placeholder col-12 placeholder-xs"></span>
`;

const SizingExample = () => (
    <PrismCode
        code={sizingCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Color

const colorCode =
    `
<!-- Color -->
<span className="placeholder col-12 mb-3"></span>

<span className="placeholder col-12 mb-3 bg-primary"></span>

<span className="placeholder col-12 mb-3 bg-secondary"></span>

<span className="placeholder col-12 mb-3 bg-success"></span>

<span className="placeholder col-12 mb-3 bg-danger"></span>

<span className="placeholder col-12 mb-3 bg-warning"></span>

<span className="placeholder col-12 mb-3 bg-info"></span>

<span className="placeholder col-12 mb-3 bg-light"></span>

<span className="placeholder col-12 mb-3 bg-dark"></span>
`;

const ColorExample = () => (
    <PrismCode
        code={colorCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { DefaultPlaceholderExample, WidthExample, SizingExample, ColorExample };