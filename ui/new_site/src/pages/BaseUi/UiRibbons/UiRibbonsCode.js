import PrismCode from "../../../Components/Common/Prism";

// Rounded Ribbon

const roundedRibbonCode =
    `
<!-- Rounded Ribbon -->
<Card className="ribbon-box border shadow-none mb-lg-0">
    <CardBody>
        <div className="ribbon ribbon-primary round-shape">Primary</div>
            <h5 className="fs-14 text-end">Rounded Ribbon</h5>
        <div className="ribbon-content mt-4 text-muted">
            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
        </div>
    </CardBody>
</Card>

<Card className="ribbon-box border shadow-none mb-lg-0">
    <CardBody>
        <div className="ribbon ribbon-success round-shape">Success</div>
            <h5 className="fs-14 text-end">Rounded Ribbon</h5>
        <div className="ribbon-content mt-4 text-muted">
            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
        </div>
    </CardBody>
</Card>

<!-- Right Ribbon -->
<Card className="ribbon-box border shadow-none right mb-lg-0">
    <CardBody>
        <div className="ribbon ribbon-info round-shape">Info</div>
            <h5 className="fs-14 text-start">Rounded Ribbon Right</h5>
        <div className="ribbon-content mt-4 text-muted">
            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
        </div>
    </CardBody>
</Card>
`;

const RoundedRibbonExample = () => (
    <PrismCode
        code={roundedRibbonCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Ribbon Shape

const ribbonShapeCode =
    `
<!-- Ribbon Shape -->
<Card className="ribbon-box border shadow-none mb-lg-0">
    <CardBody>
        <div className="ribbon ribbon-primary ribbon-shape">Primary</div>
        <h5 className="fs-14 text-end">Ribbon Shape</h5>
        <div className="ribbon-content text-muted mt-4">
            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
        </div>
    </CardBody>
</Card>

<Card className="ribbon-box border shadow-none mb-lg-0">
    <CardBody>
        <div className="ribbon ribbon-success ribbon-shape">Success</div>
        <h5 className="fs-14 text-end">Ribbon Shape</h5>
        <div className="ribbon-content text-muted mt-4">
            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
        </div>
    </CardBody>
</Card>

<Card className="ribbon-box border shadow-none mb-lg-0 right">
    <CardBody>
        <div className="ribbon ribbon-info ribbon-shape">Info</div>
        <h5 className="fs-14 text-start">Ribbon Shape Right</h5>
        <div className="ribbon-content text-muted mt-4">
            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
        </div>
    </CardBody>
</Card>
`;

const RibbonShapeExample = () => (
    <PrismCode
        code={ribbonShapeCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Filled Ribbons

const filledRibbonsCode =
    `
<!-- Filled Ribbons -->
<Card className="ribbon-box border ribbon-fill shadow-none mb-lg-0">
    <CardBody>
        <div className="ribbon ribbon-primary">- 10 %</div>
        <h5 className="fs-14 text-end">Fill Ribbon</h5>
        <div className="ribbon-content mt-5 text-muted">
            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
        </div>
    </CardBody>
</Card>

<Card className="ribbon-box border ribbon-fill shadow-none mb-lg-0">
    <CardBody>
        <div className="ribbon ribbon-success">- 20 %</div>
        <h5 className="fs-14 text-end">Fill Ribbon</h5>
        <div className="ribbon-content mt-5 text-muted">
            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
        </div>
    </CardBody>
</Card>

<!-- Right Ribbon -->
<Card className="ribbon-box border ribbon-fill shadow-none right mb-lg-0">
    <CardBody>
        <div className="ribbon ribbon-info">- 30 %</div>
        <h5 className="fs-14 text-start">Fill Ribbon Right</h5>
        <div className="ribbon-content mt-5 text-muted">
            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
        </div>
    </CardBody>
</div>
`;

const FilledRibbonsExample = () => (
    <PrismCode
        code={filledRibbonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Boxed Ribbons

const boxedRibbonsCode =
    `
<!-- Boxed Ribbons -->
<Card className="ribbon-box border shadow-none mb-lg-0">
    <CardBody className="text-muted">
        <div className="ribbon-two ribbon-two-primary"><span>Primary</span></div>
        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
    </CardBody>
</Card>

<Card className="ribbon-box border shadow-none mb-lg-0">
    <CardBody className="text-muted">
        <div className="ribbon-two ribbon-two-success"><span>Success</span></div>
        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
    </CardBody>
</Card>

<Card className="ribbon-box border shadow-none mb-lg-0">
    <CardBody className="text-muted">
        <div className="ribbon-two ribbon-two-success"><span>Success</span></div>
        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
    </CardBody>
</Card>
`;

const BoxedRibbonsExample = () => (
    <PrismCode
        code={boxedRibbonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Ribbons

const ribbonsCode =
    `
<!-- Ribbons -->
<Card className="ribbon-box border shadow-none mb-lg-0">
    <CardBody className="text-muted">
        <span className="ribbon-three ribbon-three-primary"><span>Primary</span></span>
        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
    </CardBody>
</Card>

<Card className="ribbon-box border shadow-none mb-lg-0">
    <CardBody className="text-muted">
        <div className="ribbon-three ribbon-three-success"><span>Success</span></div>
        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
    </CardBody>
</Card>

<Card className="ribbon-box border shadow-none mb-lg-0">
    <CardBody className="text-muted">
        <div className="ribbon-three ribbon-three-info"><span>Success</span></div>
        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
    </CardBody>
</Card>
`;

const RibbonsExample = () => (
    <PrismCode
        code={ribbonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Ribbons Hover

const ribbonsHoverCode =
    `
<!-- Ribbons Hover -->
<Card className="ribbon-box border shadow-none overflow-hidden">
    <CardBody className="text-muted">
        <div className="ribbon ribbon-info ribbon-shape trending-ribbon">
            <span className="trending-ribbon-text">Trending</span> <i className="ri-flashlight-fill text-white align-bottom float-end ms-1"></i>
        </div>
        <h5 className="fs-14 text-end mb-3">Ribbon Shape</h5>
        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
    </div>
</Card>

<Card className="ribbon-box border shadow-none overflow-hidden">
    <CardBody className="text-muted">
        <div className="ribbon ribbon-info ribbon-shape trending-ribbon">
            <span className="trending-ribbon-text">Trending</span> <i className="ri-flashlight-fill text-white align-bottom float-end ms-1"></i>
        </div>
        <h5 className="fs-14 text-end mb-3">Ribbon Shape</h5>
        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
    </div>
</Card>

<Card className="ribbon-box right border shadow-none overflow-hidden">
    <CardBody className="text-muted">
        <div className="ribbon ribbon-info ribbon-shape trending-ribbon">
            <i className="ri-flashlight-fill text-white align-bottom float-start me-1"></i> <span className="trending-ribbon-text">Trending</span>
        </div>
        <h5 className="fs-14 mb-3">Ribbon Right Shape</h5>
        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
    </div>
</Card>
`;

const RibbonsHoverExample = () => (
    <PrismCode
        code={ribbonsHoverCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { RoundedRibbonExample, RibbonShapeExample, FilledRibbonsExample, BoxedRibbonsExample, RibbonsExample, RibbonsHoverExample };