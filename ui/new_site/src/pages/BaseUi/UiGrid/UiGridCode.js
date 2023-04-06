import PrismCode from "../../../Components/Common/Prism";

// Vertical alignment (align-items-start)

const verticalStartCode =
    `
<!-- Vertical alignment (align-items-start) -->
<Row className="align-items-start">
    <Col sm={4}>
        ...
    </Col>
    <Col sm={4}>
        ...
    </Col>
    <Col sm={4}>
        ...
    </Col>
</Row>
`;

const VerticalStartExample = () => (
    <PrismCode
        code={verticalStartCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Vertical alignment (align-items-center)

const verticalCenterCode =
    `
<!-- Vertical alignment (align-items-center) -->
<Row className="align-items-center">
    <Col sm={4}>
        ...
    </Col>
    <Col sm={4}>
        ...
    </Col>
    <Col sm={4}>
        ...
    </Col>
</Row>
`;

const VerticalCenterExample = () => (
    <PrismCode
        code={verticalCenterCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Vertical alignment (align-items-end)

const verticalEndCode =
    `
<!-- Vertical alignment (align-items-end) -->
<Row className="align-items-end">
    <Col sm={4}>
        ...
    </Col>
    <Col sm={4}>
        ...
    </Col>
    <Col sm={4}>
        ...
    </Col>
</Row>
`;

const VerticalEndExample = () => (
    <PrismCode
        code={verticalEndCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Align Self

const alignSelfCode =
    `
<!-- Align Self -->
<Row>
    <Col sm={4} className="align-self-start">
        ...
    </Col>
    <Col sm={4} className="align-self-center">
        ...
    </Col>
    <Col sm={4} className="align-self-end">
        ...
    </Col>
</Row>
`;

const AlignSelfExample = () => (
    <PrismCode
        code={alignSelfCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Horizontal Alignment

const horizontalAlignCode =
    `
<!-- Horizontal Alignment -->
<Row className="justify-content-start">
    <Col sm={4}>
        ...
    </Col>
</Row>
<Row className="justify-content-center">
    <Col sm={4}>
        ...
    </Col>
</Row>
<Row className="justify-content-end">
<Col sm={4}>
        ...
</Col>
</Row>
`;

const HorizontalAlignExample = () => (
    <PrismCode
        code={horizontalAlignCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


export { VerticalStartExample, VerticalCenterExample, VerticalEndExample, AlignSelfExample, HorizontalAlignExample };