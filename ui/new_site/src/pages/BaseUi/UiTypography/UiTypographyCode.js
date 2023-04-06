import PrismCode from "../../../Components/Common/Prism";

// Display Headings

const displayHeadingCode =
    `
<!-- Display Headings -->
<h1 className="display-1">Display 1</h1>

<h1 className="display-2">Display 2</h1>

<h1 className="display-3">Display 3</h1>

<h1 className="display-4">Display 4</h1>

<h1 className="display-5">Display 5</h1>

<h1 className="display-6">Display 6</h1>
`;

const DisplayHeadingExample = () => (
    <PrismCode
        code={displayHeadingCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Headings

const headingsCode =
    `
<!-- Headings -->
<h1 className="mb-3">h1. Bootstrap heading <small className="text-muted">Semibold 2.03125rem (32.5px)</small></h1>

<h2 className="mb-3">h2. Bootstrap heading <small className="text-muted">Semibold 1.625rem (26px)</small></h2>

<h3 className="mb-3">h3. Bootstrap heading <small className="text-muted">Semibold 1.42188rem (22.8px)</small></h3>

<h4 className="mb-3">h4. Bootstrap heading <small className="text-muted">Semibold 1.21875rem (19.5px)</small></h4>

<h5 className="mb-3">h5. Bootstrap heading <small className="text-muted">Semibold 1.01563rem (16.25px)</small></h5>

<h6 className="mb-1">h6. Bootstrap heading <small className="text-muted">Semibold 0.8125rem (13px)</small></h6>
`;

const HeadingsExample = () => (
    <PrismCode
        code={headingsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Blockquotes

const blockquotesCode =
    `
<!-- Blockquotes -->
<blockquote className="blockquote fs-14">
    <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer posuere erat a ante.</p>
    <footer className="blockquote-footer mt-0">Someone famous in <cite title="Source Title">Source Title</cite></footer>
</blockquote>

<blockquote className="blockquote blockquote-reverse fs-14">
    <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer posuere erat a ante.</p>
    <footer className="blockquote-footer mt-0">Someone famous in <cite title="Source Title">Source Title</cite></footer>
</blockquote>
`;

const BlockquotesExample = () => (
    <PrismCode
        code={blockquotesCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Blockquote Background Color

const blockquoteColorCode =
    `
<!-- Blockquote Background Color -->
<blockquote className="blockquote custom-blockquote blockquote-primary rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Primary Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-secondary rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Secondary Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-success rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Success Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-info rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Info Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-danger rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Danger Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-warning rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Warning Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-dark rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Dark Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>
`;

const BlockquoteColorExample = () => (
    <PrismCode
        code={blockquoteColorCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Blockquote Border Color

const blockquoteBorderColorCode =
    `
<!-- Blockquote Border Color -->
<blockquote className="blockquote custom-blockquote blockquote-outline blockquote-primary rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Outline Primary Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-outline blockquote-secondary rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Outline Secondary Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-outline blockquote-success rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Outline Success Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-outline blockquote-info rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Outline Info Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-outline blockquote-danger rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Outline Danger Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-outline blockquote-warning rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Outline Warning Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>

<blockquote className="blockquote custom-blockquote blockquote-outline blockquote-dark rounded mb-0">
    <p className="text-dark mb-2">Custom Blockquote Outline Dark Example</p>
    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
</blockquote>
`;

const BlockquoteBorderColorExample = () => (
    <PrismCode
        code={blockquoteBorderColorCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Inline Text Elements

const inlineTextCode =
    `
<!-- Inline Text -->
<p className="lead">Vivamus sagittis lacus vel augue laoreet rutrum faucibus dolor auctor.</p>

<p>You can use the mark tag to <mark>highlight</mark> text.</p>

<p><del>This line of text is meant to be treated as deleted text.</del></p>

<p><s>This line of text is meant to be treated as no longer accurate.</s></p>

<p><ins>This line of text is meant to be treated as an addition to the document.</ins></p>

<p><u>This line of text will render as underlined</u></p>

<p><small>This line of text is meant to be treated as fine print.</small></p>

<p><strong>This line rendered as bold text.</strong></p>

<p><em>This line rendered as italicized text.</em></p>
`;

const InlineTextExample = () => (
    <PrismCode
        code={inlineTextCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Unstyled List

const unstyleListCode =
    `
<!-- Unstyle List -->
<ul className="list-unstyled">
    <li>Integer molestie lorem at massa</li>
    <li>Nulla volutpat aliquam velit
        <ul>
            <li>Phasellus iaculis neque</li>
            <li>Purus sodales ultricies</li>
            <li>Vestibulum laoreet porttitor sem</li>
        </ul>
    </li>
    <li>Faucibus porta lacus fringilla vel</li>
</ul>
`;

const UnstyleListExample = () => (
    <PrismCode
        code={unstyleListCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Inline List

const inlineListCode =
    `
<!-- Inline List -->
<ul className="list-inline">
    <li className="list-inline-item">Lorem ipsum</li>
    <li className="list-inline-item">Phasellus iaculis</li>
    <li className="list-inline-item">Nulla volutpat</li>
</ul>
`;

const InlineListExample = () => (
    <PrismCode
        code={inlineListCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Description List Alignment

const descriptionListCode =
    `
<!-- Description List -->
<dl className="row mb-0">
    <dt className="col-sm-3">Description lists</dt>
    <dd className="col-sm-9">A description list is perfect for defining terms.</dd>

    <dt className="col-sm-3">Euismod</dt>
    <dd className="col-sm-9">Vestibulum id ligula porta felis euismod semper eget lacinia odio sem nec elit.</dd>
    <dd className="col-sm-9 offset-sm-3">Donec id elit non mi porta gravida at eget metus.</dd>

    <dt className="col-sm-3">Malesuada porta</dt>
    <dd className="col-sm-9">Etiam porta sem malesuada magna mollis euismod.</dd>

    <dt className="col-sm-3 text-truncate">Truncated term is truncated</dt>
    <dd className="col-sm-9">Fusce dapibus, tellus ac cursus commodo, tortor mauris condimentum nibh, ut fermentum massa justo sit amet risus.</dd>

    <dt className="col-sm-3">Nesting</dt>
    <dd className="col-sm-9 mb-0">
        <dl className="row mb-0">
            <dt className="col-sm-4">Nested definition list</dt>
            <dd className="col-sm-8">Aenean posuere, tortor sed cursus feugiat, nunc augue blandit nunc.</dd>
        </dl>
    </dd>
</dl>
`;

const DescriptionListExample = () => (
    <PrismCode
        code={descriptionListCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Text Wrapping and Overflow

const textWrappingCode =
    `
<!-- Text Wrap -->
<Badge color="primary" className="text-wrap" style={{ width: "6rem" }}>
    This text should wrap.
</Badge>

<!-- Text nowrap -->
<div className="text-nowrap border bg-light mt-3" style={{width: "8rem"}}>
    This text should overflow the parent.
</div>
`;

const TextWrappingExample = () => (
    <PrismCode
        code={textWrappingCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Font size

const fontSizeCode =
    `
<!-- Font size -->
<p className="fs-1">.fs-1 text</p>

<p className="fs-2">.fs-2 text</p>

<p className="fs-3">.fs-3 text</p>

<p className="fs-4">.fs-4 text</p>

<p className="fs-5">.fs-5 text</p>

<p className="fs-6">.fs-6 text</p>
`;

const FontSizeExample = () => (
    <PrismCode
        code={fontSizeCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Word Break

const wordBreakCode =
    `
<!-- Word Break -->
<p className="text-break mb-0">mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm</p>;
`;

const WordBreakExample = () => (
    <PrismCode
        code={wordBreakCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Font weight and italics

const fontWeightCode =
    `
<!-- Font weight and italics -->
<p className="fw-bold">Bold text.</p>

<p className="fw-bolder">Bolder weight text (relative to the parent element).</p>

<p className="fw-normal">Normal weight text.</p>

<p className="fw-light">Light weight text.</p>

<p className="fw-lighter">Lighter weight text (relative to the parent element).</p>

<p className="fst-italic">Italic text.</p>

<p className="fst-normal mb-0">Text with normal font style</p>
`;

const FontWeightExample = () => (
    <PrismCode
        code={fontWeightCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Line height

const lineHeightCode =
    `
<!-- Line height -->
<p className="lh-1">This is a long paragraph written to show how the line-height of an element is affected by our utilities. Classes are applied to the element itself or sometimes the parent element. These classes can be customized as needed with our utility API.</p>

<p className="lh-sm">This is a long paragraph written to show how the line-height of an element is affected by our utilities. Classes are applied to the element itself or sometimes the parent element. These classes can be customized as needed with our utility API.</p>

<p className="lh-base">This is a long paragraph written to show how the line-height of an element is affected by our utilities. Classes are applied to the element itself or sometimes the parent element.</p>

<p className="lh-lg mb-0">This is a long paragraph written to show how the line-height of an element is affected by our utilities. Classes are applied to the element itself or sometimes the parent element.</p>
`;

const LineHeightExample = () => (
    <PrismCode
        code={lineHeightCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Text Transform

const textTransformCode =
    `
<!-- Text Transform -->
<p className="text-lowercase">Lowercased text.</p>

<p className="text-uppercase">Uppercased text.</p>

<p className="text-capitalize mb-0">CapiTaliZed text.</p>
`;

const TextTransformExample = () => (
    <PrismCode
        code={textTransformCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Text Decoration

const textDecrationCode =
    `
<!-- Text Decoration -->
<p className="text-decoration-underline">This text has a line underneath it.</p>

<p className="text-decoration-line-through">This text has a line going through it.</p>

<Link to="#" className="text-decoration-none">This link has its text decoration removed</Link>
`;

const TextDecrationExample = () => (
    <PrismCode
        code={textDecrationCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Text Truncation

const textTruncationCode =
    `
<!-- Block level -->
<Row>
    <div className="col-2 text-truncate">
    This text is quite long, and will be truncated once displayed.
    </div>
</Row>

<!-- Inline level -->
<span className="d-inline-block text-truncate" style={{maxWidth: "150px"}}>
    This text is quite long, and will be truncated once displayed.
</span>
`;

const TextTruncationExample = () => (
    <PrismCode
        code={textTruncationCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Visibility

const visibilityCode =
    `
<!-- Visibility -->
<div className="visible">text visible Lorem ipsum dolor sit amet</div>

<div className="invisible">text invisible Lorem ipsum dolor sit amet</div>
`;

const VisibilityExample = () => (
    <PrismCode
        code={visibilityCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Clearfix

const clearfixCode =
    `
<!-- Clearfix -->
<div className="bg-light clearfix">
    <Button color="secondary" className="float-start">Example Button floated left</Button>
    <Button color="success" className="float-end">Example Button floated right</Button>
</div>
`;

const ClearfixExample = () => (
    <PrismCode
        code={clearfixCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


export { DisplayHeadingExample, HeadingsExample, BlockquotesExample, BlockquoteColorExample, BlockquoteBorderColorExample, InlineTextExample, UnstyleListExample, InlineListExample, DescriptionListExample, TextWrappingExample, FontSizeExample, WordBreakExample, FontWeightExample, LineHeightExample, TextTransformExample, TextDecrationExample, TextTruncationExample, VisibilityExample, ClearfixExample };