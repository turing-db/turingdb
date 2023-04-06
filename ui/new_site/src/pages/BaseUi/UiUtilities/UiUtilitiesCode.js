import PrismCode from "../../../Components/Common/Prism";

// Stacks - Vertical

const stacksVerticalCode =
    `
<!-- Stacks - Vertical -->
<div className="vstack gap-3">
    <div className="bg-light border p-1 px-2">First item</div>
    <div className="bg-light border p-1 px-2">Second item</div>
    <div className="bg-light border p-1 px-2">Third item</div>
</div>

<div className="vstack gap-2">
    <Button color="primary">Save changes</Button>
    <Button color="secondary" outline>Cancel</Button>
</div>
`;

const StacksVerticalExample = () => (
    <PrismCode
        code={stacksVerticalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Stacks - Horizontal

const stacksHorizontalCode =
    `
<!-- Stacks - Horizontal -->
<div className="hstack gap-3">
    <div className="bg-light border p-1 px-2">First item</div>
    <div className="bg-light border p-1 px-2">Second item</div>
    <div className="bg-light border p-1 px-2">Third item</div>
</div>

<div className="hstack gap-3">
    <div className="bg-light border p-1 px-2">First item</div>
    <div className="bg-light border p-1 px-2 ms-auto">Second item</div>
    <div className="bg-light border p-1 px-2">Third item</div>
</div>

<div className="hstack gap-3">
    <div className="bg-light border p-1 px-2">First item</div>
    <div className="vr"></div>
    <div className="bg-light border p-1 px-2">Second item</div>
    
    <div className="bg-light border p-1 px-2 ms-auto">Third item</div>
</div>

<div className="hstack gap-3"> 
    <Input className="form-control me-auto" type="text" placeholder="Add your item here..." />
    <Button color="primary">Submit</Button>
    <div className="vr"></div>
    <Button color="secondary" outline>Reset</Button>
</div>
`;

const StacksHorizontalExample = () => (
    <PrismCode
        code={stacksHorizontalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Text Selection

const textSelectionCode =
    `
<!-- Text Selection -->
<p className="user-select-all">This paragraph will be entirely selected when clicked by the user.</p>

<p className="user-select-auto">This paragraph has default select behavior.</p>

<p className="user-select-none mb-0">This paragraph will not be selectable when clicked by the user.</p>
`;

const TextSelectionExample = () => (
    <PrismCode
        code={textSelectionCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Pointer Events

const pointerEventsCode =
    `
<!-- Pointer events -->
<p><Link to="#" className="text-success pe-none" tabIndex="-1" aria-disabled="true">This link</Link> can not be clicked.</p>

<p><Link to="#" className="text-success pe-auto">This link</Link> can be clicked (this is default behavior).</p>
    
<p className="pe-none mb-0"><Link to="#" className="text-success" tabindex="-1" aria-disabled="true">This link</Link> can not be clicked because the <code>pointer-events</code> property is inherited from its parent. However, <Link to="#" className="pe-auto">this link</Link> has a <code>pe-auto</code> class and can be clicked.</p>
`;

const PointerEventsExample = () => (
    <PrismCode
        code={pointerEventsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Overflow

const overflowCode =
    `
<!-- Overflow -->
    <div className="overflow-hidden p-3 mb-0 bg-light" style={{height: "100px"}}>
        This is an example of using <code>.overflow-hidden</code> on an element with set width and height dimensions. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
    </div>
    
    <div className="overflow-visible p-3 mb-0 bg-light" style={{height: "100px"}}>
        This is an example of using <code>.overflow-visible</code> on an element with set width and height dimensions.Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
    </div>
    
    <div className="overflow-scroll p-3 mb-0 bg-light" style={{height: "100px"}}>
        This is an example of using <code>.overflow-scroll</code> on an element with set width and height dimensions. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
    </div>
`;

const OverflowExample = () => (
    <PrismCode
        code={overflowCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Position

const positionCode =
    `
<!-- Arrange elements -->
<div className="position-relative p-5 bg-light m-3 border rounded" style={{height: "180px"}}>
    <div className="position-absolute top-0 start-0 avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-0 end-0 avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-50 start-50 avatar-xs bg-dark rounded"></div>
    <div className="position-absolute bottom-50 end-50 avatar-xs bg-dark rounded"></div>
    <div className="position-absolute bottom-0 start-0 avatar-xs bg-dark rounded"></div>
    <div className="position-absolute bottom-0 end-0 avatar-xs bg-dark rounded"></div>
</div>

<!-- Center elements -->
<div className="position-relative m-3 bg-light border rounded" style={{height: "180px"}}>
    <div className="position-absolute top-0 start-0 translate-middle avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-0 start-50 translate-middle avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-0 start-100 translate-middle avatar-xs bg-dark rounded"></div>

    <div className="position-absolute top-50 start-0 translate-middle avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-50 start-50 translate-middle avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-50 start-100 translate-middle avatar-xs bg-dark rounded"></div>

    <div className="position-absolute top-100 start-0 translate-middle avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-100 start-50 translate-middle avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-100 start-100 translate-middle avatar-xs bg-dark rounded"></div>
</div>

<!-- Center elements -->
    <div className="position-relative m-3 bg-light border rounded" style={{height: "180px"}}>
    <div className="position-absolute top-0 start-0 avatar-xs bg-dark rounded "></div>
    <div className="position-absolute top-0 start-50 translate-middle-x avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-0 end-0 avatar-xs bg-dark rounded"></div>

    <div className="position-absolute top-50 start-0 translate-middle-y avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-50 start-50 translate-middle avatar-xs bg-dark rounded"></div>
    <div className="position-absolute top-50 end-0 translate-middle-y avatar-xs bg-dark rounded"></div>

    <div className="position-absolute bottom-0 start-0 avatar-xs bg-dark rounded"></div>
    <div className="position-absolute bottom-0 start-50 translate-middle-x avatar-xs bg-dark rounded"></div>
    <div className="position-absolute bottom-0 end-0 avatar-xs bg-dark rounded"></div>
</div>
`;

const PositionExample = () => (
    <PrismCode
        code={positionCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Shadows

const shadowsCode =
    `
<!-- Shadows -->
<div className="shadow-none p-3 bg-light rounded">No shadow</div>

<div className="shadow-sm p-3 bg-light rounded">Small shadow</div>

<div className="shadow p-3 bg-light rounded">Regular shadow</div>

<div className="shadow-lg p-3 bg-light rounded">Larger shadow</div>
`;

const ShadowsExample = () => (
    <PrismCode
        code={shadowsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Width

const widthCode =
    `
<!-- Width -->
<div className="w-25 p-3 bg-light">Width 25%</div>

<div className="w-50 p-3 bg-light">Width 50%</div>

<div className="w-75 p-3 bg-light">Width 75%</div>

<div className="w-100 p-3 bg-light">Width 100%</div>

<div className="w-auto p-3 bg-light">Width auto</div>
`;

const WidthExample = () => (
    <PrismCode
        code={widthCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Height

const heightCode =
    `
<!-- Height -->
<div className="h-25 p-3 bg-light">Height25%</div>

<div className="h-50 p-3 bg-light">Height50%</div>

<div className="h-75 p-3 bg-light">Height75%</div>

<div className="h-100 p-3 bg-light">Height100%</div>

<div className="h-auto p-3 bg-light">Height auto</div>
`;

const HeightExample = () => (
    <PrismCode
        code={heightCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { StacksVerticalExample, StacksHorizontalExample, TextSelectionExample, PointerEventsExample, OverflowExample, PositionExample, ShadowsExample, WidthExample, HeightExample };