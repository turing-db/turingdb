import PrismCode from "../../../Components/Common/Prism";

// Default Offcanvas

const defaultOffcanvasCode =
    `
<!-- Base Examples -->
const [open, setOpen] = useState(false);

const toggleDefultCanvas = () => {
    setOpen(!open);
};
<div className="hstack flex-wrap gap-2">
    <Link to="#" className="btn btn-secondary" onClick={toggleDefultCanvas}>
        Link with href
    </Link>
    <Button color="secondary" onClick={toggleDefultCanvas}>
        Button with target
    </Button>
</div>

<Offcanvas isOpen={open} toggle={toggleDefultCanvas} id="offcanvasExample">
    <OffcanvasHeader toggle={toggleDefultCanvas} id="offcanvasExampleLabel">
        Recent Acitivity
    </OffcanvasHeader>
    <OffcanvasBody>
        ...
    </OffcanvasBody>
</Offcanvas>
`;

const DefaultOffcanvasExample = () => (
    <PrismCode
        code={defaultOffcanvasCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Placement Offcanvas
const placementOffcanvasCode =
    `
<!-- Placement Offcanvas -->

const [isTop, setIsTop] = useState(false);
const toggleTopCanvas = () => {
    setIsTop(!isTop);
};

const [isRight, setIsRight] = useState(false);
const toggleRightCanvas = () => {
    setIsRight(!isRight);
};

const [isBottom, setIsBottom] = useState(false);
const toggleBottomCanvas = () => {
    setIsBottom(!isBottom);
};

const [isLeft, setIsLeft] = useState(false);
const toggleLeftCanvas = () => {
    setIsLeft(!isLeft);
};

<div className="d-flex flex-wrap gap-2">
    <Button color="primary" onClick={toggleTopCanvas}>Toggle Top Offcanvas</Button>
    <Button color="secondary" onClick={toggleRightCanvas}>Toggle Right Offcanvas</Button>
    <Button color="success" onClick={toggleBottomCanvas}>Toggle Bottom Offcanvas</Button>
    <Button color="danger" onClick={toggleLeftCanvas}>Toggle Left Offcanvas</Button>
</div>

<!-- top offcanvas -->
<Offcanvas isOpen={isTop} toggle={toggleTopCanvas} id="offcanvasTop">
    <OffcanvasHeader toggle={toggleTopCanvas} id="offcanvasTopLabel">
        Gallery
    </OffcanvasHeader>
    <OffcanvasBody>
        ...
    </OffcanvasBody>
</Offcanvas>

<!-- right offcanvas -->
<Offcanvas isOpen={isRight} toggle={toggleRightCanvas} id="offcanvasRight">
    <OffcanvasHeader toggle={toggleRightCanvas} id="offcanvasRightLabel">
        Recent Acitivity
    </OffcanvasHeader>
    <OffcanvasBody>
        ...
    </OffcanvasBody>
</Offcanvas>


<!-- bottom offcanvas -->
<Offcanvas isOpen={isBottom} toggle={toggleBottomCanvas} id="offcanvasBottom">
    <OffcanvasHeader toggle={toggleBottomCanvas} id="offcanvasBottomLabel">
        Gallery
    </OffcanvasHeader>
    <OffcanvasBody>
        ...
    </OffcanvasBody>
</Offcanvas>

<!-- left offcanvas -->
<Offcanvas isOpen={isLeft} toggle={toggleLeftCanvas} id="offcanvasLeft">
    <OffcanvasHeader toggle={toggleLeftCanvas} id="offcanvasLeftLabel">
        Recent Acitivity
    </OffcanvasHeader>
    <OffcanvasBody>
        ...
    </OffcanvasBody>
</Offcanvas>
`;

const PlacementOffcanvasExample = () => (
    <PrismCode
        code={placementOffcanvasCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Backdrop Offcanvas

const backdropOffcanvasCode =
    `
<!-- Backdrop Offcanvas -->

const [isEnableScroll, setIsEnableScroll] = useState(false);
const toggleEnableScroll = () => {
    setIsEnableScroll(!isEnableScroll);
};

const [isBackdrop, setIsBackdrop] = useState(false);
const toggleBackdrop = () => {
    setIsBackdrop(!isBackdrop);
};

const [isScrollBackDrop, setIsScrollBackDrop] = useState(false);
const toggleScrollBackDrop = () => {
    setIsScrollBackDrop(!isScrollBackDrop);
};

<div className="d-flex flex-wrap gap-2">
    <Button className="btn btn-light" onClick={toggleEnableScroll}>Enable Body Scrolling</Button>
    <Button className="btn btn-light" onClick={toggleBackdrop}>Enable Backdrop (Default)</Button>
    <Button className="btn btn-light" onClick={toggleScrollBackDrop}>Enable Both Scrolling & Backdrop</Button>
</div>    

<Offcanvas isOpen={isEnableScroll} toggle={toggleEnableScroll} id="offcanvasScrolling">
    <OffcanvasHeader toggle={toggleEnableScroll} id="offcanvasScrollingLabel">
        Customer Activity
    </OffcanvasHeader>
    <OffcanvasBody>
        ...
    </OffcanvasBody>
</Offcanvas>

<Offcanvas isOpen={isBackdrop} toggle={toggleBackdrop} id="offcanvasWithBackdrop">
    <OffcanvasHeader toggle={toggleBackdrop} id="offcanvasWithBackdropLabel">
        Customer Activity
    </OffcanvasHeader>
    <OffcanvasBody>
        ...
    </OffcanvasBody>
</Offcanvas>

<Offcanvas isOpen={isScrollBackDrop} toggle={toggleScrollBackDrop} id="offcanvasWithBothOptions" scrollable>
    <OffcanvasHeader toggle={toggleScrollBackDrop} id="offcanvasWithBothOptionsLabel">
        Customer Activity
    </OffcanvasHeader>
    <OffcanvasBody>
        ...
    </OffcanvasBody>
</Offcanvas>
`;

const BackdropOffcanvasExample = () => (
    <PrismCode
        code={backdropOffcanvasCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { DefaultOffcanvasExample, PlacementOffcanvasExample, BackdropOffcanvasExample };