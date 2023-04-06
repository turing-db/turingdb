import PrismCode from "../../../Components/Common/Prism";

// Default Modal

const defaultModalCode =
    `
<!-- Default Modals -->

const [modal_standard, setmodal_standard] = useState(false);

    function tog_standard() {
        setmodal_standard(!modal_standard);
    }

<Button color="primary" onClick={() => tog_standard()}>Standard Modal</Button>

<Modal id="myModal"
        isOpen={modal_standard}
        toggle={() => {
            tog_standard();
        }}
>
    <ModalHeader>
        <h5
            className="modal-title"
            id="myModalLabel"
        >
            Modal Heading
        </h5>
        <Button
            type="button"
            className="btn-close"
            onClick={() => {
                setmodal_standard(false);
            }}
            aria-label="Close"
        >
        </Button>
    </ModalHeader>
    <ModalBody>
        <h5 className="fs-15">
            Overflowing text to show scroll behavior
        </h5>
        <p className="text-muted">One morning, when Gregor Samsa woke from troubled dreams, he found himself transformed in his bed into a horrible vermin. He lay on his armour-like back, and if he lifted his head a little he could see his brown belly, slightly domed and divided by arches into stiff sections.</p>
        <p className="text-muted">The bedding was hardly able to cover it and seemed ready to slide off any moment. His many legs, pitifully thin compared with the size of the rest of him, waved about helplessly as he looked. "What's happened to me?" he thought.</p>
        <p className="text-muted">It wasn't a dream. His room, a proper human room although a little too small, lay peacefully between its four familiar walls.</p>
    </ModalBody>
    <ModalFooter>
        <Button
            color="light"
            onClick={() => {
                tog_standard();
            }}
        >
            Close
        </Button>
        <Button
            color="primary"
        >
            Save changes
        </Button>
    </ModalFooter>
</Modal>
`;

const DefaultModalExample = () => (
    <PrismCode
        code={defaultModalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Vertically Centered Modal
const centeredModalCode =
    `
<!-- Vertically Centered -->

const [modal_center, setmodal_center] = useState(false);

    function tog_center() {
        setmodal_center(!modal_center);
    }

<Button color="primary" onClick={() => tog_center()}>Center Modal</Button>

<Modal
    isOpen={modal_center}
    toggle={() => {
        tog_center();
    }}
    centered
>
    ...
</Modal>
`;

const CenteredModalExample = () => (
    <PrismCode
        code={centeredModalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Grids in Modals
const gridsModalCode =
    `
<!-- Grids in modals -->

const [modal_grid, setmodal_grid] = useState(false);

    function tog_grid() {
        setmodal_grid(!modal_grid);
    }

<Button color="primary" onClick={() => setmodal_grid(true)}>
    Launch Demo Modal
</Button>

<Modal
    isOpen={modal_grid}
    toggle={() => {
        tog_grid();
    }}
>
    <ModalHeader>
        <h5 className="modal-title">
            Grid Modals
        </h5>
        <Button
            type="button"
            onClick={() => {
                setmodal_grid(false);
            }}
            className="btn-close"
            aria-label="Close"
        >
        </Button>
    </ModalHeader>
    <ModalBody>
        <form action="#">
            <div className="row g-3">
                <Col xxl={6}>
                    <div>
                        <label htmlFor="firstName" className="form-label">First Name</label>
                        <Input type="text" className="form-control" id="firstName" placeholder="Enter firstname" />
                    </div>
                </Col>
                <Col xxl={6}>
                    <div>
                        <label htmlFor="lastName" className="form-label">Last Name</label>
                        <Input type="text" className="form-control" id="lastName" placeholder="Enter lastname" />
                    </div>
                </Col>
                <div className="col-lg-12">
                    <label className="form-label">Gender</label>
                    <div>
                        <div className="form-check form-check-inline">
                            <Input className="form-check-input" type="radio" name="inlineRadioOptions" id="inlineRadio1" value="option1" />
                            <label className="form-check-label" htmlFor="inlineRadio1">Male</label>
                        </div>
                        <div className="form-check form-check-inline">
                            <Input className="form-check-input" type="radio" name="inlineRadioOptions" id="inlineRadio2" value="option2" />
                            <label className="form-check-label" htmlFor="inlineRadio2">Female</label>
                        </div>
                        <div className="form-check form-check-inline">
                            <Input className="form-check-input" type="radio" name="inlineRadioOptions" id="inlineRadio3" value="option3" />
                            <label className="form-check-label" htmlFor="inlineRadio3">Others</label>
                        </div>
                    </div>
                </div>
                <Col xxl={6}>
                    <label htmlFor="emailInput" className="form-label">Email</label>
                    <Input type="email" className="form-control" id="emailInput" placeholder="Enter your email" />
                </Col>
                <Col xxl={6}>
                    <label htmlFor="passwordInput" className="form-label">Password</label>
                    <Input type="password" className="form-control" id="passwordInput" value="451326546" />
                </Col>
                <div className="col-lg-12">
                    <div className="hstack gap-2 justify-content-end">
                        <Button color="light" onClick={() => setmodal_grid(false)}>Close</Button>
                        <Button color="primary">Submit</Button>
                    </div>
                </div>
            </div>
        </form>
    </ModalBody>
</Modal>
`;

const GridsModalExample = () => (
    <PrismCode
        code={gridsModalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Static Backdrop Modal
const staticBackdropModalCode =
    `
<!-- Static Backdrop -->

const [modal_backdrop, setmodal_backdrop] = useState(false);

    function tog_backdrop() {
        setmodal_backdrop(!modal_backdrop);
    }

<Modal
    isOpen={modal_backdrop}
    toggle={() => {
        tog_backdrop();
    }}
    backdrop={'static'}
    id="staticBackdrop"
    centered
>
    <ModalHeader>
        <h5 className="modal-title" id="staticBackdropLabel">Modal title</h5>
        <Button type="button" className="btn-close"
            onClick={() => {
                setmodal_backdrop(false);
            }} aria-label="Close"></Button>
    </ModalHeader>
    <ModalBody className="text-center p-5">
        <lord-icon
            src="https://cdn.lordicon.com/lupuorrc.json"
            trigger="loop"
            colors="primary:#121331,secondary:#08a88a"
            style={{ width: "120px", height: "120px" }}>
        </lord-icon>

        <div className="mt-4">
            <h4 className="mb-3">You've made it!</h4>
            <p className="text-muted mb-4"> The transfer was not successfully received by us. the email of the recipient wasn't correct.</p>
            <div className="hstack gap-2 justify-content-center">
                <Link to="#" className="btn btn-link link-success fw-medium" onClick={() => setmodal_backdrop(false)}><i className="ri-close-line me-1 align-middle"></i> Close</Link>
                <Link to="#" className="btn btn-success">Completed</Link>
            </div>
        </div>
    </ModalBody>
</Modal>
`;

const StaticBackdropModalExample = () => (
    <PrismCode
        code={staticBackdropModalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Toggle Between Modal
const togglebetweenCode =
    `
<!-- Default List -->

const [modal_togFirst, setmodal_togFirst] = useState(false);

    function tog_togFirst() {
        setmodal_togFirst(!modal_togFirst);
    }

    const [modal_togSecond, setmodal_togSecond] = useState(false);

    function tog_togSecond() {
        setmodal_togSecond(!modal_togSecond);
    }

<Button color="primary" onClick={() => tog_togFirst()}>Open First Modal</Button>


<Modal
    isOpen={modal_togFirst}
    toggle={() => {
        tog_togFirst();
    }}
    id="firstmodal"
    centered
>
    <ModalHeader>
        <h5 className="modal-title" id="exampleModalToggleLabel">
            Modal 1
        </h5>
        <Button
            type="button"
            className="btn-close"
            onClick={() => {
                setmodal_togFirst(false);
            }}
            aria-label="Close"
        >
        </Button>
    </ModalHeader>
    <ModalBody className="text-center p-5">
        <lord-icon
            src="https://cdn.lordicon.com/tdrtiskw.json"
            trigger="loop"
            colors="primary:#f7b84b,secondary:#5ea3cb"
            style={{ width: "130px", height: "130px" }}>
        </lord-icon>
        <div className="mt-4 pt-4">
            <h4>Uh oh, something went wrong!</h4>
            <p className="text-muted"> The transfer was not successfully received by us. the email of the recipient wasn't correct.</p>
            <Button className="btn btn-warning" onClick={() => { tog_togSecond(); tog_togFirst(false); }}>
                Continue
            </Button>
        </div>
    </ModalBody>
</Modal>
<Modal
    isOpen={modal_togSecond}
    toggle={() => {
        tog_togSecond();
    }}
    id="secondmodal"
    centered
>
    <ModalHeader>
        <h5 className="modal-title" id="exampleModalToggleLabel2">Modal 2</h5>
        <Button
            type="button"
            className="btn-close"
            onClick={() => {
                setmodal_togSecond(false);
            }}
            aria-label="Close"
        ></Button>
    </ModalHeader>
    <ModalBody className="text-center p-5">
        <lord-icon
            src="https://cdn.lordicon.com/zpxybbhl.json"
            trigger="loop"
            colors="primary:#405189,secondary:#0ab39c"
            style={{ width: "150px", height: "150px" }}>
        </lord-icon>
        <div className="mt-4 pt-3">
            <h4 className="mb-3">Follow-Up Email</h4>
            <p className="text-muted mb-4">Hide this modal and show the first with the button below Automatically Send your invitees a follow -Up email.</p>
            <div className="hstack gap-2 justify-content-center">
                <Button className="btn btn-soft-danger" onClick={() => { tog_togFirst(); tog_togSecond(false); }}>
                    Back to First
                </Button>
                <Button color="light" onClick={() => tog_togSecond(false)}>Close</Button>
            </div>
        </div>
    </ModalBody>
</Modal>
`;

const TogglebetweenExample = () => (
    <PrismCode
        code={togglebetweenCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Tooltips and Popovers
const tooltipModalCode =
    `
<!-- Tooltips and Popovers -->

const [modal_tooltip, setmodal_tooltip] = useState(false);

    function tog_tooltip() {
        setmodal_tooltip(!modal_tooltip);
    }

<Button color="primary" onClick={() => tog_tooltip()}>
    Launch Demo Modal
</Button>

<Modal
    isOpen={modal_tooltip}
    toggle={() => {
        tog_tooltip();
    }}
>
    <ModalHeader>
        <h5 className="modal-title">
            Tooltips and Popovers Modal
        </h5>
        <Button
            type="button"
            onClick={() => {
                setmodal_tooltip(false);
            }}
            className="btn-close"
            aria-label="Close"
        >
        </Button>
    </ModalHeader>
    <ModalBody>
        <h5 className="fs-16">Popover in a Modal</h5>
        <p className="text-muted">You only need to know a little to make a big
            <Link to="#" className="popover-test fw-medium text-decoration-underline link-success" id="UncontrolledPopover">
                Popover on Click
            </Link>
            you do every day. So let's get started. First, some common types of fonts and what you need to know about them triggers a popover on click.</p>
        <UncontrolledPopover placement="bottom" target="UncontrolledPopover" >
            <PopoverHeader> Common Types of Fonts </PopoverHeader>
            <PopoverBody> They're a good choice for more traditional projects. </PopoverBody>
        </UncontrolledPopover>
        <h5 className="fs-16">Tooltips in a Modal</h5>
        <p className="text-muted">Opposites attract, and that’s a fact. It’s in our
            <Link to="#" className="tooltip-test text-decoration-underline fw-medium" id="UncontrolledTooltip1">
                graphic design
            </Link> to be interested in the unusual, and that’s why using
            <Link to="#" className="tooltip-test text-decoration-underline" id="UncontrolledTooltip2">
                design
            </Link> contrasting colors in Graphic Design is a must.</p>
        <UncontrolledTooltip placement="top" target="UncontrolledTooltip1" > Morton Bayer </UncontrolledTooltip>
        <UncontrolledTooltip placement="top" target="UncontrolledTooltip2" > Web Developers </UncontrolledTooltip>
    </ModalBody>
    <ModalFooter>
        <div className="mx-auto">
            <Link to="#" className="btn btn-link fw-medium">Read More <i className="ri-arrow-right-line ms-1 align-middle"></i></Link>
        </div>
    </ModalFooter>
</Modal>
`;

const TooltipModalExample = () => (
    <PrismCode
        code={tooltipModalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Scrollable Modal
const scrollableModalCode =
    `
<!-- Scrollable Modal -->
const [modal_scroll, setmodal_scroll] = useState(false);

    function tog_scroll() {
        setmodal_scroll(!modal_scroll);
    }

<Button color="primary" onClick={() => setmodal_scroll(true)}>Scrollable Modal</Button>

<Modal
    isOpen={modal_scroll}
    toggle={() => {
        tog_scroll();
    }}
    scrollable={true}
    id="exampleModalScrollable"
>
    ...
</Modal>
`;

const ScrollableModalExample = () => (
    <PrismCode
        code={scrollableModalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Varying Modal Content
const varyingModalCode =
    `
<!-- Varying Modal Content -->
<div className="hstack gap-2 flex-wrap">
    <Button color="primary" onClick={() => tog_varying1()}>Open Modal for Mary</Button>
    <Button color="primary" onClick={() => tog_varying2()}>Open Modal for Jennifer</Button>
    <Button color="primary" onClick={() => tog_varying3()}>Open Modal for Roderick</Button>
</div>

<Modal isOpen={modal_varying1} toggle={() => { tog_varying1(); }} id="exampleModal">
    <ModalHeader>
        <h5 className="modal-title" id="exampleModalLabel">New message to Mary</h5>
        <Button
            type="button"
            className="btn-close"
            onClick={() => {
                setmodal_varying1(false);
            }}
            aria-label="Close"
        ></Button>
    </ModalHeader>
    <ModalBody>
        <form>
            <div className="mb-3">
                <label htmlFor="customer-name" className="col-form-label">Customer Name:</label>
                <Input type="text" className="form-control" id="customer-name" value="Mary" />
            </div>
            <div className="mb-3">
                <label htmlFor="message-text" className="col-form-label">Message:</label>
                <textarea className="form-control" id="message-text" rows="4"></textarea>
            </div>
        </form>
    </ModalBody>
    <ModalFooter>
        <Button
        color="light"
            className="btn btn-light"
            onClick={() => {
                setmodal_varying1(false);
            }}
        >
            Back
        </Button>
        <Button color="primary">
            Send message
        </Button>
    </ModalFooter>
</Modal>
`;

const VaryingModalExample = () => (
    <PrismCode
        code={varyingModalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Optional Sizes
const optionalModalCode =
    `
<!-- Optional Modal Sizes -->

<!-- Fullscreen modal --> 
const [modal_fullscreen, setmodal_fullscreen] = useState(false);

    function tog_fullscreen() {
        setmodal_fullscreen(!modal_fullscreen);
    }

<!-- Extra Large modal --> 
const [modal_xlarge, setmodal_xlarge] = useState(false);

    function tog_xlarge() {
        setmodal_xlarge(!modal_xlarge);
    }

<!-- Large modal --> 
const [modal_large, setmodal_large] = useState(false);

    function tog_large() {
        setmodal_large(!modal_large);
    }

<!-- Small modal -->
const [modal_small, setmodal_small] = useState(false);

    function tog_small() {
        setmodal_small(!modal_small);
    }

<div className="hstack flex-wrap gap-2">

    <!-- Fullscreen modal --> 
    <Button color="primary" onClick={() => tog_fullscreen()}>Fullscreen Modal</Button>


    <!-- Extra Large modal --> 
    <Button color="info" onClick={() => tog_xlarge()}>Extra large Modal</Button>


    <!-- Large modal --> 
    <Button color="success" onClick={() => tog_large()}>Large Modal</Button>


    <!-- Small modal --> 
    <Button color="danger" onClick={() => tog_small()}>Small Modal</Button>
</div>


<Modal
    size="xl"
    isOpen={modal_fullscreen}
    toggle={() => {
        tog_fullscreen();
    }}
    className="modal-fullscreen"
    id="exampleModalFullscreen"
>
    ...
</Modal>

<Modal
    size="xl"
    isOpen={modal_xlarge}
    toggle={() => {
        tog_xlarge();
    }}
>
    ...
</Modal>

<Modal
    size="lg"
    isOpen={modal_large}
    toggle={() => {
        tog_large();
    }}
>
    ...
</Modal>

<Modal
    size="sm"
    isOpen={modal_small}
    toggle={() => {
        tog_small();
    }}
>
    ...
</Modal>
`;

const OptionalModalExample = () => (
    <PrismCode
        code={optionalModalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Fullscreen Responsive Modals
const fullscreenResponsiveCode =
    `
<!-- Fullscreen Modals -->
const [modal_fullscreen1, setmodal_fullscreen1] = useState(false);

    function tog_fullscreen1() {
        setmodal_fullscreen1(!modal_fullscreen1);
    }

{/* FullscreenSm */}
const [modal_fullscreen_sm, setmodal_fullscreen_sm] = useState(false);

    function tog_fullscreen_sm() {
        setmodal_fullscreen_sm(!modal_fullscreen_sm);
    }

{/* FullscreenMd */}
const [modal_fullscreen_md, setmodal_fullscreen_md] = useState(false);

    function tog_fullscreen_md() {
        setmodal_fullscreen_md(!modal_fullscreen_md);
    }

{/* FullscreenLg */}
const [modal_fullscreen_lg, setmodal_fullscreen_lg] = useState(false);

    function tog_fullscreen_lg() {
        setmodal_fullscreen_lg(!modal_fullscreen_lg);
    }

{/* FullscreenXl */}
const [modal_fullscreen_xl, setmodal_fullscreen_xl] = useState(false);

    function tog_fullscreen_xl() {
        setmodal_fullscreen_xl(!modal_fullscreen_xl);
    }

{/* FullscreenXxl */}
const [modal_fullscreen_xxl, setmodal_fullscreen_xxl] = useState(false);

    function tog_fullscreen_xxl() {
        setmodal_fullscreen_xxl(!modal_fullscreen_xxl);
    }

<div className="hstack gap-2 flex-wrap">
    <Button color="primary" onClick={() => tog_fullscreen1()}> Fullscreen modal </Button>
    <Button color="primary" onClick={() => tog_fullscreen_sm()}>Full Screen Below sm</Button>
    <Button color="primary" onClick={() => tog_fullscreen_md()}>Full Screen Below md</Button>
    <Button color="primary" onClick={() => tog_fullscreen_lg()}>Full Screen Below lg</Button>
    <Button color="primary" onClick={() => tog_fullscreen_xl()}>Full Screen Below xl</Button>
    <Button color="primary" onClick={() => tog_fullscreen_xxl()}>Full Screen Below xxl</Button>
</div>

{/* Fullscreen Responsive Modals */}
<Modal size="xl" isOpen={modal_fullscreen1} toggle={() => { tog_fullscreen1(); }} className="modal-fullscreen" id="fullscreeexampleModal">
    ...
</Modal>

{/* FullscreenSm */}
<Modal id="exampleModalFullscreenSm" isOpen={modal_fullscreen_sm} toggle={() => { tog_fullscreen_sm(); }} >
    ...
</Modal>

{/* FullscreenMd */}
<Modal id="exampleModalFullscreenMd" isOpen={modal_fullscreen_md} toggle={() => { tog_fullscreen_md(); }} >
    ...
</Modal>

{/* FullscreenLg */}
<Modal id="exampleModalFullscreenLg" isOpen={modal_fullscreen_lg} toggle={() => { tog_fullscreen_lg(); }} >
    ...
</Modal>

{/* FullscreenXl */}
<Modal id="exampleModalFullscreenXl" isOpen={modal_fullscreen_xl} toggle={() => { tog_fullscreen_xl(); }} >
    ...
</Modal>

{/* FullscreenXxl */}
<Modal id="exampleModalFullscreenXxl" isOpen={modal_fullscreen_xxl} toggle={() => { tog_fullscreen_xxl(); }} >
    ...
</Modal>
`;

const FullscreenResponsiveExample = () => (
    <PrismCode
        code={fullscreenResponsiveCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Animation Modals
const animationModalCode =
    `
<!-- Animation Modals -->

const [modal_animationRight, setmodal_animationRight] = useState(false);
    function tog_animationRight() {
        setmodal_animationRight(!modal_animationRight);
    }

    const [modal_animationLeft, setmodal_animationLeft] = useState(false);
    function tog_animationLeft() {
        setmodal_animationLeft(!modal_animationLeft);
    }

    const [modal_animationUp, setmodal_animationUp] = useState(false);
    function tog_animationUp() {
        setmodal_animationUp(!modal_animationUp);
    }

    const [modal_animationFlip, setmodal_animationFlip] = useState(false);
    function tog_animationFlip() {
        setmodal_animationFlip(!modal_animationFlip);
    }

    const [modal_animationZoom, setmodal_animationZoom] = useState(false);
    function tog_animationZoom() {
        setmodal_animationZoom(!modal_animationZoom);
    }


<div className="hstack gap-2 flex-wrap">
    <Button color="primary" onClick={() => tog_animationRight()}>Fade In Right Modal</Button>
    <Button color="primary" onClick={() => tog_animationLeft()} >Fade In Left Modal</Button>
    <Button color="primary" onClick={() => tog_animationUp()} >Fade In Up Modal</Button>
    <Button color="primary" onClick={() => tog_animationFlip()} >Flip Modal</Button>
    <Button color="primary" onClick={() => tog_animationZoom()} >Zoom In Modal</Button>
</div>


<Modal id="fadeInRightModal" isOpen={modal_animationRight} toggle={() => { tog_animationRight(); }} modalClassName="fadeInRight" centered >
    ...
</Modal>

<Modal id="fadeInleftModal" isOpen={modal_animationLeft} toggle={() => { tog_animationLeft(); }} modalClassName="fadeInLeft" centered >
    ...
</Modal>

<Modal id="fadeInUpModal" isOpen={modal_animationUp} toggle={() => { tog_animationUp(); }} modalClassName="fadeInUp" centered >
    ...
</Modal>

<Modal id="flipModal" isOpen={modal_animationFlip} toggle={() => { tog_animationFlip(); }} modalClassName="flip" centered >
    ...
</Modal>

<Modal id="flipModal" isOpen={modal_animationZoom} toggle={() => { tog_animationZoom(); }} modalClassName="zoomIn" centered >
    ...
</Modal>
`;

const AnimationModalExample = () => (
    <PrismCode
        code={animationModalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Modal Positions
const positionModalCode =
    `
<!-- Positions Modals -->

const [modal_positionTop, setmodal_positionTop] = useState(false);
    function tog_positionTop() {
        setmodal_positionTop(!modal_positionTop);
    }

    const [modal_positionTopRight, setmodal_positionTopRight] = useState(false);
    function tog_positionTopRight() {
        setmodal_positionTopRight(!modal_positionTopRight);
    }

    const [modal_positionBottom, setmodal_positionBottom] = useState(false);
    function tog_positionBottom() {
        setmodal_positionBottom(!modal_positionBottom);
    }

    const [modal_positionBottomRight, setmodal_positionBottomRight] = useState(false);
    function tog_positionBottomRight() {
        setmodal_positionBottomRight(!modal_positionBottomRight);
    }


<div className="hstack gap-2 flex-wrap">
    <Button color="primary" onClick={() => tog_positionTop()}>Top Modal</Button>
    <Button color="secondary" onClick={() => tog_positionTopRight()}>Top Right Modal</Button>
    <Button color="success" onClick={() => tog_positionBottom()}>Bottom Modal</Button>
    <Button color="danger" onClick={() => tog_positionBottomRight()}>Bottom Right Modal</Button>
</div>


<Modal id="topmodal" isOpen={modal_positionTop} toggle={() => { tog_positionTop(); }} >
    ...
</Modal>

<Modal id="top-rightmodal" isOpen={modal_positionTopRight} toggle={() => { tog_positionTopRight(); }} className="modal-dialog-right" >
    ...
</Modal>

<Modal id="bottomModal" isOpen={modal_positionBottom} toggle={() => { tog_positionBottom(); }} className="modal-dialog-bottom" >
    ...
</Modal>

<Modal id="bottom-rightModal" isOpen={modal_positionBottomRight} toggle={() => { tog_positionBottomRight(); }} className="modal-dialog-bottom-right" >
    ...
</Modal
`;

const PositionModalExample = () => (
    <PrismCode
        code={positionModalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { DefaultModalExample, CenteredModalExample, GridsModalExample, StaticBackdropModalExample, TogglebetweenExample, TooltipModalExample, ScrollableModalExample, VaryingModalExample, OptionalModalExample, FullscreenResponsiveExample, AnimationModalExample, PositionModalExample };