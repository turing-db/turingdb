import PrismCode from "../../../Components/Common/Prism";

// Default Buttons

const defaultButtonsCode =
    `
<!-- Base Buttons -->
<Button color="primary"> Primary </Button>

<Button color="secondary"> Secondary </Button>

<Button color="success"> Success </Button>

<Button color="info"> Info </Button>

<Button color="warning"> Warning </Button>

<Button color="danger"> Danger </Button>

<Button color="dark"> Dark </Button>

<Button color="link"> Link </Button>

<Button color="light"> Light </Button>
`;

const DefaultButtonsExample = () => (
    <PrismCode
        code={defaultButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Outline Buttons

const outlineButtonsCode =
    `
<!-- Outline Buttons -->
<Button color="primary" outline > primary </Button>

<Button color="secondary" outline > Secondary </Button>

<Button color="success" outline > Success </Button>

<Button color="info" outline > Info </Button>

<Button color="warning" outline > Warning </Button>

<Button color="danger" outline > Danger </Button>

<Button color="dark" outline > Dark </Button>

<Button color="light" outline > Light </Button>
`;

const OutlineButtonsExample = () => (
    <PrismCode
        code={outlineButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Rounded Buttons

const roundedButtonsCode =
    `
<!-- Rounded Buttons -->
<Button color="primary" className="rounded-pill"> Primary </Button>

<Button color="secondary" className="rounded-pill"> Secondary </Button>

<Button color="success" className="rounded-pill"> Success </Button>

<Button color="info" className="rounded-pill"> Info </Button>

<Button color="warning" className="rounded-pill"> Warning </Button>

<Button color="danger" className="rounded-pill"> Danger </Button>

<Button color="dark" className="rounded-pill"> Dark </Button>

<Button color="light" className="rounded-pill"> Light </Button>
`;

const RoundedButtonsExample = () => (
    <PrismCode
        code={roundedButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Soft Buttons

const softButtonsCode =
    `
<!-- Soft Buttons -->
<Button className="btn-soft-primary"> Primary </Button>

<Button className="btn-soft-secondary"> Secondary </Button>

<Button className="btn-soft-success"> Success </Button>

<Button className="btn-soft-info"> Info </Button>

<Button className="btn-soft-warning"> Warning </Button>

<Button className="btn-soft-danger"> Danger </Button>

<Button className="btn-soft-dark"> Dark </Button>
`;

const SoftButtonsExample = () => (
    <PrismCode
        code={softButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);



// Gradient Buttons

const gradientButtonsCode =
    `
<!-- Gradient Buttons -->
<Button color="primary" className="bg-gradient"> Primary </Button>

<Button color="secondary" className="bg-gradient"> Secondary </Button>

<Button color="success" className="bg-gradient"> Success </Button>

<Button color="info" className="bg-gradient"> Info </Button>

<Button color="warning" className="bg-gradient"> Warning </Button>

<Button color="danger" className="bg-gradient"> Danger </Button>

<Button color="dark" className="bg-gradient"> Dark </Button>

<Button color="light" className="bg-gradient"> Light </Button>
`;

const GradientButtonsExample = () => (
    <PrismCode
        code={gradientButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Animation Buttons

const animationButtonsCode =
    `
<!-- Animation Buttons -->
<Button color="primary" className="btn-animation" data-text="Primary"> <span>Primary</span> </Button>

<Button color="secondary" className="btn-animation" data-text="Secondary"> <span>Secondary</span> </Button>

<Button color="success" className="btn-animation" data-text="Success"> <span>Success</span> </Button>

<Button color="info" className="btn-animation" data-text="Info"> <span>Info</span> </Button>

<Button color="warning" className="btn-animation" data-text="Warning"> <span>Warning</span> </Button>

<Button color="danger" className="btn-animation" data-text="Danger"> <span>Danger</span> </Button>

<Button color="dark" className="btn-animation" data-text="Dark"> <span>Dark</span> </Button>
`;

const AnimationButtonsExample = () => (
    <PrismCode
        code={animationButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Buttons with Label

const labelButtonsCode =
    `
<!-- Buttons with Label -->
<Button color="primary" className="btn-label"> <i className="ri-user-smile-line label-icon align-middle fs-16 me-2"></i> Primary </Button>

<Button color="success" className="btn-label"> <i className="ri-check-double-line label-icon align-middle fs-16 me-2"></i> Success </Button>

<Button color="warning" className="btn-label"> <i className="ri-error-warning-line label-icon align-middle fs-16 me-2"></i> Warning </Button>

<!-- Rounded with Label -->
<Button color="primary" className="btn-label rounded-pill"> <i className="ri-user-smile-line label-icon align-middle fs-16 me-2"></i> Primary </Button>

<Button color="success" className="btn-label rounded-pill"> <i className="ri-check-double-line label-icon align-middle fs-16 me-2"></i> Success </Button>

<Button color="warning" className="btn-label rounded-pill"> <i className="ri-error-warning-line label-icon align-middle fs-16 me-2"></i> Warning </Button>

<!-- Buttons with Label Right -->
<Button color="primary" className="btn-label right"> <i className="ri-user-smile-line label-icon align-middle fs-16 ms-2"></i> Primary </Button>

<Button color="success" className="btn-label right rounded-pill"> <i className="ri-check-double-line label-icon align-middle rounded-pill fs-16 ms-2"></i> Success </Button>
`;

const LabelButtonsExample = () => (
    <PrismCode
        code={labelButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Buttons Sizes

const sizeButtonsCode =
    `
<!-- Large Button -->
<Button size="lg" color="primary"> Large button </Button>

<Button size="lg" color="light"> Large button </Button>

<!-- Small Button -->
<Button size="sm" color="primary"> Small button </Button>

<Button size="sm" color="light"> Small button </Button>
`;

const SizeButtonsExample = () => (
    <PrismCode
        code={sizeButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Buttons Width

const widthButtonsCode =
    `
<!-- Width Button -->
<Button color="primary" className="w-xs"> Xs </Button>

<Button color="danger" className="w-sm"> Small </Button>

<Button color="warning" className="w-md"> Medium </Button>

<Button color="success" className="w-lg"> Large </Button>
`;

const WidthButtonsExample = () => (
    <PrismCode
        code={widthButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Buttons Tag

const tagButtonsCode =
    `
<!-- Tag Button -->
<Link to="#" className="btn btn-primary"> Link </Link>

<Button color="success" type="submit"> Button </Button>

<Button color="info" type="button"> Input </Button>

<Button color="success" type="submit"> Submit </Button>

<Button color="warning" type="reset"> Reset </Button>
`;

const TagButtonsExample = () => (
    <PrismCode
        code={tagButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Buttons Grid

const gridButtonsCode =
    `
<!-- Buttons Grid -->
<div className="d-grid gap-2">
    <Button color="primary"> Button </Button>
    <Button color="primary"> Button </Button>
</div>
`;

const GridButtonsExample = () => (
    <PrismCode
        code={gridButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Buttons Checkbox & Radio

const checkButtonsCode =
`<div className="d-flex flex-wrap align-items-center gap-2">
<ButtonGroup>
    <Input type="checkbox" className="btn-check" id="btncheck1" defaultChecked="" />
    <Button color="primary" className="mb-0" id="btncheck1"> Checkbox 1 </Button>

    <Input type="checkbox" className="btn-check" id="btncheck2" />
    <Button color="primary" className="mb-0" id="btncheck2"> Checkbox 2 </Button>

    <Input type="checkbox" className="btn-check" id="btncheck3" />
    <Button color="primary" className="mb-0" id="btncheck3"> Checkbox 3 </Button>
</ButtonGroup>

<ButtonGroup>
    <Input type="radio" className="btn-check" name="btnradio" id="btnradio1" defaultChecked="" />
    <Button color="secondary" className="mb-0" id="btnradio1" outline> Radio 1 </Button>

    <Input type="radio" className="btn-check" name="btnradio" id="btnradio2" />
    <Button color="secondary" className="mb-0" id="btnradio2" outline> Radio 2 </Button>

    <Input type="radio" className="btn-check" name="btnradio" id="btnradio3" />
    <Button color="secondary" className="mb-0" id="btnradio3" outline> Radio 3 </Button>

</ButtonGroup>
</div>
`;

const CheckButtonsExample = () => (
    <PrismCode
        code={checkButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Buttons Group

const groupButtonsCode =
    `
<!-- Buttons Group -->
<ButtonGroup>
    <Button color="primary"> Left </Button>
    <Button color="primary"> Middle </Button>
    <Button color="primary"> Right </Button>
</ButtonGroup>

<!-- Radio Buttons -->
<ButtonGroup className="mt-4 mt-sm-0">
    <Button color="light" className="btn-icon"> <i className="ri-align-right" /> </Button>
    <Button color="light" className="btn-icon"> <i className="ri-align-center" /> </Button>
    <Button color="light" className="btn-icon"> <i className="ri-align-left" /> </Button>
</ButtonGroup>
`;

const GroupButtonsExample = () => (
    <PrismCode
        code={groupButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Buttons Icon

const iconButtonsCode =
    `
<!-- Buttons Group -->
<Button color="primary" className="btn-icon"> <i className="ri-map-pin-line" /> </Button>
<Button color="danger" className="btn-icon"> <i className="ri-delete-bin-5-line" /> </Button>
<Button color="success" className="btn-icon"> <i className="ri-check-double-line" /> </Button>
<Button color="light" className="btn-icon"> <i className="ri-brush-2-fill" /> </Button>

<Button color="primary" className="btn-icon" outline> <i className="ri-24-hours-fill" /> </Button>
<Button color="danger" className="btn-icon" outline> <i className="ri-customer-service-2-line" /> </Button>
<Button color="success" className="btn-icon" outline> <i className="ri-mail-send-line" /> </Button>
<Button color="warning" className="btn-icon" outline> <i className="ri-menu-2-line" /> </Button>
`;

const IconButtonsExample = () => (
    <PrismCode
        code={iconButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Buttons Toolbar

const toolbarButtonsCode =
    `
<!-- Buttons Toolbar -->
<ButtonToolbar>
    <ButtonGroup className="me-2">
        <Button color="light">1</Button>
        <Button color="light">2</Button>
        <Button color="light">3</Button>
        <Button color="light">4</Button>
    </ButtonGroup>
    <ButtonGroup className="me-2">
        <Button color="light">5</Button>
        <Button color="light">6</Button>
        <Button color="light">7</Button>
    </ButtonGroup>
    <ButtonGroup>
        <Button color="light">8</Button>
    </ButtonGroup>
</ButtonToolbar>
`;

const ToolbarButtonsExample = () => (
    <PrismCode
        code={toolbarButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Group Buttons Sizing

const groupSizingButtonsCode =
    `
<!-- Group Buttons Sizing -->
<ButtonGroup size="lg">
    <Button color="primary">Left</Button>
    <Button color="primary">Middle</Button>
    <Button color="primary">Right</Button>
</ButtonGroup>

<ButtonGroup className="mt-2">
    <Button color="light">Left</Button>
    <Button color="light">Middle</Button>
    <Button color="light">Right</Button>
</ButtonGroup>

<ButtonGroup size="sm" className="mt-2">
    <Button color="secondary">Left</Button>
    <Button color="secondary">Middle</Button>
    <Button color="secondary">Right</Button>
</ButtonGroup>
`;

const GroupsizingButtonsExample = () => (
    <PrismCode
        code={groupSizingButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Vertical Variation

const verticalButtonsCode =
`<Row>
<div className="col-auto">
    <ButtonGroup>
        <Button color="primary">1</Button>
        <Button color="primary">2</Button>
        <UncontrolledButtonDropdown id="btnGroupDrop1">
            <DropdownToggle color="primary" caret>
                Dropdown
            </DropdownToggle>
            <DropdownMenu>
                <DropdownItem> Dropdown link </DropdownItem>
                <DropdownItem> Dropdown link </DropdownItem>
            </DropdownMenu>
        </UncontrolledButtonDropdown>
    </ButtonGroup>
</div>
<div className="col-auto">
    <ButtonGroup vertical>
        <Button color="light">Button</Button>

        <UncontrolledButtonDropdown id="btnGroupVerticalDrop1">
            <DropdownToggle color="light" caret>
                Dropdown
            </DropdownToggle>
            <DropdownMenu>
                <DropdownItem> Dropdown link </DropdownItem>
                <DropdownItem> Dropdown link </DropdownItem>
            </DropdownMenu>
        </UncontrolledButtonDropdown>

        <Button color="light">Button</Button>
        <Button color="light">Button</Button>
    </ButtonGroup>
</div>
<div className="col-auto">
    <ButtonGroup vertical>
        <Input type="radio" className="btn-check" name="vbtn" id="vbtn-radio1" defaultChecked="" />
        <Button color="secondary" htmlFor="vbtn-radio1" outline>Radio 1</Button>
        <Input type="radio" className="btn-check" name="vbtn" id="vbtn-radio2" />
        <Button color="secondary" htmlFor="vbtn-radio2" outline>Radio 2</Button>
        <Input type="radio" className="btn-check" name="vbtn" id="vbtn-radio3" />
        <Button color="secondary" htmlFor="vbtn-radio3" outline>Radio 3</Button>
    </ButtonGroup>
</div>
</Row>
`;

const VerticalButtonsExample = () => (
    <PrismCode
        code={verticalButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Ghost Buttons
const ghostButtonsCode =
    `
<!-- Ghost Buttons -->

<Button color="primary" outline className="btn btn-ghost-primary">Primary</Button>

<Button color="secondary" outline className="btn btn-ghost-secondary">Secondary</Button>

<Button color="success" outline className="btn btn-ghost-success">Success</Button>

<Button color="info" outline className="btn btn-ghost-info">Info</Button>

<Button color="warning" outline className="btn btn-ghost-warning">Warning</Button>

<Button color="danger" outline className="btn btn-ghost-danger">Danger</Button>

<Button color="dark" outline className="btn btn-ghost-dark">Dark</Button>
`;

const GhostButtonsExample = () => (
    <PrismCode
        code={ghostButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Loading Buttons
const loadingbuttonsCode =
    `
<!-- Load More Buttons -->

<div className="hstack flex-wrap gap-2 mb-3 mb-lg-0">
    <Button color="primary" className="btn-load" outline>
        <span className="d-flex align-items-center">
            <Spinner size="sm" className="flex-shrink-0"> Loading... </Spinner>
            <span className="flex-grow-1 ms-2">
                Loading...
            </span>
        </span>
    </Button>
    <Button color="success" className="btn-load">
        <span className="d-flex align-items-center">
            <Spinner size="sm" className="flex-shrink-0"> Loading... </Spinner>
            <span className="flex-grow-1 ms-2">
                Loading...
            </span>
        </span>
    </Button>
    <Button color="secondary" className="btn-load" outline>
        <span className="d-flex align-items-center">
            <Spinner size="sm" type="grow" className="flex-shrink-0"> Loading... </Spinner>
            <span className="flex-grow-1 ms-2">
                Loading...
            </span>
        </span>
    </Button>
    <Button color="danger" className="btn-load">
        <span className="d-flex align-items-center">
            <Spinner size="sm" type="grow" className="flex-shrink-0"> Loading... </Spinner>
            <span className="flex-grow-1 ms-2">
                Loading...
            </span>
        </span>
    </Button>
</div>

<div className="d-flex flex-wrap gap-2 mb-3 mb-lg-0">
    <Button color="primary" className="btn-load" outline>
        <span className="d-flex align-items-center">
            <span className="flex-grow-1 me-2">
                Loading...
            </span>
            <Spinner size="sm" className="flex-shrink-0" role="status"> Loading... </Spinner>
        </span>
    </Button>
    <Button color="success" className="btn-load">
        <span className="d-flex align-items-center">
            <span className="flex-grow-1 me-2">
                Loading...
            </span>
            <Spinner size="sm" className="flex-shrink-0" role="status"> Loading... </Spinner>
        </span>
    </Button>
    <Button color="warning" className="btn-load" outline>
        <span className="d-flex align-items-center">
            <span className="flex-grow-1 me-2">
                Loading...
            </span>
            <Spinner size="sm" type="grow" className="flex-shrink-0" role="status"> Loading... </Spinner>
        </span>
    </Button>
    <Button color="info" className="btn-load">
        <span className="d-flex align-items-center">
            <span className="flex-grow-1 me-2">
                Loading...
            </span>
            <Spinner size="sm" type="grow" className="flex-shrink-0" role="status"> Loading... </Spinner>
        </span>
    </Button>
</div>
`;

const LoadingbuttonsExample = () => (
    <PrismCode
        code={loadingbuttonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Border Buttons
const borderButtonsCode =
    `
<!-- Border Buttons -->

<div className="hstack flex-wrap gap-2 mb-3 mb-lg-0">
    <Button color="primary" className="btn-border">Primary</Button>
    <Button color="secondary" className="btn-border">Secondary</Button>
    <Button color="success" className="btn-border">Success</Button>
    <Button color="warning" className="btn-border">Warning</Button>
    <Button color="danger" className="btn-border">Danger</Button>
</div>

<div className="hstack flex-wrap gap-2 mb-3 mb-lg-0">
    <Button color="primary" outline className="btn-border">Primary</Button>
    <Button color="secondary" outline className="btn-border">Secondary</Button>
    <Button color="success" outline className="btn-border">Success</Button>
    <Button className="btn-soft-warning btn-border">Warning</Button>
    <Button className="btn-soft-danger btn-border">Danger</Button>
    <Button className="btn-soft-dark btn-border">Dark</Button>
</div>
`;

const BorderButtonsExample = () => (
    <PrismCode
        code={borderButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Custom Toggle Buttons
const customToggleButtonsCode =
    `
<!-- Custom Toggle Buttons -->

<div className="d-flex flex-wrap align-items-center gap-2">
    <Button color='primary' className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on"><i className="ri-alarm-line align-bottom"></i> Active</span>
        <span className="icon-off">Unactive</span>
    </Button>
    <Button color='secondary' className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on"><i className="ri-user-add-line align-bottom me-1"></i> Connect</span>
        <span className="icon-off"><i className="ri-check-fill align-bottom me-1"></i> Unconnect</span>
    </Button>
    <Button color='success' className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on"><i className="ri-thumb-up-line align-bottom me-1"></i> Yes</span>
        <span className="icon-off"><i className="ri-thumb-down-line align-bottom me-1"></i> No</span>
    </Button>
    <Button color='warning' className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on"><i className="ri-add-line align-bottom me-1"></i> Follow</span>
        <span className="icon-off"><i className="ri-user-unfollow-line align-bottom me-1"></i> Unfollow</span>
    </Button>
    <Button color='danger' className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on">On</span>
        <span className="icon-off">Off</span>
    </Button>
    <Button color='dark' className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on"><i className="ri-bookmark-line align-bottom me-1"></i> Bookmark</span>
        <span className="icon-off"><i className="ri-bookmark-3-fill align-bottom me-1"></i> Unbookmark</span>
    </Button>
</div>

<div className="d-flex flex-wrap align-items-center gap-2">
    <Button color="primary" outline className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on">Active</span>
        <span className="icon-off">Unactive</span>
    </Button>
    <Button color="secondary" outline className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on"><i className="ri-add-line align-bottom me-1"></i> Follow</span>
        <span className="icon-off"><i className="ri-user-unfollow-line align-bottom me-1"></i> Unfollow</span>
    </Button>
    <Button color="success" outline className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on">On</span>
        <span className="icon-off">Off</span>
    </Button>
    <Button color="warning" outline className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on">Follow</span>
        <span className="icon-off">Unfollow</span>
    </Button>
    <Button color="danger" outline className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on">On</span>
        <span className="icon-off">Off</span>
    </Button>
    <Button color="dark" outline className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
        <span className="icon-on"><i className="ri-bookmark-line align-bottom"></i></span>
        <span className="icon-off"><i className="ri-bookmark-3-fill align-bottom"></i></span>
    </Button>
</div>
`;

const CustomToggleButtonsExample = () => (
    <PrismCode
        code={customToggleButtonsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { DefaultButtonsExample, OutlineButtonsExample, RoundedButtonsExample, SoftButtonsExample, GradientButtonsExample, AnimationButtonsExample, LabelButtonsExample, SizeButtonsExample, WidthButtonsExample, TagButtonsExample, GridButtonsExample, CheckButtonsExample, GroupButtonsExample, IconButtonsExample, ToolbarButtonsExample, GroupsizingButtonsExample, VerticalButtonsExample, GhostButtonsExample, LoadingbuttonsExample, BorderButtonsExample, CustomToggleButtonsExample };