import PrismCode from "../../../Components/Common/Prism";

// Single Button Dropdown

const singleButtonDropdownCode =
    `
<!-- Single Button Dropdown -->
<UncontrolledDropdown>
    <DropdownToggle tag="button" className="btn btn-secondary" id="dropdownMenuButton">
        Dropdown button <i className="mdi mdi-chevron-down"></i>
    </DropdownToggle>
    <DropdownMenu>
        <DropdownItem>Action</DropdownItem>
        <DropdownItem>Another action</DropdownItem>
        <DropdownItem>Something else here</DropdownItem>
    </DropdownMenu>
</UncontrolledDropdown>

<UncontrolledDropdown>
    <DropdownToggle tag="a" className="btn btn-secondary">
        Dropdown link <i className="mdi mdi-chevron-down"></i>
    </DropdownToggle>
    <DropdownMenu>
        <DropdownItem>Action</DropdownItem>
        <DropdownItem>Another action</DropdownItem>
        <DropdownItem>Something else here</DropdownItem>
    </DropdownMenu>
</UncontrolledDropdown>
`;

const SingleButtonDropdownExample = () => (
    <PrismCode
        code={singleButtonDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Dropdown Color Variant
const colorDropdownCode =
    `
<!-- Dropdown Variant -->
<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-primary">
            Primary <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-success">
            Success <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-light">
            Light <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-info">
            Info <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-secondary">
            Secondary <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-warning">
            Warning <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-danger">
            Danger <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-dark">
            Dark <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

`;

const ColorDropdownExample = () => (
    <PrismCode
        code={colorDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Split Button Dropdown
const splitButtonDropdownCode =
    `
<!-- Split Button Dropdown -->
<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="primary">Primary</Button>
        <DropdownToggle tag="button" className="btn btn-primary" split>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="success">Success</Button>
        <DropdownToggle tag="button" className="btn btn-success" split>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="light">Light</Button>
        <DropdownToggle tag="button" className="btn btn-light" split>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="info">Info</Button>
        <DropdownToggle tag="button" className="btn btn-info" split>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="secondary">Secondary</Button>
        <DropdownToggle tag="button" className="btn btn-secondary" split>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="warning">Warning</Button>
        <DropdownToggle tag="button" className="btn btn-warning" split>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="danger">Danger</Button>
        <DropdownToggle tag="button" className="btn btn-danger" split>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="dark">Dark</Button>
        <DropdownToggle tag="button" className="btn btn-dark" split>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>
`;

const SplitButtonDropdownExample = () => (
    <PrismCode
        code={splitButtonDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Dropdown Sizing
const sizingDropdownCode =
    `
<!-- Dropdown Sizing -->
<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-primary btn-lg">
            Large button <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="light" className="btn-lg">Large split button</Button>
        <DropdownToggle tag="button" className="btn btn-light btn-lg" split>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-primary btn-sm">
            Small button <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="light" className="btn-sm">Small split button</Button>
        <DropdownToggle tag="button" className="btn btn-light btn-sm" split>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>
`;

const SizingDropdownExample = () => (
    <PrismCode
        code={sizingDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Dark Dropdowns
const darkDropdownCode =
    `
<!-- Dark Dropdowns -->
<UncontrolledDropdown>
    <DropdownToggle tag="button" className="btn btn-secondary" id="dropdownMenuButton2">
        Dropdown button <i className="mdi mdi-chevron-down"></i>
    </DropdownToggle>
    <DropdownMenu className="dropdown-menu-dark">
        <DropdownItem active >Action</DropdownItem>
        <DropdownItem>Another action</DropdownItem>
        <DropdownItem>Something else here</DropdownItem>
        <DropdownItem divider />
        <DropdownItem>Separated link</DropdownItem>
    </DropdownMenu>
</UncontrolledDropdown>
`;

const DarkDropdownExample = () => (
    <PrismCode
        code={darkDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Alignment options
const alignDropdownCode =
    `
<!-- Alignment options -->
<ButtonGroup>
    <UncontrolledDropdown>
            <DropdownToggle tag="button" className="btn btn-secondary" id="dropdownMenuButton">
                Dropdown <i className="mdi mdi-chevron-down"></i>
            </DropdownToggle>
            <DropdownMenu>
                <DropdownItem>Menu item</DropdownItem>
                <DropdownItem>Menu item</DropdownItem>
                <DropdownItem>Menu item</DropdownItem>
            </DropdownMenu>
        </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-secondary">
            Right-aligned menu <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu end={true}>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-secondary">
            Left-aligned, right-aligned lg <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu className="dropdown-menu-lg-end" end={true}>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-secondary">
            Right-aligned, left-aligned lg <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu className="dropdown-menu-lg-start">
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown direction="start">
        <DropdownToggle tag="button" className="btn btn-secondary">
            Dropstart <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown direction="end">
        <DropdownToggle tag="button" className="btn btn-secondary">
            Dropend <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown direction="up">
        <DropdownToggle tag="button" className="btn btn-secondary">
            Dropup<i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>
`;

const AlignDropdownExample = () => (
    <PrismCode
        code={alignDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Dropdown Options
const optionDropdownCode =
    `
<!-- Dropdown Options -->
<UncontrolledDropdown className="me-1">
<DropdownToggle tag="button" className="btn btn-secondary" id="dropdownMenuOffset">
            Dropup<i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
        </DropdownMenu>
</UncontrolledDropdown>

<ButtonGroup>
    <UncontrolledButtonDropdown>
        <Button color="secondary">Reference</Button>
        <DropdownToggle tag="button" className="btn btn-secondary" id="dropdownMenuReference" split>
            <span className="visually-hidden">Toggle Dropdown</span>
        </DropdownToggle>
        <DropdownMenu >
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledButtonDropdown>
</ButtonGroup>
`;

const OptionDropdownExample = () => (
    <PrismCode
        code={optionDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Auto Close Behavior
const autoCloseDropdownCode =
    `
<!-- Default dropdown -->
<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-light">
            Default dropdown <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-light">
            Clickable outside <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-light" id="dropdownMenuClickableInside">
            Clickable inside <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-light" id="dropdownMenuClickable">
            Manual close <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
            <DropdownItem>Menu item</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>
`;

const AutoCloseDropdownExample = () => (
    <PrismCode
        code={autoCloseDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Dropdown Menu Item Color
const menuItemDropdownCode =
    `
<!-- Dropdown Menu Item Color -->
<div>
    <h6 className="font-size-13 mb-3">Dropdown Menu Success link example</h6>
    <div className="clearfix">
        <div className="dropdown-menu d-inline-block position-relative dropdownmenu-success" style={{ zIndex: "1" }}>
            <Link className="dropdown-item" to="#">Action</Link>
            <Link className="dropdown-item" to="#">Another action</Link>
            <Link className="dropdown-item active" to="#">Something else here</Link>
            <div className="dropdown-divider"></div>
            <Link className="dropdown-item" to="#">Separated link</Link>
        </div>
    </div>
</div>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-primary">
            Primary <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu className="dropdownmenu-primary">
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-secondary">
            Secondary <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu className="dropdownmenu-secondary">
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-success">
            Success <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu className="dropdownmenu-success">
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-warning">
            Warning <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu className="dropdownmenu-warning">
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-info">
            Info <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu className="dropdownmenu-info">
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-danger">
            Danger <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu className="dropdownmenu-danger">
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>
`;

const MenuItemDropdownExample = () => (
    <PrismCode
        code={menuItemDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Menu Content
const menuContentDropdownCode =
    `
<!-- Menu Content -->
<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-primary">
            Header <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu>
            <div className="dropdown-header noti-title">
                <h5 className="font-size-13 text-muted text-truncate mb-0">Welcome Jessie!</h5>
            </div>
            <DropdownItem>Action</DropdownItem>
            <DropdownItem>Another action</DropdownItem>
            <DropdownItem>Something else here</DropdownItem>
            <DropdownItem divider />
            <DropdownItem>Separated link</DropdownItem>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-success">
            Text <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu className="dropdown-menu-md p-3">
            <p>
                Some example text that's free-flowing within the dropdown menu.
            </p>
            <p className="mb-0">
                And this is more example text.
            </p>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

<ButtonGroup>
    <UncontrolledDropdown>
        <DropdownToggle tag="button" className="btn btn-light">
            Forms <i className="mdi mdi-chevron-down"></i>
        </DropdownToggle>
        <DropdownMenu className="dropdown-menu-md p-4">
            <form>
                <div className="mb-2">
                    <label className="form-label" htmlFor="exampleDropdownFormEmail">Email address</label>
                    <Input type="email" className="form-control" id="exampleDropdownFormEmail" placeholder="email@example.com" />
                </div>
                <div className="mb-2">
                    <label className="form-label" htmlFor="exampleDropdownFormPassword">Password</label>
                    <Input type="password" className="form-control" id="exampleDropdownFormPassword" placeholder="Password" />
                </div>
                <div className="mb-2">
                    <div className="form-check custom-checkbox">
                        <Input type="checkbox" className="form-check-input" id="rememberdropdownCheck" />
                        <label className="form-check-label" htmlFor="rememberdropdownCheck">Remember me</label>
                    </div>
                </div>
                <Button type="submit" className="btn btn-primary">Sign in</Button>
            </form>
        </DropdownMenu>
    </UncontrolledDropdown>
</ButtonGroup>

`;

const MenuContentDropdownExample = () => (
    <PrismCode
        code={menuContentDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Notifications
const notificationDropdownCode =
    `
<!-- Notifications -->
<div className="dropdown-menu dropdown-menu-lg dropdown-menu-end p-0 d-inline-block position-relative w-100" aria-labelledby="page-header-notifications-dropdown" style={{ zIndex: "1" }}>
    <div className="dropdown-head bg-primary bg-pattern rounded-top">
        <div className="p-3">
            <Row className="align-items-center">
                <Col>
                    <h6 className="m-0 fs-16 fw-semibold text-white"> Notifications </h6>
                </Col>
                <div className="col-auto dropdown-tabs">
                    <span className="badge badge-soft-light fs-13"> 4 New</span>
                </div>
            </Row>
        </div>
        <div className="px-2 pt-2">
            <Nav tabs className="dropdown-tabs nav-tabs-custom">
                <NavItem>
                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: activeTab === "1", })} onClick={() => { toggle("1"); }} >
                        All (4)
                    </NavLink>
                </NavItem>
                <NavItem>
                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: activeTab === "2", })} onClick={() => { toggle("2"); }} >
                        Messages
                    </NavLink>
                </NavItem>
                <NavItem>
                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: activeTab === "3", })} onClick={() => { toggle("3"); }} >
                        Alerts
                    </NavLink>
                </NavItem>
            </Nav>
        </div>
    </div>
    <TabContent activeTab={activeTab} id="notificationItemsTabContent4">
        <TabPane tabId="1" id="all-noti-tab1" className="py-2 ps-2">
            <SimpleBar style={{ maxHeight: "300px" }} className="pe-2">
                <div className="text-reset notification-item d-block dropdown-item">
                    <div className="d-flex">
                        <div className="flex-shrink-0 avatar-xs me-3">
                            <span className="avatar-title bg-soft-info text-info rounded-circle fs-16">
                                <i className="bx bx-badge-check"></i>
                            </span>
                        </div>
                        <div className="flex-1">
                            <h6 className="mt-0 mb-2 lh-base">Your <b>Elite</b> author Graphic Optimization <span className="text-secondary">reward</span> is ready!</h6>
                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                <span><i className="mdi mdi-clock-outline"></i> Just 30 sec ago</span>
                            </p>
                        </div>
                        <div className="px-2 fs-16">
                            <Input className="form-check-input" type="checkbox" />
                        </div>
                    </div>
                </div>
                <div className="text-reset notification-item d-block dropdown-item">
                    <div className="d-flex">
                        <img src={avatar2}
                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                        <div className="flex-1">
                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Angela Bernier</h6></Link>
                            <div className="fs-13 text-muted">
                                <p className="mb-1">Answered to your comment on the cash flow forecast's graph 🔔.</p>
                            </div>
                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                <span><i className="mdi mdi-clock-outline"></i> 48 min ago</span>
                            </p>
                        </div>
                        <div className="px-2 fs-16">
                            <Input className="form-check-input" type="checkbox" />
                        </div>
                    </div>
                </div>
                <div className="text-reset notification-item d-block dropdown-item">
                    <div className="d-flex">
                        <div className="flex-shrink-0 avatar-xs me-3">
                            <span className="avatar-title bg-soft-danger text-danger rounded-circle fs-16">
                                <i className='bx bx-message-square-dots' ></i>
                            </span>
                        </div>
                        <div className="flex-1">
                            <h6 className="mt-0 mb-2 fs-13 lh-base">You have received <b className="text-success">20</b> new messages in the conversation</h6>
                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                <span><i className="mdi mdi-clock-outline"></i> 2 hrs ago</span>
                            </p>
                        </div>
                        <div className="px-2 fs-16">
                            <Input className="form-check-input" type="checkbox" />
                        </div>
                    </div>
                </div>
                <div className="text-reset notification-item d-block dropdown-item">
                    <div className="d-flex">
                        <img src={avatar8}
                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                        <div className="flex-1">
                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Maureen Gibson</h6></Link>
                            <div className="fs-13 text-muted">
                                <p className="mb-1">We talked about a project on linkedin.</p>
                            </div>
                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                <span><i className="mdi mdi-clock-outline"></i> 4 hrs ago</span>
                            </p>
                        </div>
                        <div className="px-2 fs-16">
                            <Input className="form-check-input" type="checkbox" />
                        </div>
                    </div>
                </div>
                <div className="my-3 text-center">
                    <button type="button" className="btn btn-soft-success waves-effect waves-light">View All Notifications <i className="ri-arrow-right-line align-middle"></i></button>
                </div>
            </SimpleBar>
        </TabPane>
        <TabPane tabId="2" id="messages-tab1" className="py-2 ps-2">
            <SimpleBar style={{ maxHeight: "300px" }} className="pe-2">
                <div className="text-reset notification-item d-block dropdown-item">
                    <div className="d-flex">
                        <img src={avatar3}
                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                        <div className="flex-1">
                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">James Lemire</h6></Link>
                            <div className="fs-13 text-muted">
                                <p className="mb-1">We talked about a project on linkedin.</p>
                            </div>
                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                <span><i className="mdi mdi-clock-outline"></i> 30 min ago</span>
                            </p>
                        </div>
                        <div className="px-2 fs-16">
                            <Input className="form-check-input" type="checkbox" />
                        </div>
                    </div>
                </div>
                <div className="text-reset notification-item d-block dropdown-item">
                    <div className="d-flex">
                        <img src={avatar2}
                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                        <div className="flex-1">
                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Angela Bernier</h6></Link>
                            <div className="fs-13 text-muted">
                                <p className="mb-1">Answered to your comment on the cash flow forecast's graph 🔔.</p>
                            </div>
                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                <span><i className="mdi mdi-clock-outline"></i> 2 hrs ago</span>
                            </p>
                        </div>
                        <div className="px-2 fs-16">
                            <Input className="form-check-input" type="checkbox" />
                        </div>
                    </div>
                </div>
                <div className="text-reset notification-item d-block dropdown-item">
                    <div className="d-flex">
                        <img src={avatar6}
                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                        <div className="flex-1">
                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Kenneth Brown</h6></Link>
                            <div className="fs-13 text-muted">
                                <p className="mb-1">Mentionned you in his comment on 📃 invoice #12501.</p>
                            </div>
                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                <span><i className="mdi mdi-clock-outline"></i> 10 hrs ago</span>
                            </p>
                        </div>
                        <div className="px-2 fs-16">
                            <Input className="form-check-input" type="checkbox" />
                        </div>
                    </div>
                </div>
                <div className="text-reset notification-item d-block dropdown-item">
                    <div className="d-flex">
                        <img src={avatar8}
                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                        <div className="flex-1">
                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Maureen Gibson</h6></Link>
                            <div className="fs-13 text-muted">
                                <p className="mb-1">We talked about a project on linkedin.</p>
                            </div>
                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                <span><i className="mdi mdi-clock-outline"></i> 3 days ago</span>
                            </p>
                        </div>
                        <div className="px-2 fs-16">
                            <Input className="form-check-input" type="checkbox" />
                        </div>
                    </div>
                </div>
                <div className="my-3 text-center">
                    <button type="button" className="btn btn-soft-success waves-effect waves-light">View All Messages <i className="ri-arrow-right-line align-middle"></i></button>
                </div>
            </SimpleBar>
        </TabPane>
        <TabPane tabId="3" id="alerts-tab1" className="p-4">
            <div className="w-50 pt-3 mx-auto">
                <img src={bell} className="img-fluid" alt="user-pic" />
            </div>
            <div className="text-center pb-5 mt-2">
                <h6 className="fs-18 fw-semibold lh-base">Hey! You have no any notifications </h6>
            </div>
        </TabPane>
    </TabContent>
</div>
`;

const NotificationDropdownExample = () => (
    <PrismCode
        code={notificationDropdownCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { SingleButtonDropdownExample, ColorDropdownExample, SplitButtonDropdownExample, SizingDropdownExample, DarkDropdownExample, AlignDropdownExample, OptionDropdownExample, AutoCloseDropdownExample, MenuItemDropdownExample, MenuContentDropdownExample, NotificationDropdownExample };