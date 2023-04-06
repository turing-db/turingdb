import PrismCode from "../../../Components/Common/Prism";

// Popovers

const popoversCode =
    `
<!-- Popovers -->
<div className="hstack flex-wrap gap-2">
    <Button color="light" id="PopoverTop">
        Popover on top
    </Button>
    <UncontrolledPopover placement="top" target="PopoverTop" >
        <PopoverHeader> Top Popover </PopoverHeader>
        <PopoverBody> Vivamus sagittis lacus vel augue laoreet rutrum faucibus. </PopoverBody>
    </UncontrolledPopover>
    <Button color="light" id="PopoverRight">
        Popover on right
    </Button>
    <UncontrolledPopover placement="right" target="PopoverRight" >
        <PopoverHeader> Right Popover </PopoverHeader>
        <PopoverBody> Vivamus sagittis lacus vel augue laoreet rutrum faucibus. </PopoverBody>
    </UncontrolledPopover>
    <Button color="light" id="PopoverBottom">
        Popover on right
    </Button>
    <UncontrolledPopover placement="bottom" target="PopoverBottom" >
        <PopoverHeader> Bottom Popover </PopoverHeader>
        <PopoverBody> Vivamus sagittis lacus vel augue laoreet rutrum faucibus. </PopoverBody>
    </UncontrolledPopover>
    <Button color="light" id="PopoverLeft">
        Popover on right
    </Button>
    <UncontrolledPopover placement="left" target="PopoverLeft" >
        <PopoverHeader> Left Popover </PopoverHeader>
        <PopoverBody> Vivamus sagittis lacus vel augue laoreet rutrum faucibus. </PopoverBody>
    </UncontrolledPopover>
    <Button color="success" id="PopoverDismissible">
        Dismissible popover
    </Button>
    <UncontrolledPopover placement="right" target="PopoverDismissible" >
        <PopoverHeader> Dismissible popover </PopoverHeader>
        <PopoverBody> And here's some amazing content. It's very engaging. Right? </PopoverBody>
    </UncontrolledPopover>
</div>
`;

const PopoversExample = () => (
    <PrismCode
        code={popoversCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Tooltips
const tooltipsCode =
    `
<!-- Tooltips -->
<div className="hstack flex-wrap gap-2">
    <Button color="light" id="tooltipTop">
        Tooltip on top
    </Button>
    <UncontrolledTooltip placement="top" target="tooltipTop" > Tooltip on top </UncontrolledTooltip>

    <Button color="light" id="tooltipRight">
        Tooltip on right
    </Button>
    <UncontrolledTooltip placement="right" target="tooltipRight" > Tooltip on right </UncontrolledTooltip>

    <Button color="light" id="tooltipBottom">
        Tooltip on bottom
    </Button>
    <UncontrolledTooltip placement="bottom" target="tooltipBottom" > Tooltip on bottom </UncontrolledTooltip>

    <Button color="light" id="tooltipLeft">
        Tooltip on left
    </Button>
    <UncontrolledTooltip placement="left" target="tooltipLeft" > Tooltip on left </UncontrolledTooltip>

    <Button color="primary" id="tooltipwithHtml">
        Tooltip with HTML
    </Button>
    <UncontrolledTooltip placement="top" target="tooltipwithHtml" > <em>Tooltip</em> <u>with</u> <b>HTML</b> </UncontrolledTooltip>
</div>
`;

const TooltipsExample = () => (
    <PrismCode
        code={tooltipsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Breadcrumb
const breadcrumbCode =
    `
<Breadcrumb listClassName='p-3 py-2 bg-light'>
    <BreadcrumbItem active> Home </BreadcrumbItem>
</Breadcrumb>
<Breadcrumb listClassName='p-3 py-2 bg-light'>
    <BreadcrumbItem> <Link to="#"> Home </Link> </BreadcrumbItem>
    <BreadcrumbItem active> Library </BreadcrumbItem>
</Breadcrumb>
<Breadcrumb listClassName='p-3 py-2 bg-light'>
    <BreadcrumbItem> <Link to="#"> Home </Link> </BreadcrumbItem>
    <BreadcrumbItem> <Link to="#"> Base UI </Link> </BreadcrumbItem>
    <BreadcrumbItem active> General </BreadcrumbItem>
</Breadcrumb>
<Breadcrumb listClassName='p-3 py-2 bg-light mb-0'>
    <BreadcrumbItem> <Link to="#"> <i className="ri-home-5-fill" /> </Link> </BreadcrumbItem>
    <BreadcrumbItem><Link to="#"> Base UI </Link> </BreadcrumbItem>
    <BreadcrumbItem active> General </BreadcrumbItem>
</Breadcrumb>
`;

const BreadcrumbExample = () => (
    <PrismCode
        code={breadcrumbCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Pagination
const paginationCode =
    `
<Pagination>
    <PaginationItem> <PaginationLink to="#"> Previous </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> Next </PaginationLink> </PaginationItem>
</Pagination>

<Pagination>
    <PaginationItem> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
</Pagination>


<!-- Pagination Disabled & Active -->
<Pagination>
    <PaginationItem disabled> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
    <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
</Pagination>
<Pagination>
    <PaginationItem disabled> <PaginationLink to="#"> <i className="mdi mdi-chevron-left" /> </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
    <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> <i className="mdi mdi-chevron-right" /> </PaginationLink> </PaginationItem>
</Pagination>


<!-- Pagination sizing -->

<!-- Pagination Large -->
<Pagination size='lg'>
    <PaginationItem disabled> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
</Pagination>


<!-- Pagination Small -->
<Pagination size='sm'>
    <PaginationItem disabled> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
</Pagination>


<!-- Pagination Alignment -->

<!-- Center Alignment -->
<Pagination listClassName="justify-content-center">
    <PaginationItem disabled> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
</Pagination>

<!-- Right Alignment -->
<Pagination listClassName="justify-content-end">
    <PaginationItem disabled> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
</Pagination>


<!-- Pagination Rounded -->
<Pagination className="pagination-rounded">
    <PaginationItem disabled> <PaginationLink to="#"> ← </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
    <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 4 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> 5 </PaginationLink> </PaginationItem>
    <PaginationItem> <PaginationLink to="#"> → </PaginationLink> </PaginationItem>
</Pagination>
`;

const PaginationExample = () => (
    <PrismCode
        code={paginationCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Spinners
const spinnersCode =
    `
<!-- Border spinner -->

<Spinner color="primary"> Loading... </Spinner>

<Spinner color="secondary"> Loading... </Spinner>

<Spinner color="success"> Loading... </Spinner>

<Spinner color="info"> Loading... </Spinner>

<Spinner color="warning"> Loading... </Spinner>

<Spinner color="danger"> Loading... </Spinner>

<Spinner color="dark"> Loading... </Spinner>

<Spinner color="light"> Loading... </Spinner>


<!-- Growing spinner -->

<Spinner color="primary" type="grow" > Loading... </Spinner>

<Spinner color="secondary" type="grow" > Loading... </Spinner>

<Spinner color="success" type="grow" > Loading... </Spinner>

<Spinner color="info" type="grow" > Loading... </Spinner>

<Spinner color="warning" type="grow" > Loading... </Spinner>

<Spinner color="danger" type="grow" > Loading... </Spinner>

<Spinner color="dark" type="grow" > Loading... </Spinner>

<Spinner color="light" type="grow" > Loading... </Spinner>
`;

const SpinnersExample = () => (
    <PrismCode
        code={spinnersCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { PopoversExample, TooltipsExample, BreadcrumbExample, PaginationExample, SpinnersExample };