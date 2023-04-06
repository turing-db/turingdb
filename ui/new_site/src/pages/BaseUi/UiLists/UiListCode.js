import PrismCode from "../../../Components/Common/Prism";

// Default List

const defaultListCode =
    `
<!-- Default List -->
<ListGroup>
    <ListGroupItem><i className="ri-bill-line align-middle me-2"></i> Send the billing agreement</ListGroupItem>
    <ListGroupItem><i className="ri-file-copy-2-line align-middle me-2"></i>Send over all the documentation.</ListGroupItem>
    <ListGroupItem><i className="ri-question-answer-line align-middle me-2"></i>Meeting with daron to review the intake form</ListGroupItem>
    <ListGroupItem><i className="ri-secure-payment-line align-middle me-2"></i>Check uikings theme and give customer support</ListGroupItem>
</ListGroup>
`;

const DefaultListExample = () => (
    <PrismCode
        code={defaultListCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Active Item
const activeItemCode =
    `
<!-- Active Item -->
<ListGroup>
    <ListGroupItem active>Send the billing agreement</ListGroupItem>
    <ListGroupItem>Send over all the documentation.</ListGroupItem>
    <ListGroupItem>Meeting with daron to review the intake form</ListGroupItem>
    <ListGroupItem>Check uikings theme and give customer support</ListGroupItem>
    <ListGroupItem>Start making a presentation</ListGroupItem>
</ListGroup>
`;

const ActiveItemExample = () => (
    <PrismCode
        code={activeItemCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Disabled Items
const disabledItemCode =
    `
<!-- Disabled Items -->
<ListGroup>
    <ListGroupItem disabled>
        <div className="d-flex align-items-center">
            <div className="flex-shrink-0">
                <img src={avatar1} alt="" className="avatar-xs rounded-circle" />
            </div>
            <div className="flex-grow-1 ms-2">
                James Ballard
            </div>
        </div>
    </ListGroupItem>
    <ListGroupItem>
        <div className="d-flex align-items-center">
            <div className="flex-shrink-0">
                <img src={avatar2} alt="" className="avatar-xs rounded-circle" />
            </div>
            <div className="flex-grow-1 ms-2">
                Nancy Martino
            </div>
        </div>
    </ListGroupItem>
    <ListGroupItem>
        <div className="d-flex align-items-center">
            <div className="flex-shrink-0">
                <img src={avatar3} alt="" className="avatar-xs rounded-circle" />
            </div>
            <div className="flex-grow-1 ms-2">
                Henry Baird
            </div>
        </div>
    </ListGroupItem>
    <ListGroupItem>
        <div className="d-flex align-items-center">
            <div className="flex-shrink-0">
                <img src={avatar3} alt="" className="avatar-xs rounded-circle" />
            </div>
            <div className="flex-grow-1 ms-2">
                Erica Kernan
            </div>
        </div>
    </ListGroupItem>
</ListGroup>
`;

const DisabledItemExample = () => (
    <PrismCode
        code={disabledItemCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// List with Link
const listlinkCode =
    `
<!-- List with Link -->
<ListGroup className="list-group-fill-success">
    <ListGroupItem tag="a" to="#" className="list-group-item-action active"><i className="ri-download-2-fill align-middle me-2"></i>Category Download</ListGroupItem>
    <ListGroupItem tag="a" to="#" className="list-group-item-action"><i className="ri-shield-check-line align-middle me-2"></i>Security Access</ListGroupItem>
    <ListGroupItem tag="a" to="#" className="list-group-item-action"><i className="ri-database-2-line align-middle me-2"></i>Storage folder</ListGroupItem>
    <ListGroupItem tag="a" to="#" className="list-group-item-action"><i className="ri-notification-3-line align-middle me-2"></i>Push Notification</ListGroupItem>
    <ListGroupItem tag="a" to="#" className="list-group-item-action disabled" tabIndex="-1"><i className="ri-moon-fill align-middle me-2"></i>Dark Mode</ListGroupItem>
</ListGroup>
`;

const ListlinkExample = () => (
    <PrismCode
        code={listlinkCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// List with Button
const listButtonCode =
    `
<!-- List with Button -->
<ListGroup>
    <ListGroupItem tag="button" className="list-group-item-action active"><i className="ri-reply-fill align-middle me-2"></i>Reply</ListGroupItem>
    <ListGroupItem tag="button" className="list-group-item-action"><i className="ri-share-forward-fill align-middle me-2"></i>Forward Message</ListGroupItem>
    <ListGroupItem tag="button" className="list-group-item-action"><i className="ri-equalizer-line align-middle me-2"></i>Filter Message</ListGroupItem>
    <ListGroupItem tag="button" className="list-group-item-action"><i className="ri-delete-bin-5-line align-middle me-2"></i>Delete Message</ListGroupItem>
    <ListGroupItem tag="button" className="list-group-item-action" disabled><i className="ri-forbid-line align-middle me-2"></i>Block "Mark Spencer</ListGroupItem>
</ListGroup>
`;

const ListButtonExample = () => (
    <PrismCode
        code={listButtonCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Flush List
const flushListCode =
    `
<!-- Flush List -->
<ListGroup flush>
    <ListGroupItem>Send the billing agreement</ListGroupItem>
    <ListGroupItem>Send over all the documentation.</ListGroupItem>
    <ListGroupItem>Meeting with daron to review the intake form</ListGroupItem>
    <ListGroupItem>Check uikings theme and give customer support</ListGroupItem>
    <ListGroupItem>Start making a presentation</ListGroupItem>
</ListGroup>
`;

const FlushListExample = () => (
    <PrismCode
        code={flushListCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Horizontal List
const horizontalListCode =
    `
<!-- Start Alignment -->
<ListGroup className="mb-3" horizontal>
    <ListGroupItem>Inbox</ListGroupItem>
    <ListGroupItem>Work</ListGroupItem>
    <ListGroupItem>Shopping</ListGroupItem>
</ListGroup>
<ListGroup className="justify-content-center mb-3" horizontal>
    <ListGroupItem>Inbox</ListGroupItem>
    <ListGroupItem>Work</ListGroupItem>
    <ListGroupItem>Shopping</ListGroupItem>
</ListGroup>
<ListGroup className="justify-content-end" horizontal>
    <ListGroupItem>Inbox</ListGroupItem>
    <ListGroupItem>Work</ListGroupItem>
    <ListGroupItem>Shopping</ListGroupItem>
</ListGroup>
`;

const HorizontalListExample = () => (
    <PrismCode
        code={horizontalListCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Contextual Classes
const contextualClassCode =
    `
<!-- Contextual Classes -->
<ListGroup>
    <ListGroupItem >Dapibus ac facilisis in</ListGroupItem>
    <ListGroupItem color="primary">A simple primary list group item</ListGroupItem>
    <ListGroupItem color="secondary">A simple secondary list group item</ListGroupItem>
    <ListGroupItem color="success">A simple success list group item</ListGroupItem>
    <ListGroupItem color="danger">A simple danger list group item</ListGroupItem>
    <ListGroupItem color="warning">A simple warning list group item</ListGroupItem>
    <ListGroupItem color="info">A simple info list group item</ListGroupItem>
    <ListGroupItem color="light">A simple light list group item</ListGroupItem>
    <ListGroupItem color="dark">A simple dark list group item</ListGroupItem>
</ListGroup>
`;

const ContextualClassExample = () => (
    <PrismCode
        code={contextualClassCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Contextual Classes with Link
const contextualLinkCode =
    `
<!-- Contextual Classes with Link -->
<ListGroup>
    <ListGroupItem color="" tag="a" to="#" className="list-group-item-action">A simple default list group item</ListGroupItem>
    <ListGroupItem color="primary" tag="a" to="#" className="list-group-item-action">A simple primary list group item</ListGroupItem>
    <ListGroupItem color="secondary" tag="a" to="#" className="list-group-item-action">A simple secondary list group item</ListGroupItem>
    <ListGroupItem color="success" tag="a" to="#" className="list-group-item-action">A simple success list group item</ListGroupItem>
    <ListGroupItem color="danger" tag="a" to="#" className="list-group-item-action">A simple danger list group item</ListGroupItem>
    <ListGroupItem color="warning" tag="a" to="#" className="list-group-item-action">A simple warning list group item</ListGroupItem>
    <ListGroupItem color="info" tag="a" to="#" className="list-group-item-action">A simple info list group item</ListGroupItem>
    <ListGroupItem color="light" tag="a" to="#" className="list-group-item-action">A simple light list group item</ListGroupItem>
    <ListGroupItem color="dark" tag="a" to="#" className="list-group-item-action">A simple dark list group item</ListGroupItem>
</ListGroup>
`;

const ContextualLinkExample = () => (
    <PrismCode
        code={contextualLinkCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Colored Lists
const coloredListCode =
    `
<!-- Colored Lists -->
<ListGroup>
    <ListGroupItem >Dapibus ac facilisis in</ListGroupItem>
    <ListGroupItem className="list-group-fill-primary">A simple primary list group item</ListGroupItem>
    <ListGroupItem className="list-group-fill-secondary">A simple secondary list group item</ListGroupItem>
    <ListGroupItem className="list-group-fill-success">A simple success list group item</ListGroupItem>
    <ListGroupItem className="list-group-fill-danger">A simple danger list group item</ListGroupItem>
    <ListGroupItem className="list-group-fill-warning">A simple warning list group item</ListGroupItem>
    <ListGroupItem className="list-group-fill-info">A simple info list group item</ListGroupItem>
    <ListGroupItem className="list-group-fill-light">A simple light list group item</ListGroupItem>
    <ListGroupItem className="list-group-fill-dark">A simple dark list group item</ListGroupItem>
</ListGroup>
`;

const ColoredListExample = () => (
    <PrismCode
        code={coloredListCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Custom Content
const customContentCode =
    `
<!-- Custom Content -->
<ListGroup>
    <ListGroupItem tag="a" to="#" className="list-group-item-action active">
        <div className="float-end">
            Pending
        </div>
        <div className="d-flex mb-2 align-items-center">
            <div className="flex-shrink-0">
                <img src={avatar1} alt="" className="avatar-sm rounded-circle" />
            </div>
            <div className="flex-grow-1 ms-3">
                <h5 className="list-title fs-15 mb-1">Charlie Pritchard</h5>
                <p className="list-text mb-0 fs-12">12 min Ago</p>
            </div>
        </div>
        <p className="list-text mb-0">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. That's why it's important to think about your message, then choose a font that fits. Cosby sweater eu banh mi, qui irure terry richardson ex squid.</p>
    </ListGroupItem>
    <ListGroupItem tag="a" to="#" className="list-group-item-action">
        <div className="float-end">
            Pending
        </div>
        <div className="d-flex mb-2 align-items-center">
            <div className="flex-shrink-0">
                <img src={avatar2} alt="" className="avatar-sm rounded-circle" />
            </div>
            <div className="flex-grow-1 ms-3">
                <h5 className="list-title fs-15 mb-1">Dominic Charlton</h5>
                <p className="list-text mb-0 fs-12">12 min Ago</p>
            </div>
        </div>
        <p className="list-text mb-0">Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin.</p>
    </ListGroupItem>
    <ListGroupItem tag="a" to="#" className="list-group-item-action">
        <div className="float-end">
            Rejected
        </div>
        <div className="d-flex mb-2 align-items-center">
            <div className="flex-shrink-0">
                <img src={avatar3} alt="" className="avatar-sm rounded-circle" />
            </div>
            <div className="flex-grow-1 ms-3">
                <h5 className="list-title fs-15 mb-1">Declan Long</h5>
                <p className="list-text mb-0 fs-12">12 min Ago</p>
            </div>
        </div>
        <p className="list-text mb-0">Big July earthquakes confound zany experimental vow. My girl wove six dozen plaid jackets before she quit. Six big devils from Japan quickly forgot how to waltz. try again until it looks right, and each assumenda labore aes Homo nostrud organic, assumenda labore aesthetic magna elements, buttons, everything.</p>
    </ListGroupItem>
    <ListGroupItem tag="a" to="#" className="list-group-item-action">
        <div className="float-end">
            Successful
        </div>
        <div className="d-flex mb-2 align-items-center">
            <div className="flex-shrink-0">
                <img src={avatar4} alt="" className="avatar-sm rounded-circle" />
            </div>
            <div className="flex-grow-1 ms-3">
                <h5 className="list-title fs-15 mb-1">Angela Bernier</h5>
                <p className="list-text mb-0 fs-12">5 days Ago</p>
            </div>
        </div>
        <p className="list-text mb-0">Just like in the image where we talked about using multiple fonts, you can see that the background in this graphic design is blurred. Whenever you put text on top of an image, it’s important that your viewers can understand the text, and sometimes that means applying a gaussian readable.</p>
    </ListGroupItem>
</ListGroup>
`;

const CustomContentExample = () => (
    <PrismCode
        code={customContentCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// List with Badges
const listBadgeCode =
    `
<!-- List with Badges -->
<ListGroup>
    <ListGroupItem className="d-flex justify-content-between align-items-center">
        Send the billing agreement <span className="badge bg-success">High</span>
    </ListGroupItem>
    <ListGroupItem className="d-flex justify-content-between align-items-center">
        Send over all the documentation
    </ListGroupItem>
    <ListGroupItem className="d-flex justify-content-between align-items-center">
        Meeting with daron to review the intake form <span className="badge bg-danger">Low</span>
    </ListGroupItem>
    <ListGroupItem className="d-flex justify-content-between align-items-center">
        Check uikings theme and give customer support <span className="badge bg-secondary">Medium</span>
    </ListGroupItem>
    <ListGroupItem className="d-flex justify-content-between align-items-center">
        Start making a presentation <span className="badge bg-success">High</span>
    </ListGroupItem>
</ListGroup>
`;

const ListBadgeExample = () => (
    <PrismCode
        code={listBadgeCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// List with Checkboxs
const listCheckboxCode =
    `
<!-- List with Checkboxs -->
<ListGroup>
    <ListGroupItem tag="label">
        <Input className="form-check-input me-1" type="checkbox" value="" />
        Declined Payment
    </ListGroupItem>
    <ListGroupItem tag="label">
        <Input className="form-check-input me-1" type="checkbox" value="" defaultChecked />
        Delivery Error
    </ListGroupItem>
    <ListGroupItem tag="label">
        <Input className="form-check-input me-1" type="checkbox" value="" defaultChecked />
        Wrong Amount
    </ListGroupItem>
    <ListGroupItem tag="label">
        <Input className="form-check-input me-1" type="checkbox" value="" />
        Wrong Address
    </ListGroupItem>
    <ListGroupItem tag="label">
        <Input className="form-check-input me-1" type="checkbox" value="" />
        Wrong UX/UI Solution
    </ListGroupItem>
</ListGroup>
`;

const ListCheckboxExample = () => (
    <PrismCode
        code={listCheckboxCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// List with Radios
const listRadioCode =
    `
<!-- List with Radios -->
<ListGroup>
    <ListGroupItem tag="label">
        <Input className="form-check-input fs-14 mt-0 align-middle me-1" name="exampleRadios" type="radio" value="" />
        Declined Payment
    </ListGroupItem>
    <ListGroupItem tag="label">
        <Input className="form-check-input fs-14 mt-0 align-middle me-1" name="exampleRadios" type="radio" value="" defaultChecked />
        Delivery Error
    </ListGroupItem>
    <ListGroupItem tag="label">
        <Input className="form-check-input fs-14 mt-0 align-middle me-1" name="exampleRadios" type="radio" value="" />
        Wrong Amount
    </ListGroupItem>
    <ListGroupItem tag="label">
        <Input className="form-check-input fs-14 mt-0 align-middle me-1" name="exampleRadios" type="radio" value="" />
        Wrong Address
    </ListGroupItem>
    <ListGroupItem tag="label">
        <Input className="form-check-input fs-14 mt-0 align-middle me-1" name="exampleRadios" type="radio" value="" />
        Wrong UX/UI Solution
    </ListGroupItem>
</ListGroup>
`;

const ListRadioExample = () => (
    <PrismCode
        code={listRadioCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// List with Icon
const listIconCode =
    `
<!-- List with Icon -->
<ListGroup>
    <ListGroupItem><i className="mdi mdi-check-bold align-middle lh-1 me-2"></i> Send the billing agreement</ListGroupItem>
    <ListGroupItem><i className="mdi mdi-check-bold align-middle lh-1 me-2"></i> Send over all the documentation.</ListGroupItem>
    <ListGroupItem><i className="mdi mdi-check-bold align-middle lh-1 me-2"></i> Meeting with daron to review the intake form</ListGroupItem>
    <ListGroupItem><i className="mdi mdi-check-bold align-middle lh-1 me-2"></i> Check uikings theme and give customer support</ListGroupItem>
    <ListGroupItem><i className="mdi mdi-check-bold align-middle lh-1 me-2"></i> Start making a presentation</ListGroupItem>
</ListGroup>
`;

const ListIconExample = () => (
    <PrismCode
        code={listIconCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// List with Numbered
const listNumberedCode =
    `
<!-- List with Numbered -->
<ListGroup className="list-group-numbered">
    <ListGroupItem>Send the billing agreement</ListGroupItem>
    <ListGroupItem>Send over all the documentation.</ListGroupItem>
    <ListGroupItem>Meeting with daron to review the intake form</ListGroupItem>
    <ListGroupItem>Check uikings theme and give customer support</ListGroupItem>
    <ListGroupItem>Start making a presentation</ListGroupItem>
</ListGroup>
`;

const ListNumberedExample = () => (
    <PrismCode
        code={listNumberedCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Custom Content Lists
const customListCode =
    `
<!-- Custom Content Lists -->
<SimpleBar style={{ maxHeight: "215px" }}>
    <ListGroup className="mb-1">
        <ListGroupItem>
            <div className="d-flex align-items-center">
                <div className="flex-grow-1">
                    <div className="d-flex">
                        <div className="flex-shrink-0 avatar-xs">
                            <div className="avatar-title bg-soft-danger text-danger rounded">
                                <i className="ri-netflix-fill"></i>
                            </div>
                        </div>
                        <div className="flex-shrink-0 ms-2">
                            <h6 className="fs-14 mb-0">Netfilx</h6>
                            <small className="text-muted">2 min Ago</small>
                        </div>
                    </div>
                </div>
                <div className="flex-shrink-0">
                    <span className="text-danger">-$25.50</span>
                </div>
            </div>
        </ListGroupItem>
        <ListGroupItem>
            <div className="d-flex align-items-center">
                <div className="flex-grow-1">
                    <div className="d-flex">
                        <div className="flex-shrink-0 avatar-xs">
                            <div className="avatar-title bg-soft-success text-success rounded">
                                <i className="ri-spotify-fill"></i>
                            </div>
                        </div>
                        <div className="flex-shrink-0 ms-2">
                            <h6 className="fs-14 mb-0">Spotify</h6>
                            <small className="text-muted">5 days Ago</small>
                        </div>
                    </div>
                </div>
                <div className="flex-shrink-0">
                    <span className="text-success">$48.25</span>
                </div>
            </div>
        </ListGroupItem>
        <ListGroupItem>
            <div className="d-flex align-items-center">
                <div className="flex-grow-1">
                    <div className="d-flex">
                        <div className="flex-shrink-0">
                            <img src={avatar2} alt="" className="avatar-xs rounded" />
                        </div>
                        <div className="flex-shrink-0 ms-2">
                            <h6 className="fs-14 mb-0">Emily Slater</h6>
                            <small className="text-muted">8 days Ago</small>
                        </div>
                    </div>
                </div>
                <div className="flex-shrink-0">
                    <span className="text-success">$354.90</span>
                </div>
            </div>
        </ListGroupItem>
        <ListGroupItem>
            <div className="d-flex align-items-center">
                <div className="flex-grow-1">
                    <div className="d-flex">
                        <div className="flex-shrink-0 avatar-xs">
                            <div className="avatar-title bg-soft-secondary text-secondary rounded">
                                <i className="ri-paypal-fill"></i>
                            </div>
                        </div>
                        <div className="flex-shrink-0 ms-2">
                            <h6 className="fs-14 mb-0">Paypal</h6>
                            <small className="text-muted">1 month Ago</small>
                        </div>
                    </div>
                </div>
                <div className="flex-shrink-0">
                    <span className="text-danger">-$12.22</span>
                </div>
            </div>
        </ListGroupItem>
        <ListGroupItem>
            <div className="d-flex align-items-center">
                <div className="flex-grow-1">
                    <div className="d-flex">
                        <div className="flex-shrink-0">
                            <img src={avatar3} alt="" className="avatar-xs rounded" />
                        </div>
                        <div className="flex-shrink-0 ms-2">
                            <h6 className="fs-14 mb-0">Harvey Wells</h6>
                            <small className="text-muted">4 month Ago</small>
                        </div>
                    </div>
                </div>
                <div className="flex-shrink-0">
                    <span className="text-success">-$459.78</span>
                </div>
            </div>
        </ListGroupItem>
    </ListGroup>
</SimpleBar>
`;

const CustomListExample = () => (
    <PrismCode
        code={customListCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { DefaultListExample, ActiveItemExample, DisabledItemExample, ListlinkExample, ListButtonExample, FlushListExample, HorizontalListExample, ContextualClassExample, ContextualLinkExample, ColoredListExample, CustomContentExample, ListBadgeExample, ListCheckboxExample, ListRadioExample, ListIconExample, ListNumberedExample, CustomListExample };