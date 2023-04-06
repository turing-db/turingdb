import React from 'react';
import { InputExample, InputSizing, FileInput, InputGroup, InputGroupSizing, MultipleInputs, ButtonsCheckboxesRadiosGroup, ButtonsWithDropdowns, CustomForms } from './BasicElementCode';
import UiContent from "../../../Components/Common/UiContent";

//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import { Card, CardBody, Col, Container, DropdownItem, DropdownMenu, DropdownToggle, Input, Label, Row, UncontrolledDropdown } from 'reactstrap';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';


const BasicElements = () => {
    document.title="Basic Elements | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">
            
                <Container fluid>
                    <BreadCrumb title="Basic Elements" pageTitle="Forms" />

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Input Example" />

                                <CardBody className="card-body">
                                    <div className="live-preview">
                                        <Row className="gy-4">
                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="basiInput" className="form-label">Basic Input</Label>
                                                    <Input type="password" className="form-control" id="basiInput" />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="labelInput" className="form-label">Input with Label</Label>
                                                    <Input type="password" className="form-control" id="labelInput" />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="placeholderInput" className="form-label">Input with Placeholder</Label>
                                                    <Input type="password" className="form-control" id="placeholderInput" placeholder="Placeholder" />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="valueInput" className="form-label">Input with Value</Label>
                                                    <Input type="text" className="form-control" id="valueInput" defaultValue="Input value" />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="readonlyPlaintext" className="form-label">Readonly Plain Text Input</Label>
                                                    <Input type="text" className="form-control-plaintext" id="readonlyPlaintext" defaultValue="Readonly input" readOnly />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="readonlyInput" className="form-label">Readonly Input</Label>
                                                    <Input type="text" className="form-control" id="readonlyInput" defaultValue="Readonly input" readOnly />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="disabledInput" className="form-label">Disabled Input</Label>
                                                    <Input type="text" className="form-control" id="disabledInput" defaultValue="Disabled input" disabled />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="iconInput" className="form-label">Input with Icon</Label>
                                                    <div className="form-icon">
                                                        <Input type="email" className="form-control form-control-icon" id="iconInput" placeholder="example@gmail.com" />
                                                        <i className="ri-mail-unread-line"></i>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="iconrightInput" className="form-label">Input with Icon Right</Label>
                                                    <div className="form-icon right">
                                                        <Input type="email" className="form-control form-control-icon" id="iconrightInput" placeholder="example@gmail.com" />
                                                        <i className="ri-mail-unread-line"></i>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="exampleInputdate" className="form-label">Input Date</Label>
                                                    <Input type="date" className="form-control" id="exampleInputdate" />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="exampleInputtime" className="form-label">Input Time</Label>
                                                    <Input type="time" className="form-control" id="exampleInputtime" />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="exampleInputpassword" className="form-label">Input Password</Label>
                                                    <Input type="password" className="form-control" id="exampleInputpassword" defaultValue="44512465" />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="exampleFormControlTextarea5" className="form-label">Example Textarea</Label>
                                                    <textarea className="form-control" id="exampleFormControlTextarea5" rows="3"></textarea>
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="formtextInput" className="form-label">Form Text</Label>
                                                    <Input type="password" className="form-control" id="formtextInput" />
                                                    <div id="passwordHelpBlock" className="form-text">
                                                        Must be 8-20 characters long.
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="colorPicker" className="form-label">Color Picker</Label>
                                                    <Input type="color" className="form-control form-control-color w-100" id="colorPicker" defaultValue="#364574" />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="borderInput" className="form-label">Input Border Style</Label>
                                                    <Input type="text" className="form-control border-dashed" id="borderInput" placeholder="Enter your name" />
                                                </div>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <Label htmlFor="exampleDataList" className="form-label">Datalist example</Label>
                                                <Input className="form-control" list="datalistOptions" id="exampleDataList" placeholder="Search your country..." />
                                                <datalist id="datalistOptions">
                                                    <option defaultValue="Switzerland"> </option>
                                                    <option defaultValue="New York"> </option>
                                                    <option defaultValue="France"> </option>
                                                    <option defaultValue="Spain"> </option>
                                                    <option defaultValue="Chicago"> </option>
                                                    <option defaultValue="Bulgaria"> </option>
                                                    <option defaultValue="Hong Kong"> </option>
                                                </datalist>
                                            </Col>

                                            <Col xxl={3} md={6}>
                                                <div>
                                                    <Label htmlFor="exampleInputrounded" className="form-label">Rounded Input</Label>
                                                    <Input type="text" className="form-control rounded-pill" id="exampleInputrounded" placeholder="Enter your name" />
                                                </div>
                                            </Col>

                                        </Row>

                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "352px" }}>
                                            <code>
                                                <InputExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                    </Row>


                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Input Sizing" />

                                <div className="card-body">
                                    <p className="text-muted">Use <code>form-control-lg</code> class to set large size input and Use <code>form-control-sm</code> class to set small size input. No class is needed for default size input.</p>
                                    <div className="live-preview">
                                        <Row className="align-items-center g-3">
                                            <Col lg={4}>
                                                <Input className="form-control form-control-sm" type="text" placeholder=".form-control-sm" />
                                            </Col>

                                            <Col lg={4}>
                                                <Input className="form-control" type="text" placeholder=".form-control" />
                                            </Col>

                                            <Col lg={4}>
                                                <Input className="form-control form-control-lg" type="text" placeholder=".form-control-lg" />
                                            </Col>

                                        </Row>

                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <InputSizing />
                                            </code>
                                        </pre>
                                    </div>
                                </div>
                            </Card>
                        </Col>

                    </Row>


                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="File Input" />

                                <CardBody>
                                    <div className="live-preview">
                                        <Row className="align-items-center g-3">
                                            <Col lg={4}>
                                                <div>
                                                    <Label htmlFor="formFile" className="form-label">Default File Input Example</Label>
                                                    <p className="text-muted">Use <code>input</code> attribute with <code>type="file"</code> tag for default file input.</p>
                                                    <Input className="form-control" type="file" id="formFile" />
                                                </div>
                                            </Col>

                                            <Col lg={4}>
                                                <div>
                                                    <Label htmlFor="formFileMultiple" className="form-label">Multiple Files Input Example</Label>
                                                    <p className="text-muted">Use <code>multiple</code> attribute within the input attribute to select multiple files.</p>
                                                    <Input className="form-control" type="file" id="formFileMultiple" multiple />
                                                </div>
                                            </Col>

                                            <Col lg={4}>
                                                <div>
                                                    <Label htmlFor="formFileDisabled" className="form-label">Disabled File Input Example</Label>
                                                    <p className="text-muted">Use <code>disabled</code> attribute within the input attribute to disable the file input.</p>
                                                    <Input className="form-control" type="file" id="formFileDisabled" disabled />
                                                </div>
                                            </Col>
                                        </Row>

                                        <Row className="mt-4 align-items-center g-3">
                                            <h5 className="fs-14">File Input Sizing</h5>
                                            <Col lg={4}>
                                                <div>
                                                    <Label htmlFor="formSizeSmall" className="form-label">Small Size File Input Example</Label>
                                                    <p className="text-muted">Use <code>form-control-sm</code> class within the form-control class to set a small size file input.</p>
                                                    <Input className="form-control form-control-sm" id="formSizeSmall" type="file" />
                                                </div>
                                            </Col>
                                            <Col lg={4}>
                                                <div>
                                                    <Label htmlFor="formSizeDefault" className="form-label">Default Size File Input Example</Label>
                                                    <p className="text-muted">Use <code>form-control </code>class and <code>type="file"</code> attribute within the input attribute to set a default size file input.</p>
                                                    <Input className="form-control" id="formSizeDefault" type="file" />
                                                </div>
                                            </Col>
                                            <Col lg={4}>
                                                <div>
                                                    <Label htmlFor="formSizeLarge" className="form-label">Large Size File Input Example</Label>
                                                    <p className="text-muted">Use <code>form-control-lg</code> class within the form-control class to set a large size file input.</p>

                                                    <Input className="form-control form-control-lg" id="formSizeLarge" type="file" />
                                                </div>
                                            </Col>

                                        </Row>

                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <FileInput />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                    </Row>


                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Input Group" />

                                <CardBody>
                                    <div className="live-preview">
                                        <div>
                                            <h5 className="fs-15">Basic example</h5>
                                            <p className="text-muted">Use <code>input-group</code> class to div element which contains input attribute to wrap a default input in the group.</p>
                                            <Row className="g-3">
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <span className="input-group-text" id="basic-addon1">@</span>
                                                        <Input type="text" className="form-control" placeholder="Username" aria-label="Username" aria-describedby="basic-addon1" />
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <Input type="text" className="form-control" placeholder="Recipient's username" aria-label="Recipient's username" aria-describedby="basic-addon2" />
                                                        <span className="input-group-text" id="basic-addon2">@example.com</span>
                                                    </div>
                                                </Col>
                                                <Col lg={12}>
                                                    <div className="input-group">
                                                        <span className="input-group-text">$</span>
                                                        <Input type="text" className="form-control" aria-label="Amount (to the nearest dollar)" />
                                                        <span className="input-group-text">.00</span>
                                                    </div>
                                                </Col>
                                                <Col lg={12}>
                                                    <div className="input-group">
                                                        <Input type="text" className="form-control" placeholder="Username" aria-label="Username" />
                                                        <span className="input-group-text">@</span>
                                                        <Input type="text" className="form-control" placeholder="Server" aria-label="Server" />
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <span className="input-group-text">With textarea</span>
                                                        <textarea className="form-control" aria-label="With textarea" rows="2"></textarea>
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <Label htmlFor="basic-url" className="form-label">Your vanity URL</Label>
                                                    <div className="input-group">
                                                        <span className="input-group-text" id="basic-addon3">https://example.com/users/</span>
                                                        <Input type="text" className="form-control" id="basic-url" aria-describedby="basic-addon3" />
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>

                                        <div className="mt-4">
                                            <h5 className="fs-15">Wrapping</h5>
                                            <p className="text-muted">
                                                Input groups wrap by default via <code>flex-wrap: wrap</code> in order to accommodate custom form field validation within an input group. You may disable this with <code>flex-nowrap</code> class.
                                            </p>
                                            <div className="input-group flex-nowrap">
                                                <span className="input-group-text" id="addon-wrapping">@</span>
                                                <Input type="text" className="form-control" placeholder="Username" aria-label="Username" aria-describedby="addon-wrapping" />
                                            </div>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <InputGroup />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>


                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Input Group Sizing" />

                                <div className="card-body">
                                    <p className="text-muted">
                                        Use <code>input-group-sm</code> class to set a small size input group and <code>input-group-lg</code> class to input-group class to set a large size input group respectively. no such class is required for a
                                        default size input group.
                                    </p>
                                    <div className="live-preview">
                                        <Row className="align-items-center g-3">
                                            <Col lg={4} >
                                                <div className="input-group input-group-sm">
                                                    <span className="input-group-text" id="inputGroup-sizing-sm">Small</span>
                                                    <Input type="text" className="form-control" aria-label="Sizing example input" aria-describedby="inputGroup-sizing-sm" />
                                                </div>
                                            </Col>
                                            <Col lg={4} >
                                                <div className="input-group">
                                                    <span className="input-group-text" id="inputGroup-sizing-default">Default</span>
                                                    <Input type="text" className="form-control" aria-label="Sizing example input" aria-describedby="inputGroup-sizing-default" />
                                                </div>
                                            </Col>
                                            <Col lg={4} >
                                                <div className="input-group input-group-lg">
                                                    <span className="input-group-text" id="inputGroup-sizing-lg">Large</span>
                                                    <Input type="text" className="form-control" aria-label="Sizing example input" aria-describedby="inputGroup-sizing-lg" />
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <InputGroupSizing />
                                            </code>
                                        </pre>
                                    </div>
                                </div>
                            </Card>
                        </Col>

                    </Row>


                    <Row>
                        <Col lg={12}>
                            <div className="card">
                                <PreviewCardHeader title="Multiple Inputs" />

                                <div className="card-body">
                                    <p className="text-muted">While multiple <code>&lt;input&gt;</code>s are supported visually, validation styles are only available for input groups with a single <code>&lt;input&gt;</code>.</p>
                                    <div className="live-preview">
                                        <div className="input-group mb-3">
                                            <span className="input-group-text">First and last name</span>
                                            <Input type="text" aria-label="First name" className="form-control" />
                                            <Input type="text" aria-label="Last name" className="form-control" />
                                        </div>

                                        <div className="input-group mb-3">
                                            <span className="input-group-text">$</span>
                                            <span className="input-group-text">0.00</span>
                                            <Input type="text" className="form-control" aria-label="Dollar amount (with dot and two decimal places)" />
                                        </div>

                                        <div className="input-group">
                                            <Input type="text" className="form-control" aria-label="Dollar amount (with dot and two decimal places)" />
                                            <span className="input-group-text">$</span>
                                            <span className="input-group-text">0.00</span>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <MultipleInputs />
                                            </code>
                                        </pre>
                                    </div>
                                </div>
                            </div>
                        </Col>

                    </Row>


                    <Row>
                        <Col lg={12}>
                            <div className="card">
                                <PreviewCardHeader title="Buttons, Checkboxs and Radios Group" />

                                <div className="card-body">
                                    <div className="live-preview">
                                        <div>
                                            <p className="text-muted">
                                                Use any checkbox, radio, or button option within an input group’s addon instead of text. We recommend adding <code>mt-0</code> class to the <code>form-check-input</code> when there’s no visible text next
                                                to the input.
                                            </p>
                                            <Row className="g-3">
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <div className="input-group-text">
                                                            <Input className="form-check-input mt-0" type="checkbox" defaultValue="" aria-label="Checkbox for following text input" />
                                                        </div>
                                                        <Input type="text" className="form-control" aria-label="Text input with checkbox" />
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <div className="input-group-text">
                                                            <Input className="form-check-input mt-0" type="radio" defaultValue="" aria-label="Radio button for following text input" />
                                                        </div>
                                                        <Input type="text" className="form-control" aria-label="Text input with radio button" />
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <button className="btn btn-outline-primary" type="button" id="button-addon1">Button</button>
                                                        <Input type="text" className="form-control" placeholder="" aria-label="Example text with button addon" aria-describedby="button-addon1" />
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <Input type="text" className="form-control" aria-label="Recipient's username" aria-describedby="button-addon2" />
                                                        <button className="btn btn-outline-success" type="button" id="button-addon2">Button</button>
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <button className="btn btn-primary" type="button">Button</button>
                                                        <button className="btn btn-success" type="button">Button</button>
                                                        <Input type="text" className="form-control" placeholder="" aria-label="Example text with two button addons" />
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <Input type="text" className="form-control" aria-label="Recipient's username with two button addons" />
                                                        <button className="btn btn-primary" type="button">Button</button>
                                                        <button className="btn btn-success" type="button">Button</button>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <ButtonsCheckboxesRadiosGroup />
                                            </code>
                                        </pre>
                                    </div>
                                </div>
                            </div>
                        </Col>

                    </Row>
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Buttons with dropdowns" />

                                <div className="card-body">
                                    <div className="live-preview">
                                        <div>
                                            <p className="text-muted">You can use a button with the dropdown toggle to set the file input group.</p>
                                            <Row className="g-3">
                                                <Col lg={6}>
                                                    <UncontrolledDropdown className="input-group">
                                                        <DropdownToggle tag="button" className="btn btn-primary" type="button">Dropdown</DropdownToggle>
                                                        <DropdownMenu>
                                                            <li><DropdownItem>Action</DropdownItem></li>
                                                            <li><DropdownItem>Another action</DropdownItem></li>
                                                            <li><DropdownItem>Something else here</DropdownItem></li>
                                                            <li><hr className="dropdown-divider" /></li>
                                                            <li><DropdownItem>Separated link</DropdownItem></li>
                                                        </DropdownMenu>
                                                        <Input type="text" className="form-control" aria-label="Text input with dropdown button" />
                                                    </UncontrolledDropdown>
                                                </Col>
                                                <Col lg={6}>
                                                    <UncontrolledDropdown className="input-group">
                                                        <Input type="text" className="form-control" aria-label="Text input with dropdown button" />
                                                        <DropdownToggle tag="button" className="btn btn-success" type="button" data-bs-toggle="dropdown" aria-expanded="false">Dropdown</DropdownToggle>
                                                        <DropdownMenu className="dropdown-menu-end">
                                                            <li><DropdownItem>Action</DropdownItem></li>
                                                            <li><DropdownItem>Another action</DropdownItem></li>
                                                            <li><DropdownItem>Something else here</DropdownItem></li>
                                                            <li><hr className="dropdown-divider" /></li>
                                                            <li><DropdownItem>Separated link</DropdownItem></li>
                                                        </DropdownMenu>
                                                    </UncontrolledDropdown>
                                                </Col>
                                                <Col lg={12}>
                                                    <UncontrolledDropdown className="input-group">
                                                        <DropdownToggle tag="button" className="btn btn-outline-secondary" type="button" data-bs-toggle="dropdown" aria-expanded="false">Dropdown</DropdownToggle>
                                                        <DropdownMenu>
                                                            <li><DropdownItem>Action before</DropdownItem></li>
                                                            <li><DropdownItem>Another action before</DropdownItem></li>
                                                            <li><DropdownItem>Something else here</DropdownItem></li>
                                                            <li><hr className="dropdown-divider" /></li>
                                                            <li><DropdownItem>Separated link</DropdownItem></li>
                                                        </DropdownMenu>
                                                        <Input type="text" className="form-control" aria-label="Text input with 2 dropdown buttons" />
                                                        <DropdownToggle tag="button" className="btn btn-outline-secondary" type="button" data-bs-toggle="dropdown" aria-expanded="false">Dropdown</DropdownToggle>
                                                        <DropdownMenu className="dropdown-menu-end">
                                                            <li><DropdownItem>Action</DropdownItem></li>
                                                            <li><DropdownItem>Another action</DropdownItem></li>
                                                            <li><DropdownItem>Something else here</DropdownItem></li>
                                                            <li><hr className="dropdown-divider" /></li>
                                                            <li><DropdownItem>Separated link</DropdownItem></li>
                                                        </DropdownMenu>
                                                    </UncontrolledDropdown>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <ButtonsWithDropdowns />
                                            </code>
                                        </pre>
                                    </div>
                                </div>
                            </Card>
                        </Col>

                    </Row>


                    <Row >
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Custom Forms" />

                                <div className="card-body">
                                    <p className="text-muted">Input groups include support for custom selects and custom file inputs. Browser default versions of these are not supported.</p>
                                    <div className="live-preview">
                                        <div>
                                            <h5 className="fs-14 mb-3">Select</h5>
                                            <Row className="g-3">
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <Label className="input-group-text" htmlFor="inputGroupSelect01">Options</Label>
                                                        <select className="form-select" id="inputGroupSelect01">
                                                            <option >Choose...</option>
                                                            <option defaultValue="1">One</option>
                                                            <option defaultValue="2">Two</option>
                                                            <option defaultValue="3">Three</option>
                                                        </select>
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <select className="form-select" id="inputGroupSelect02">
                                                            <option >Choose...</option>
                                                            <option defaultValue="1">One</option>
                                                            <option defaultValue="2">Two</option>
                                                            <option defaultValue="3">Three</option>
                                                        </select>
                                                        <Label className="input-group-text" htmlFor="inputGroupSelect02">Options</Label>
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <button className="btn btn-outline-primary" type="button">Button</button>
                                                        <select className="form-select" id="inputGroupSelect03" aria-label="Example select with button addon">
                                                            <option >Choose...</option>
                                                            <option defaultValue="1">One</option>
                                                            <option defaultValue="2">Two</option>
                                                            <option defaultValue="3">Three</option>
                                                        </select>
                                                    </div>
                                                </Col>
                                                <Col lg={6}>
                                                    <div className="input-group">
                                                        <select className="form-select" id="inputGroupSelect04" aria-label="Example select with button addon">
                                                            <option >Choose...</option>
                                                            <option defaultValue="1">One</option>
                                                            <option defaultValue="2">Two</option>
                                                            <option defaultValue="3">Three</option>
                                                        </select>
                                                        <button className="btn btn-outline-secondary" type="button">Button</button>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>

                                        <div className="mt-4">
                                            <h5 className="fs-14 mb-3">File Input</h5>
                                            <Row className="g-3">
                                                <Col lg={6} >
                                                    <div className="input-group">
                                                        <Label className="input-group-text" htmlFor="inputGroupFile01">Upload</Label>
                                                        <Input type="file" className="form-control" id="inputGroupFile01" />
                                                    </div>
                                                </Col>
                                                <Col lg={6} >
                                                    <div className="input-group">
                                                        <Input type="file" className="form-control" id="inputGroupFile02" />
                                                        <Label className="input-group-text" htmlFor="inputGroupFile02">Upload</Label>
                                                    </div>
                                                </Col>
                                                <Col lg={6} >
                                                    <div className="input-group">
                                                        <button className="btn btn-outline-primary" type="button" id="inputGroupFileAddon03">Button</button>
                                                        <Input type="file" className="form-control" id="inputGroupFile03" aria-describedby="inputGroupFileAddon03" aria-label="Upload" />
                                                    </div>
                                                </Col>
                                                <Col lg={6} >
                                                    <div className="input-group">
                                                        <Input type="file" className="form-control" id="inputGroupFile04" aria-describedby="inputGroupFileAddon04" aria-label="Upload" />
                                                        <button className="btn btn-outline-success" type="button" id="inputGroupFileAddon04">Button</button>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <CustomForms />
                                            </code>
                                        </pre>
                                    </div>
                                </div>
                            </Card>
                        </Col>

                    </Row>

                </Container>

            </div>


        </React.Fragment>
    );
}

export default BasicElements;