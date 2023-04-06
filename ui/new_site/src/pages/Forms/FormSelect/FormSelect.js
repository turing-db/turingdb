import React, { useState } from 'react';
import { Card, CardBody, CardHeader, Col, Label, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';
import UiContent from "../../../Components/Common/UiContent";

import { DefaultSelect, MenuSize, SelectSize } from '../FormSelect/FormSelectCode';
import Select from "react-select";
import makeAnimated from "react-select/animated";
const animatedComponents = makeAnimated();

const SingleOptions = [
    { value: 'Choices 1', label: 'Choices 1' },
    { value: 'Choices 2', label: 'Choices 2' },
    { value: 'Choices 3', label: 'Choices 3' },
    { value: 'Choices 4', label: 'Choices 4' }
];

const GroupOptions = [

    {
        label: "UK",
        options: [
            { label: "London", value: "London" },
            { label: "Manchester", value: "Manchester" },
            { label: "Liverpool", value: "Liverpool" }
        ]
    },
    {
        label: "FR",
        options: [
            { label: "Paris", value: "Paris" },
            { label: "Lyon", value: "Lyon" },
            { label: "Marseille", value: "Marseille" }
        ]
    },
    {
        label: "DE",
        options: [
            { label: "Hamburg", value: "Hamburg" },
            { label: "Munich", value: "Munich" },
            { label: "Berlin", value: "Berlin" }
        ]
    },
    {
        label: "US",
        options: [
            { label: "New York", value: "New York" },
            { label: "Washington", value: "Washington" },
            { label: "Michigan", value: "Michigan" }
        ]
    },
    {
        label: "SP",
        options: [
            { label: "Madrid", value: "Madrid" },
            { label: "Barcelona", value: "Barcelona" },
            { label: "Malaga", value: "Malaga" }
        ]
    },
    {
        label: "CA",
        options: [
            { label: "Montreal", value: "Montreal" },
            { label: "Toronto", value: "Toronto" },
            { label: "Vancouver", value: "Vancouver" }
        ]
    }

];

const GroupOptions2 = [
    { value: 'Zero', label: 'Zero' },
    { value: 'Two', label: 'Two' },
    { value: 'Four', label: 'Four' },
    { value: 'One', label: 'One' },
    { value: 'Five', label: 'Five' },
    { value: 'Three', label: 'Three' },
    { value: 'Six', label: 'Six' },
];

const noSortingGroup = [
    { value: 'Madrid', label: 'Madrid' },
    { value: 'Toronto', label: 'Toronto' },
    { value: 'Vancouver', label: 'Vancouver' },
    { value: 'London', label: 'London' },
    { value: 'Manchester', label: 'Manchester' },
    { value: 'Liverpool', label: 'Liverpool' },
    { value: 'Paris', label: 'Paris' },
    { value: 'Malaga', label: 'Malaga' },
    { value: 'Washington', label: 'Washington' },
    { value: 'Lyon', label: 'Lyon' },
    { value: 'Marseille', label: 'Marseille' },
    { value: 'Hamburg', label: 'Hamburg' },
    { value: 'Munich', label: 'Munich' },
    { value: 'Barcelona', label: 'Barcelona' },
    { value: 'Berlin', label: 'Berlin' },
    { value: 'Montreal', label: 'Montreal' },
    { value: 'New York', label: 'New York' },
    { value: 'Michigan', label: 'Michigan' },
];

const options = [
    { label: "josh@joshuajohnson.co.uk", value: 1, disabled: true },
    { label: "joe@bloggs.co.uk", value: 2, disabled: true }
];

const FormSelect = () => {
    document.title="Form Select | Velzon - React Admin & Dashboard Template";

    const [selectedSingle, setSelectedSingle] = useState(null);
    const [selectedGroup, setSelectedGroup] = useState(null);
    const [selectedGroup2, setSelectedGroup2] = useState(null);
    const [selectedNoSortingGroup, setSelectedNoSortingGroup] = useState(null);
    const [selectedMulti, setselectedMulti] = useState(null);
    const [selectedMulti2, setselectedMulti2] = useState(null);
    const [selectedMulti3, setselectedMulti3] = useState(null);

    function handleSelectSingle(selectedSingle) {
        setSelectedSingle(selectedSingle);
    }

    function handleSelectGroups(selectedGroup) {
        setSelectedGroup(selectedGroup);
    }

    function handleSelectGroups2(selectedGroup2) {
        setSelectedGroup2(selectedGroup2);
    }

    function handleSelectNoSortingGroup(selectedNoSortingGroup) {
        setSelectedNoSortingGroup(selectedNoSortingGroup);
    }

    function handleMulti(selectedMulti) {
        setselectedMulti(selectedMulti);
    }

    function handleMulti2(selectedMulti2) {
        setselectedMulti2(selectedMulti2);
    }

    function handleMulti3(selectedMulti3) {
        setselectedMulti3(selectedMulti3);
    }


    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">                
                <div className="container-fluid">
                    <BreadCrumb title="Form Select" pageTitle="Forms" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Default Select" />
                                <CardBody>                                    
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={6} >
                                                <select className="form-select mb-3" aria-label="Default select example">
                                                    <option >Select your Status </option>
                                                    <option value="1">Declined Payment</option>
                                                    <option value="2">Delivery Error</option>
                                                    <option value="3">Wrong Amount</option>
                                                </select>
                                            </Col>
                                            <Col lg={6}>
                                                <select className="form-select rounded-pill mb-3" aria-label="Default select example">
                                                    <option >Search for services</option>
                                                    <option defaultValue="1">Information Architecture</option>
                                                    <option defaultValue="2">UI/UX Design</option>
                                                    <option defaultValue="3">Back End Development</option>
                                                </select>
                                            </Col>
                                            <Col lg={6}>
                                                <select className="form-select" aria-label="Disabled select example" disabled>
                                                    <option >Open this select menu (Disabled)</option>
                                                    <option defaultValue="1">One</option>
                                                    <option defaultValue="2">Two</option>
                                                    <option defaultValue="3">Three</option>
                                                </select>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <DefaultSelect />
                                            </code></pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card >
                                <PreviewCardHeader title="Menu Size" />
                                <CardBody>                                   
                                    <div className="live-preview">
                                        <Row className="gy-4">
                                            <Col lg={6}>
                                                <select className="form-select" multiple aria-label="multiple select example">
                                                    <option >Open this select menu (multiple select option)</option>
                                                    <option defaultValue="1">One</option>
                                                    <option defaultValue="2">Two</option>
                                                    <option defaultValue="3">Three</option>
                                                </select>
                                            </Col>
                                            <Col lg={6}>
                                                <select className="form-select" size="3" aria-label="size 3 select example">
                                                    <option >Open this select menu (select menu size)</option>
                                                    <option defaultValue="1">One</option>
                                                    <option defaultValue="2">Two</option>
                                                    <option defaultValue="3">Three</option>
                                                    <option defaultValue="4">Four</option>
                                                    <option defaultValue="5">Five</option>
                                                </select>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <MenuSize />
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
                                <PreviewCardHeader title="Select Size" />
                                <CardBody>
                                    <p className="text-muted">Use <code>form-select-sm</code> class to set small select size and <code>form-select-lg</code> class to form-select class to
                                        set large select size. No such class is required for default select size.</p>
                                    <div className="live-preview">
                                        <Row className="align-items-center g-3">
                                            <Col lg={4}>
                                                <select className="form-select form-select-sm" aria-label=".form-select-sm example">
                                                    <option >Open this select menu</option>
                                                    <option value="1">One</option>
                                                    <option value="2">Two</option>
                                                    <option value="3">Three</option>
                                                </select>
                                            </Col>
                                            <Col lg={4}>
                                                <select className="form-select" aria-label=".form-select-sm example">
                                                    <option >Open this select menu</option>
                                                    <option defaultValue="1">One</option>
                                                    <option defaultValue="2">Two</option>
                                                    <option defaultValue="3">Three</option>
                                                </select>
                                            </Col>
                                            <Col lg={4}>
                                                <select className="form-select form-select-lg" aria-label=".form-select-lg example">
                                                    <option >Open this select menu</option>
                                                    <option defaultValue="1">One</option>
                                                    <option defaultValue="2">Two</option>
                                                    <option defaultValue="3">Three</option>
                                                </select>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <SelectSize />
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
                                <CardHeader><h4 className="card-title mb-0">Choices</h4></CardHeader>
                                <CardBody>
                                    <div>
                                        <div>                                            
                                            <Row>
                                                <Col lg={4} md={6}>
                                                    <div className="mb-3">
                                                        <Label htmlFor="choices-single-default" className="form-label text-muted">Default</Label>                                                        
                                                        <Select
                                                            value={selectedSingle}
                                                            onChange={() => {
                                                                handleSelectSingle();
                                                            }}
                                                            options={SingleOptions}
                                                        />
                                                    </div>
                                                </Col>

                                                <Col lg={4} md={6}>
                                                    <div className="mb-3">
                                                        <Label htmlFor="choices-single-groups" className="form-label text-muted">Option
                                                            Groups</Label>                                                        
                                                        <Select
                                                            value={selectedGroup}
                                                            // searchable
                                                            onChange={() => {
                                                                handleSelectGroups();
                                                            }}
                                                            options={GroupOptions}
                                                        />
                                                    </div>
                                                </Col>

                                                <Col lg={4} md={6}>
                                                    <div className="mb-3">
                                                        <Label htmlFor="choices-single-no-search" className="form-label text-muted">Options added
                                                            via config with no search</Label>                                                        
                                                        <Select
                                                            isClearable={true}
                                                            value={selectedGroup2}
                                                            onChange={() => {
                                                                handleSelectGroups2();
                                                            }}
                                                            options={GroupOptions2}
                                                        />
                                                    </div>
                                                </Col>

                                                <Col lg={4} md={6}>
                                                    <div className="mb-3">
                                                        <Label htmlFor="choices-single-no-sorting" className="form-label text-muted">Options added
                                                            via config with no sorting</Label>                                                        
                                                        <Select
                                                            value={selectedNoSortingGroup}
                                                            onChange={() => {
                                                                handleSelectNoSortingGroup();
                                                            }}
                                                            options={noSortingGroup}
                                                        />
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>

                                        <div className="mt-4">
                                            <h5 className="fs-14 mb-3">Multiple select input</h5>
                                            <Row>
                                                <Col lg={4} md={6}>
                                                    <div className="mb-3">
                                                        <Label htmlFor="choices-multiple-default" className="form-label text-muted">Default</Label>                                                        
                                                        <Select
                                                            value={selectedMulti}
                                                            isMulti={true}                                                            
                                                            onChange={() => {
                                                                handleMulti();
                                                            }}
                                                            options={SingleOptions}
                                                        />
                                                    </div>
                                                </Col>

                                                <Col lg={4} md={6}>
                                                    <div className="mb-3">
                                                        <Label htmlFor="choices-multiple-remove-button" className="form-label text-muted">With
                                                            remove button</Label>                                                        
                                                        <Select
                                                            value={selectedMulti2}
                                                            isMulti={true}
                                                            isClearable={true}
                                                            onChange={() => {
                                                                handleMulti2();
                                                            }}
                                                            options={SingleOptions}
                                                        />
                                                    </div>
                                                </Col>

                                                <Col lg={4} md={6}>
                                                    <div className="mb-3">
                                                        <Label htmlFor="choices-multiple-groups" className="form-label text-muted">Option
                                                            groups</Label>                                                      
                                                        <Select
                                                            value={selectedMulti3}
                                                            isMulti={true}
                                                            onChange={() => {
                                                                handleMulti3();
                                                            }}
                                                            options={GroupOptions}
                                                            closeMenuOnSelect={false}
                                                            components={animatedComponents}
                                                        />
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>


                                        <div className="mt-4">
                                            {/* <h5 className="fs-14 mb-3">Text inputs</h5>
                                            <Row>
                                                <Col lg={4} md={6}>
                                                    <div className="mb-3">
                                                        <Label htmlFor="choices-text-remove-button" className="form-label text-muted">Set limit
                                                            values with remove button</Label>                                                        
                                                        <Input className="form-control" onFocus={handleFocus}
                                                            defaultValue="Task-1" />
                                                    </div>
                                                </Col>
                                                <Col lg={4} md={6}>
                                                    <div className="mb-3">
                                                        <Label htmlFor="choices-text-unique-values" className="form-label text-muted">Unique values
                                                            only, no pasting</Label>                                                       
                                                        <Input className="form-control" id="choices-text-unique-values" data-choices data-choices-text-unique-true type="text"
                                                            defaultValue="Project-A, Project-B" />
                                                    </div>
                                                </Col>
                                            </Row> */}
                                            <div>
                                                <Label htmlFor="choices-text-disabled" className="form-label text-muted">Disabled</Label>                                                
                                                <Select id={'dropdown'}
                                                    options={options}
                                                    isOptionDisabled={(option) => option.disabled}>
                                                </Select>
                                            </div>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </div>
            </div>

        </React.Fragment>
    );
};

export default FormSelect;
