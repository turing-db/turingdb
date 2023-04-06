import React from 'react';
import PrismCode from '../../../Components/Common/Prism';

//FormGrid code
const FormGrid = () => {
const code =
`<Col md={6}>
<div className="mb-3">
    <Label for="firstNameinput" className="form-label">First Name</Label>
    <Input type="text" className="form-control" placeholder="Enter your firstname" id="firstNameinput"/>
</div>
</Col>
<Col md={6}>
    <div className="mb-3">
        <Label for="lastNameinput" className="form-label">Last Name</Label>
        <Input type="text" className="form-control" placeholder="Enter your lastname" id="lastNameinput"/>
    </div>
</Col>
<Col md={12}>
    <div className="mb-3">
        <Label for="compnayNameinput" className="form-label">Company Name</Label>
        <Input type="text" className="form-control" placeholder="Enter company name" id="compnayNameinput"/>
    </div>
</Col>
<Col md={6}>
    <div className="mb-3">
        <Label for="phonenumberInput" className="form-label">Phone Number</Label>
        <Input type="tel" className="form-control" placeholder="+(245) 451 45123" id="phonenumberInput"/>
    </div>
</Col>
<Col md={6}>
    <div className="mb-3">
        <Label for="emailidInput" className="form-label">Email Address</Label>
        <Input type="email" className="form-control" placeholder="example@gamil.com" id="emailidInput"/>
    </div>
</Col>
<Col md={12}>
    <div className="mb-3">
        <Label for="address1ControlTextarea" className="form-label">Address</Label>
        <Input type="text" className="form-control" placeholder="Address 1" id="address1ControlTextarea"/>
    </div>
</Col>
<Col md={6}>
    <div className="mb-3">
        <Label for="citynameInput" className="form-label">City</Label>
        <Input type="email" className="form-control" placeholder="Enter your city" id="citynameInput"/>
    </div>
</Col>
<Col md={6}>
    <div className="mb-3">
        <Label for="ForminputState" className="form-label">State</Label>
        <select id="ForminputState" className="form-select" data-choices data-choices-sorting="true" >
            <option>Choose...</option>
            <option>...</option>
        </select>
    </div>
</Col>
<Col md={12}>
    <div className="text-end">
        <button type="submit" className="btn btn-primary">Submit</button>
    </div>
</Col>
`;

    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

//Gutters
const Gutters = () => {
    const code = `
<Col md={12}>
<Label for="fullnameInput" className="form-label">Name</Label>
<Input type="text" className="form-control" id="fullnameInput" placeholder="Enter your name"/>
</Col>
<Col md={6}>
    <Label for="inputEmail4" className="form-label">Email</Label>
    <Input type="email" className="form-control" id="inputEmail4" placeholder="Email"/>
</Col>
<Col md={6}>
    <Label for="inputPassword4" className="form-label">Password</Label>
    <Input type="password" className="form-control" id="inputPassword4" placeholder="Password"/>
</Col>
<Col xs={12}>
    <Label for="inputAddress" className="form-label">Address</Label>
    <Input type="text" className="form-control" id="inputAddress" placeholder="1234 Main St"/>
</Col>
<Col xs={12}>
    <Label for="inputAddress2" className="form-label">Address 2</Label>
    <Input type="text" className="form-control" id="inputAddress2" placeholder="Apartment, studio, or floor"/>
</Col>
<Col md={6}>
    <Label for="inputCity" className="form-label">City</Label>
    <Input type="text" className="form-control" id="inputCity" placeholder="Enter your city"/>
</Col>
<Col md={4}>
    <Label for="inputState" className="form-label">State</Label>
    <select id="inputState" className="form-select" data-choices data-choices-sorting="true">
        <option selected>Choose...</option>
        <option>...</option>
    </select>
</Col>
<Col md={2}>
    <Label for="inputZip" className="form-label">Zip</Label>
    <Input type="text" className="form-control" id="inputZip" placeholder="Zin code"/>
</Col>
<Col xs={12}>
    <div className="form-check">
        <Input className="form-check-input" type="checkbox" id="gridCheck"/>
        <Label className="form-check-label" for="gridCheck">
            Check me out
        </Label>
    </div>
</Col>
<Col xs={12}>
    <div className="text-end">
        <button type="submit" className="btn btn-primary">Sign in</button>
    </div>
</Col>
`;

    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

//Vertical Form
const VerticalForm = () => {
    const code = `
<div className="mb-3">
<Label htmlFor="employeeName" className="form-label">Employee Name</Label>
    <Input type="text" className="form-control" id="employeeName" placeholder="Enter emploree name"/>
</div>
<div className="mb-3">
    <Label htmlFor="employeeUrl" className="form-label">Employee Department URL</Label>
    <Input type="url" className="form-control" id="employeeUrl" placeholder="Enter emploree url"/>
</div>
<div className="mb-3">
    <Label htmlFor="StartleaveDate" className="form-label">Start Leave Date</Label>
    <Flatpickr
        className="form-control"
        options={{
        dateFormat: "d M, Y"
        }}
    />
</div>
<div className="mb-3">
    <Label htmlFor="EndleaveDate" className="form-label">End Leave Date</Label>
    <Flatpickr
        className="form-control"
        options={{
        dateFormat: "d M, Y"
        }}
    />
</div>
<div className="mb-3">
    <Label htmlFor="VertimeassageInput" className="form-label">Message</Label>
    <textarea className="form-control" id="VertimeassageInput" rows="3" placeholder="Enter your message"></textarea>
</div>
<div className="text-end">
    <button type="submit" className="btn btn-primary">Add Leave</button>
</div>
`;

    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

//Horizontal Form
const HorizontalForm = () => {
    const code = `<Row className="mb-3">
<Col lg={3} >
    <Label htmlFor="nameInput" className="form-label">Name</Label>
</Col>
<Col lg={9} >
    <Input type="text" className="form-control" id="nameInput" placeholder="Enter your name"/>
</Col>
</Row>
<Row className="mb-3">
<Col lg={3} >
    <Label htmlFor="websiteUrl" className="form-label">Website URL</Label>
</Col>
<Col lg={9} >
    <Input type="url" className="form-control" id="websiteUrl" placeholder="Enter your url"/>
</Col>
</Row>
<Row className="mb-3">
<Col lg={3} >
    <Label htmlFor="dateInput" className="form-label">Date</Label>
</Col>
<Col lg={9} >
    <Flatpickr
        className="form-control"
        options={{
        dateFormat: "d M, Y"
        }}
    />
</Col>
</Row>
<Row className="mb-3">
<Col lg={3} >
    <Label htmlFor="timeInput" className="form-label">Time</Label>
</Col>
<Col lg={9} >
<Flatpickr
    className="form-control"
    options={{
        enableTime: true,
        noCalendar: true,
        dateFormat: "H:i",
    }}
/>
</Col>
</Row>
<Row className="mb-3">
<Col lg={3} >
    <Label htmlFor="leaveemails" className="form-label">Email Id</Label>
</Col>
<Col lg={9} >
    <Input type="email" className="form-control" id="leaveemails" placeholder="Enter your email"/>
</Col>
</Row>
<Row className="mb-3">
<Col lg={3} >
    <Label htmlFor="contactNumber" className="form-label">Contact Number</Label>
</Col>
<Col lg={9} >
    <Input type="number" className="form-control" id="contactNumber" placeholder="+91 9876543210"/>
</Col>
</Row>
<Row className="mb-3">
<Col lg={3} >
    <label htmlFor="meassageInput" className="form-label">Message</label>
</Col>
<Col lg={9} >
    <textarea className="form-control" id="meassageInput" rows="3" placeholder="Enter your message"></textarea>
</Col>
</Row>
<div className="text-end">
<button type="submit" className="btn btn-primary">Add Leave</button>
</div>
`;
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

//Horizontal Form Label Sizing
const HorizontalFormLabelSizing = () => {
    const code = `<Col xxl={4}>
<Row>
    <Label htmlFor="colFormLabelSm" className="col-sm-2 col-form-label col-form-label-sm">Email</Label>
    <div className="col-sm-10">
        <Input type="email" className="form-control form-control-sm" id="colFormLabelSm" placeholder="col-form-label-sm"/>
    </div>
</Row>
</Col>
<Col xxl={4}>
<Row>
    <label htmlFor="colFormLabel" className="col-sm-2 col-form-label">Email</label>
    <div className="col-sm-10">
        <Input type="email" className="form-control" id="colFormLabel" placeholder="col-form-label"/>
    </div>
</Row>
</Col>
<Col xxl={4}>
<Row>
    <label htmlFor="colFormLabelLg" className="col-sm-2 col-form-label col-form-label-lg">Email</label>
    <div className="col-sm-10">
        <Input type="email" className="form-control form-control-lg" id="colFormLabelLg" placeholder="col-form-label-lg"/>
    </div>
</Row>
</Col>
`;
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

//Column Sizing
const ColumnSizing = () => {
    const code = `<Col sm={6}>
<Input type="text" className="form-control" placeholder="Firstname" aria-label="First-Name"/>
</Col>
<Col sm={6}>
    <Input type="text" className="form-control" placeholder="Lastname" aria-label="Last-Name"/>
</Col>
<Col sm={4}>
    <Input type="text" className="form-control" placeholder="Email id" aria-label="Email Id"/>
</Col>
<Col sm={4}>
    <Input type="password" className="form-control" placeholder="Password" aria-label="Password"/>
</Col>
<Col sm={4}>
    <Input type="password" className="form-control" placeholder="Confirm Password" aria-label="confirm-password"/>
</Col>
<Col sm={7}>
    <Input type="text" className="form-control" placeholder="City" aria-label="City"/>
</Col>
<Col sm>
    <Input type="text" className="form-control" placeholder="State" aria-label="State"/>
</Col>
<Col sm>
    <Input type="text" className="form-control" placeholder="Zip" aria-label="Zip"/>
</Col>
    `;
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

//Auto Sizing
const AutoSizing = () => {
const code = `<Form action="#">
<Row className="gy-2 gx-3 mb-3 align-items-center">
    <Col sm>
        <Label className="visually-hidden" htmlFor="autoSizingInput">Name</Label>
        <Input type="text" className="form-control" id="autoSizingInput" placeholder="Jane Doe"/>
    </Col>
    <Col sm>
        <Label className="visually-hidden" htmlFor="autoSizingInputGroup">Username</Label>
        <div className="input-group">
            <div className="input-group-text">@</div>
            <Input type="text" className="form-control" id="autoSizingInputGroup" placeholder="Username"/>
        </div>
    </Col>
    <Col sm>
        <Label className="visually-hidden" htmlFor="autoSizingSelect">Preference</Label>
        <select className="form-select" data-choices data-choices-sorting="true" id="autoSizingSelect">
            <option >Choose...</option>
            <option defaultValue="1">One</option>
            <option defaultValue="2">Two</option>
            <option defaultValue="3">Three</option>
        </select>
    </Col>
    <Col className="sm-auto">
        <div className="form-check">
            <Input className="form-check-input" type="checkbox" id="autoSizingCheck"/>
            <label className="form-check-label" htmlFor="autoSizingCheck">
                Remember me
            </label>
        </div>
    </Col>
    <Col className="sm-auto">
        <button type="submit" className="btn btn-primary">Submit</button>
    </Col>
</Row>
</Form>

<Form>
<Row className="gx-3 gy-2 align-items-center">
    <Col sm={3}>
        <Label className="visually-hidden" htmlFor="specificSizeInputName">Name</Label>
        <Input type="text" className="form-control" id="specificSizeInputName" placeholder="Jane Doe"/>
    </Col>
    <Col sm={3}>
        <Label className="visually-hidden" htmlFor="specificSizeInputGroupUsername">Username</Label>
        <InputGroup>
            <div className="input-group-text">@</div>
            <Input type="text" className="form-control" id="specificSizeInputGroupUsername" placeholder="Username"/>
        </InputGroup>
    </Col>
    <Col sm={3}>
        <Label className="visually-hidden" htmlFor="specificSizeSelect">Preference</Label>
        <select className="form-select" data-choices data-choices-sorting="true" id="specificSizeSelect">
            <option >Choose...</option>
            <option defaultValue="1">One</option>
            <option defaultValue="2">Two</option>
            <option defaultValue="3">Three</option>
        </select>
    </Col>
    <div className="col-auto">
        <div className="form-check">
            <Input className="form-check-input" type="checkbox" id="autoSizingCheck2"/>
            <Label className="form-check-label" htmlFor="autoSizingCheck2">
                Remember me
            </Label>
        </div>
    </div>
    <Col >
        <button type="submit" className="btn btn-primary">Submit</button>
    </Col>
</Row>
</Form>
`;
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

//Inline Forms
const InlineForms = () => {
const code = `<Row className="row-cols-lg-auto g-3 align-items-center">
<Col xs={12}>
    <Label className="visually-hidden" htmlFor="inlineFormInputGroupUsername">Username</Label>
    <div className="input-group">
        <div className="input-group-text">@</div>
        <Input type="text" className="form-control" id="inlineFormInputGroupUsername" placeholder="Username" />
    </div>
</Col>
<Col xs={12}>
    <Label className="visually-hidden" htmlFor="inlineFormSelectPref">Preference</Label>
    <select className="form-select" data-choices data-choices-sorting="true" id="inlineFormSelectPref">
        <option >Choose...</option>
        <option defaultValue="1">One</option>
        <option defaultValue="2">Two</option>
        <option defaultValue="3">Three</option>
    </select>
</Col>
<Col xs={12}>
    <div className="form-check">
        <Input className="form-check-input" type="checkbox" id="inlineFormCheck" />
        <Label className="form-check-label" htmlFor="inlineFormCheck">
            Remember me
        </Label>
    </div>
</Col>
<Col xs={12}>
    <button type="submit" className="btn btn-primary">Submit</button>
</Col>
</Row>
`;
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

//Inline Forms
const FloatingLabels = () => {
    const code = `<Form action="#">
<Row className="g-3">
    <Col lg={6}>
        <div className="form-floating">
            <Input type="text" className="form-control" id="firstnamefloatingInput" placeholder="Enter your firstname"/>
            <Label htmlFor="firstnamefloatingInput">First Name</Label>
        </div>
    </Col>
    <Col lg={6}>
        <div className="form-floating">
            <Input type="text" className="form-control" id="lastnamefloatingInput" placeholder="Enter your Lastname"/>
            <Label htmlFor="lastnamefloatingInput">Last Name</Label>
        </div>
    </Col>
    <Col lg={4}>
        <div className="form-floating">
            <Input type="email" className="form-control" id="emailfloatingInput" placeholder="Enter your email"/>
            <Label htmlFor="emailfloatingInput">Email Address</Label>
        </div>
    </Col>
    <Col lg={4}>
        <div className="form-floating">
            <Input type="password" className="form-control" id="passwordfloatingInput" placeholder="Enter your password"/>
            <Label htmlFor="passwordfloatingInput">Password</Label>
        </div>
    </Col>
    <Col lg={4}>
        <div className="form-floating">
            <Input type="password" className="form-control" id="passwordfloatingInput1" placeholder="Confirm password"/>
            <Label htmlFor="passwordfloatingInput1">Confirm Password</Label>
        </div>
    </Col>
    <Col lg={4}>
        <div className="form-floating">
            <Input type="text" className="form-control" id="cityfloatingInput" placeholder="Enter your city"/>
            <Label htmlFor="cityfloatingInput">City</Label>
        </div>
    </Col>
    <Col lg={4}>
        <div className="form-floating">
            <select className="form-select" id="floatingSelect" aria-label="Floating label select example">
              <option >Choose...</option>
              <option defaultValue="1">USA</option>
              <option defaultValue="2">Brazil</option>
              <option defaultValue="3">France</option>
              <option defaultValue="4">Germany</option>
            </select>
            <Label htmlFor="floatingSelect">Country</Label>
        </div>
    </Col>
    <Col lg={4}>
        <div className="form-floating">
            <Input type="number" className="form-control" id="zipfloatingInput" placeholder="Enter your zipcode"/>
            <Label htmlFor="zipfloatingInput">Zipcode</Label>
        </div>
    </Col>
    <Col lg={12}>
        <div className="text-end">
            <button type="submit" className="btn btn-primary">Submit</button>
        </div>
    </Col>
</Row>
</Form>
`;
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};
export { FormGrid, Gutters, VerticalForm, HorizontalForm, HorizontalFormLabelSizing, ColumnSizing, AutoSizing, InlineForms, FloatingLabels };