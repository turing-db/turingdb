import React from 'react'
import PrismCode from '../../../Components/Common/Prism';
//InputExample code
const InputExample = () => {
    const code =
`<!-- Basic Input -->
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
`
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                language={"react"}
            />
        </React.Fragment>
    )
}

//InputSizing code
const InputSizing = () => {
    const code =
`<Col lg={4}>
<Input className="form-control form-control-sm" type="text" placeholder=".form-control-sm" />
</Col>

<Col lg={4}>
<Input className="form-control" type="text" placeholder=".form-control" />
</Col>

<Col lg={4}>
<Input className="form-control form-control-lg" type="text" placeholder=".form-control-lg" />
</Col>
`
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                language={"react"}
            />
        </React.Fragment>
    )
}

//FileInput code
const FileInput = () => {
    const code =
`<!-- Default File Input Example -->
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
`
return (
    <React.Fragment>
        <PrismCode
            code={code}
            language={"react"}
        />
    </React.Fragment>
)
}

//Input Group code
const InputGroup = () => {
const code =
`<div>
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
`
return (
    <React.Fragment>
        <PrismCode
            code={code}
            language={"react"}
        />
    </React.Fragment>
)
}

//Input Group Sizing
const InputGroupSizing = () => {
    const code =
`<!-- Input Group Sizing -->
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
`
return (
    <React.Fragment>
        <PrismCode
            code={code}
            language={"react"}
        />
    </React.Fragment>
)
}

//Multiple Inputs code
const MultipleInputs = () => {
    const code =
`<!-- Multiple Inputs -->
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
`
return (
    <React.Fragment>
        <PrismCode
            code={code}
            language={"react"}
        />
    </React.Fragment>
)
}

//ButtonsCheckboxesRadiosGroup code
const ButtonsCheckboxesRadiosGroup = () => {
const code =
`<!-- Checkbox Input -->
<div className="input-group">
    <div className="input-group-text">
        <input className="form-check-input mt-0" type="checkbox" value="" aria-label="Checkbox for following text input">
    </div>
    <input type="text" className="form-control" aria-label="Text input with checkbox">
</div>

<!-- Radio Input -->
<div className="input-group">
    <div className="input-group-text">
        <input className="form-check-input mt-0" type="radio" value="" aria-label="Radio button for following text input">
    </div>
    <input type="text" className="form-control" aria-label="Text input with radio button">
</div>

<!-- Buttons Input -->
<div className="input-group">
    <button className="btn btn-outline-primary" type="button" id="button-addon1">Button</button>
    <input type="text" className="form-control" placeholder="" aria-label="Example text with button addon" aria-describedby="button-addon1">
</div>
<div className="input-group">
    <input type="text" className="form-control" aria-label="Recipient's username" aria-describedby="button-addon2">
    <button className="btn btn-outline-success" type="button" id="button-addon2">Button</button>
</div>
<div className="input-group">
    <button className="btn btn-primary" type="button">Button</button>
    <button className="btn btn-success" type="button">Button</button>
    <input type="text" className="form-control" placeholder="" aria-label="Example text with two button addons">
</div>
<div className="input-group">
    <input type="text" className="form-control" aria-label="Recipient's username with two button addons">
    <button className="btn btn-primary" type="button">Button</button>
    <button className="btn btn-success" type="button">Button</button>
</div>
`
        return (
            <React.Fragment>
                <PrismCode
                    code={code}
                    language={"react"}
                />
            </React.Fragment>
        )

}

//Buttons with dropdowns code
const ButtonsWithDropdowns = () => {
    const code =
` 
<!-- Buttons with dropdowns -->
<div className="input-group">
    <button className="btn btn-primary dropdown-toggle" type="button" data-bs-toggle="dropdown" aria-expanded="false">Dropdown</button>
    <ul className="dropdown-menu">
        <li><a className="dropdown-item" href="#">Action</a></li>
        <li><a className="dropdown-item" href="#">Another action</a></li>
        <li><a className="dropdown-item" href="#">Something else here</a></li>
        <li><hr className="dropdown-divider"></li>
        <li><a className="dropdown-item" href="#">Separated link</a></li>
    </ul>
    <input type="text" className="form-control" aria-label="Text input with dropdown button">
</div>

<div className="input-group">
    <input type="text" className="form-control" aria-label="Text input with dropdown button">
    <button className="btn btn-success dropdown-toggle" type="button" data-bs-toggle="dropdown" aria-expanded="false">Dropdown</button>
    <ul className="dropdown-menu dropdown-menu-end">
      <li><a className="dropdown-item" href="#">Action</a></li>
        <li><a className="dropdown-item" href="#">Another action</a></li>
        <li><a className="dropdown-item" href="#">Something else here</a></li>
        <li><hr className="dropdown-divider"></li>
        <li><a className="dropdown-item" href="#">Separated link</a></li>
    </ul>
</div>

<div className="input-group">
    <button className="btn btn-outline-secondary dropdown-toggle" type="button" data-bs-toggle="dropdown" aria-expanded="false">Dropdown</button>
    <ul className="dropdown-menu">
        <li><a className="dropdown-item" href="#">Action before</a></li>
        <li><a className="dropdown-item" href="#">Another action before</a></li>
        <li><a className="dropdown-item" href="#">Something else here</a></li>
        <li><hr className="dropdown-divider"></li>
        <li><a className="dropdown-item" href="#">Separated link</a></li>
    </ul>
    <input type="text" className="form-control" aria-label="Text input with 2 dropdown buttons">
    <button className="btn btn-outline-secondary dropdown-toggle" type="button" data-bs-toggle="dropdown" aria-expanded="false">Dropdown</button>
    <ul className="dropdown-menu dropdown-menu-end">
        <li><a className="dropdown-item" href="#">Action</a></li>
        <li><a className="dropdown-item" href="#">Another action</a></li>
        <li><a className="dropdown-item" href="#">Something else here</a></li>
        <li><hr className="dropdown-divider"></li>
        <li><a className="dropdown-item" href="#">Separated link</a></li>
    </ul>
</div>
`
        return (
            <React.Fragment>
                <PrismCode
                    code={code}
                    language={"react"}
                />
            </React.Fragment>
        )
}

//Custom Forms code    
const CustomForms = () => {
    const code =
`<!-- Select -->
<div className="input-group">
    <label className="input-group-text" for="inputGroupSelect01">Options</label>
    <select className="form-select" id="inputGroupSelect01">
        <option selected>Choose...</option>
        <option value="1">One</option>
        <option value="2">Two</option>
        <option value="3">Three</option>
    </select>
</div>
<div className="input-group">
    <select className="form-select" id="inputGroupSelect02">
        <option selected>Choose...</option>
        <option value="1">One</option>
        <option value="2">Two</option>
        <option value="3">Three</option>
        </select>
    <label className="input-group-text" for="inputGroupSelect02">Options</label>
</div>
<div className="input-group">
    <button className="btn btn-outline-primary" type="button">Button</button>
    <select className="form-select" id="inputGroupSelect03" aria-label="Example select with button addon">
        <option selected>Choose...</option>
        <option value="1">One</option>
        <option value="2">Two</option>
        <option value="3">Three</option>
    </select>
</div>
<div className="input-group">
    <select className="form-select" id="inputGroupSelect04" aria-label="Example select with button addon">
        <option selected>Choose...</option>
        <option value="1">One</option>
        <option value="2">Two</option>
        <option value="3">Three</option>
    </select>
    <button className="btn btn-outline-secondary" type="button">Button</button>
</div>

<!-- File Input -->
<div className="input-group">
    <label className="input-group-text" for="inputGroupFile01">Upload</label>
    <input type="file" className="form-control" id="inputGroupFile01">
</div>
<div className="input-group">
    <input type="file" className="form-control" id="inputGroupFile02">
    <label className="input-group-text" for="inputGroupFile02">Upload</label>
</div>
<div className="input-group">
    <button className="btn btn-outline-primary" type="button" id="inputGroupFileAddon03">Button</button>
    <input type="file" className="form-control" id="inputGroupFile03" aria-describedby="inputGroupFileAddon03" aria-label="Upload">
</div>
<div className="input-group">
    <input type="file" className="form-control" id="inputGroupFile04" aria-describedby="inputGroupFileAddon04" aria-label="Upload">
    <button className="btn btn-outline-success" type="button" id="inputGroupFileAddon04">Button</button>
</div>
`
        return (
            <React.Fragment>
                <PrismCode
                    code={code}
                    language={"react"}
                />
            </React.Fragment>
        )
}


export { InputExample, InputSizing, FileInput, InputGroup, InputGroupSizing, MultipleInputs, ButtonsCheckboxesRadiosGroup, ButtonsWithDropdowns, CustomForms }
