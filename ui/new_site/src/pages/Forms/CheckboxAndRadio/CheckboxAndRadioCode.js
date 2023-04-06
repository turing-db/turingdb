import React from 'react';
import PrismCode from '../../../Components/Common/Prism';

//Checkbox
const Checkbox = () => {
const code =`<Col lg={4} md={6}>                                                    
<div>   
    <p className="text-muted fw-medium">Default Checkbox</p>
    <p className="text-muted">Use <code>type="checkbox"</code> attribute to set a checkbox and add <code>checked</code> 
        attribute to set a checkbox checked by default.</p>
    <div className="form-check mb-2">
        <Input className="form-check-input" type="checkbox" id="formCheck1"/>
        <Label className="form-check-label" for="formCheck1">
            Default checkbox
        </Label>
    </div>                                                        
    <div className="form-check">
        <Input className="form-check-input" type="checkbox" id="formCheck2" defaultChecked/>                                                           / 
        <Label className="form-check-label" for="formCheck2">
            Checked checkbox
        </Label>
    </div>
</div>
</Col>

<Col lg={4} md={6}> 
    <div className="mt-4 mt-md-0">
        <p className="text-muted fw-medium">Disabled Checkbox</p>
        <p className="text-muted">Use <code>disabled</code> attribute to set a checkbox disabled and add <code>checked</code> 
            attribute to set a checkbox checked by default.</p>
        <div>
            <div className="form-check form-check-right mb-2">
                <Input className="form-check-input" type="checkbox" value="" id="flexCheckDisabled"  disabled/>
                <Label className="form-check-label" for="flexCheckDisabled">
                    Disabled checkbox
                </Label>
            </div>
        </div>
        <div>
            <div className="form-check form-check-right">
                <Input className="form-check-input" type="checkbox" value="" id="flexCheckCheckedDisabled" defaultChecked disabled/>
                <Label className="form-check-label" for="flexCheckCheckedDisabled">
                    Disabled checked checkbox
                </Label>
            </div>
        </div>
    </div>
</Col>

<Col lg={4} md={6}> 
    <div className="mt-4 mt-md-0">
        <p className="text-muted fw-medium">Checkbox Right</p>
        <p className="text-muted">Use <code>form-check-right</code> class to form-check class to set a checkbox on the right side.</p>
        <div className="form-check form-check-right mb-2">
            <Input className="form-check-input" type="checkbox" name="formCheckboxRight" id="formCheckboxRight1" />
            <Label className="form-check-label" for="formCheckboxRight1">
                Form Radio Right
            </Label>
        </div>
        <div>
            <div className="form-check form-check-right">
                <Input className="form-check-input" type="checkbox" value="" id="flexCheckCheckedRightDisabled" defaultChecked disabled/>
                <Label className="form-check-label" for="flexCheckCheckedRightDisabled">
                    Disabled checked checkbox
                </Label>
            </div>
        </div>
    </div>
</Col>

<Col lg={6}>
    <div className="mt-3">
        <p className="text-muted fw-medium">Indeterminate</p>
        <div>
            <div className="form-check">
                <Input className="form-check-input" type="checkbox" value="" id="defaultIndeterminateCheck"/>
                <Label className="form-check-label" for="defaultIndeterminateCheck">
                    Indeterminate checkbox
                </Label>
            </div>
        </div>
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

//Custom Checkbox
const CustomCheckbox = () => {
    const code = `<Col md={6}>
    <div>                                               
        <p className="text-muted">Use <code>form-check-</code> class with below-mentioned color variation to set a color checkbox.</p>                                                        
        <div>
            <div className="form-check mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck6" defaultChecked/>
                <Label className="form-check-label" for="formCheck6">
                    Checkbox Primary
                </Label>
            </div>
            <div className="form-check form-check-secondary mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck7" defaultChecked/>
                <Label className="form-check-label" for="formCheck7">
                    Checkbox Secondary
                </Label>
            </div>
            <div className="form-check form-check-success mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck8" defaultChecked/>
                <Label className="form-check-label" for="formCheck8">
                    Checkbox Success
                </Label>
            </div>
            <div className="form-check form-check-warning mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck9" defaultChecked/>
                <Label className="form-check-label" for="formCheck9">
                    Checkbox Warning
                </Label>
            </div>
            <div className="form-check form-check-danger mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck10" defaultChecked/>
                <Label className="form-check-label" for="formCheck10">
                    Checkbox Danger
                </Label>
            </div>
            <div className="form-check form-check-info mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck11" defaultChecked/>
                <Label className="form-check-label" for="formCheck11">
                    Checkbox Info
                </Label>
            </div>
            <div className="form-check form-check-dark">
                <Input className="form-check-input" type="checkbox" id="formCheck12" defaultChecked/>
                <Label className="form-check-label" for="formCheck12">
                    Checkbox Dark
                </Label>
            </div>
        </div>
    </div>
</Col>

<Col md={6}>
    <div className="mt-4 mt-md-0">
        <p className="text-muted">Use <code>form-check-outline</code> class and <code>form-check-</code> class with below-mentioned color variation to set a color checkbox with outline.</p>
        
        <div>
            <div className="form-check form-check-outline form-check-primary mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck13" defaultChecked/>
                <Label className="form-check-label" for="formCheck13">
                    Checkbox Outline Primary
                </Label>
            </div>
            <div className="form-check form-check-outline form-check-secondary mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck14" defaultChecked/>
                <Label className="form-check-label" for="formCheck14">
                    Checkbox Outline Secondary
                </Label>
            </div>
            <div className="form-check form-check-outline form-check-success mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck15" defaultChecked/>
                <Label className="form-check-label" for="formCheck15">
                    Checkbox Outline Success
                </Label>
            </div>
            <div className="form-check form-check-outline form-check-warning mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck16" defaultChecked/>
                <Label className="form-check-label" for="formCheck16">
                    Checkbox Outline Warning
                </Label>
            </div>
            <div className="form-check form-check-outline form-check-danger mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck17" defaultChecked/>
                <Label className="form-check-label" for="formCheck17">
                    Checkbox Outline Danger
                </Label>
            </div>
            <div className="form-check form-check-outline form-check-info mb-3">
                <Input className="form-check-input" type="checkbox" id="formCheck18" defaultChecked/>
                <Label className="form-check-label" for="formCheck18">
                    Checkbox Outline Info
                </Label>
            </div>
            <div className="form-check form-check-outline form-check-dark">
                <Input className="form-check-input" type="checkbox" id="formCheck19" defaultChecked/>
                <Label className="form-check-label" for="formCheck19">
                    Checkbox Outline Dark
                </Label>
            </div>
        </div>
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

//Radio
const Radio = () => {
    const code = `<Col lg={4} md={6}>
<div>
    <p className="text-muted fw-medium">Default Radios</p>
    <p className="text-muted">Use <code>type="radio"</code> attribute to set a radio button and add <code>defaultChecked</code> attribute to set a radio checked by default.</p>
    <div className="form-check mb-2">
        <Input className="form-check-input" type="radio" name="flexRadioDefault" id="flexRadioDefault1" />
        <Label className="form-check-label" for="flexRadioDefault1">
            Default radio
        </Label>
    </div>
    <div className="form-check">
        <Input className="form-check-input" type="radio" name="flexRadioDefault" id="flexRadioDefault2" defaultChecked />
        <Label className="form-check-label" for="flexRadioDefault2">
            Default checked radio
        </Label>
    </div>
</div>
</Col>
<Col lg={4} md={6}>
    <div className="mt-4 mt-md-0">
        <p className="text-muted fw-medium">Disabled Radios</p>
        <p className="text-muted">Use <code>disabled</code> attribute to set a radio button disabled and add <code>defaultChecked</code> attribute to set a radio checked by default.</p>
        <div className="form-check mb-2">
            <Input className="form-check-input" type="radio" name="flexRadioDisabled" id="flexRadioDisabled" disabled />
            <Label className="form-check-label" for="flexRadioDisabled">
              Disabled radio
            </Label>
        </div>
        <div>
            <div className="form-check">
                <Input className="form-check-input" type="radio" name="flexRadioDisabled" id="flexRadioCheckedDisabled" defaultChecked disabled />
                <Label className="form-check-label" for="flexRadioCheckedDisabled">
                  Disabled checked radio
                </Label>
            </div>
        </div>
    </div>
</Col>

<Col lg={4} md={6}>
    <div className="mt-4 mt-md-0">
        <p className="text-muted fw-medium">Radio Right</p>
        <p className="text-muted">Use <code>form-check-right</code> class to form-check class to set a radio button on the right side.</p>
        <div className="form-check form-check-right mb-2">
            <Input className="form-check-input" type="radio" name="formradioRight" id="formradioRight1"/>
            <Label className="form-check-label" for="formradioRight1">
                Form Radio Right
            </Label>
        </div>
        <div>
            <div className="form-check form-check-right">
                <Input className="form-check-input" type="radio" value="" name="formradioRight" id="flexCheckCheckedDisabled2" defaultChecked disabled/>
                <Label className="form-check-label" for="flexCheckCheckedDisabled2">
                    Disabled checked radio
                </Label>
            </div>
        </div>
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

//Custom Radio
const CustomRadio = () => {
    const code = `<Col md={6}>
<div>
<p className="text-muted">Use <code>form-check-</code> class with below-mentioned color variation to set a color radio.</p>

<div className="form-check form-radio-primary mb-3">
    <Input className="form-check-input" type="radio" name="formradiocolor1" id="formradioRight5" defaultChecked/>
    <Label className="form-check-label" for="formradioRight5">
        Radio Primary
    </Label>
</div>

<div className="form-check form-radio-secondary mb-3">
    <Input className="form-check-input" type="radio" name="formradiocolor2" id="formradioRight6" defaultChecked/>
    <Label className="form-check-label" for="formradioRight6">
        Radio Secondary
    </Label>
</div>

<div className="form-check form-radio-success mb-3">
    <Input className="form-check-input" type="radio" name="formradiocolor3" id="formradioRight7" defaultChecked/>
    <Label className="form-check-label" for="formradioRight7">
        Radio Success
    </Label>
</div>

<div className="form-check form-radio-warning mb-3">
    <Input className="form-check-input" type="radio" name="formradiocolor4" id="formradioRight8" defaultChecked/>
    <Label className="form-check-label" for="formradioRight8">
        Radio Warning
    </Label>
</div>

<div className="form-check form-radio-danger mb-3">
    <Input className="form-check-input" type="radio" name="formradiocolor5" id="formradioRight9" defaultChecked/>
    <Label className="form-check-label" for="formradioRight9">
        Radio Danger
    </Label>
</div>

<div className="form-check form-radio-info mb-3">
    <Input className="form-check-input" type="radio" name="formradiocolor6" id="formradioRight10" defaultChecked/>
    <Label className="form-check-label" for="formradioRight10">
        Radio Info
    </Label>
</div>

<div className="form-check form-radio-dark">
    <Input className="form-check-input" type="radio" name="formradiocolor7" id="formradioRight11" defaultChecked/>
    <Label className="form-check-label" for="formradioRight11">
        Radio Dark
    </Label>
</div>
</div>
</Col>

<Col md={6}>
    <div className="mt-4 mt-md-0">
        <p className="text-muted">Use <code>form-check-outline</code> class and <code>form-check-</code> class with below-mentioned color variation to set a color radio with outline.</p>

        <div className="form-check form-radio-outline form-radio-primary mb-3">
            <Input className="form-check-input" type="radio" name="formradiocolor9" id="formradioRight13" defaultChecked/>
            <Label className="form-check-label" for="formradioRight13">
                Radio Outline Primary
            </Label>
        </div>

        <div className="form-check form-radio-outline form-radio-secondary mb-3">
            <Input className="form-check-input" type="radio" name="formradiocolor10" id="formradioRight14" defaultChecked/>
            <Label className="form-check-label" for="formradioRight14">
                Radio Outline Secondary
            </Label>
        </div>

        <div className="form-check form-radio-outline form-radio-success mb-3">
            <Input className="form-check-input" type="radio" name="formradiocolor11" id="formradioRight15" defaultChecked/>
            <Label className="form-check-label" for="formradioRight15">
                Radio Outline Success
            </Label>
        </div>

        <div className="form-check form-radio-outline form-radio-warning mb-3">
            <Input className="form-check-input" type="radio" name="formradiocolor12" id="formradioRight16" defaultChecked/>
            <Label className="form-check-label" for="formradioRight16">
                Radio Outline Warning
            </Label>
        </div>

        <div className="form-check form-radio-outline form-radio-danger mb-3">
            <Input className="form-check-input" type="radio" name="formradiocolor13" id="formradioRight17" defaultChecked/>
            <Label className="form-check-label" for="formradioRight17">
                Radio Outline Danger
            </Label>
        </div>

        <div className="form-check form-radio-outline form-radio-info mb-3">
            <Input className="form-check-input" type="radio" name="formradiocolor14" id="formradioRight18" defaultChecked/>
            <Label className="form-check-label" for="formradioRight18">
                Radio Outline Info
            </Label>
        </div>

        <div className="form-check form-radio-outline form-radio-dark">
            <Input className="form-check-input" type="radio" name="formradiocolor15" id="formradioRight19" defaultChecked/>
            <Label className="form-check-label" for="formradioRight19">
                Radio Outline Dark
            </Label>
        </div>

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
//Switches
const Switches = () => {
    const code = `<Col lg={4} md={6}>
<div>
    <p className="text-muted fw-medium">Default Switches</p>
    <p className="text-muted">Use <code>form-switch</code> class to form-check class to set a switch and add <code>defaultChecked</code> attribute to set a switch checked by default.</p>  
        <div className="form-check form-switch mb-2">
            <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckDefault"/>
            <Label className="form-check-label" for="flexSwitchCheckDefault">Default switch input</Label>
        </div>

        <div className="form-check form-switch">
            <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckChecked" defaultChecked/>
            <Label className="form-check-label" for="flexSwitchCheckChecked">Checked switch input</Label>
        </div>
    </div>
</Col>

<Col lg={4} md={6}>
    <div className="mt-4 mt-md-0">
        <p className="text-muted fw-medium">Disabled Switches</p> 
        <p className="text-muted">Use <code>disabled</code> attribute to set a radio button disabled and add <code>defaultChecked</code> 
            attribute to set a switch checked by default.</p>
        <div className="form-check form-switch mb-2">
            <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckDisabled" disabled/>
            <Label className="form-check-label" for="flexSwitchCheckDisabled">Switch input</Label>
        </div>
        
        <div className="form-check form-switch">
            <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckCheckedDisabled1" defaultChecked disabled/>
            <Label className="form-check-label" for="flexSwitchCheckCheckedDisabled1">Disabled checked switch input</Label>
        </div>
    </div>
</Col>

<Col lg={4} md={6}>
    <div className="mt-4 mt-md-0">
        <p className="text-muted fw-medium">Switch Right</p>
        <p className="text-muted">Use <code>form-check-right</code> class to form-check class to set a switch button on the right side.</p>
        <div>
            <div className="form-check form-switch form-check-right mb-2">
                <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckRightDisabled" defaultChecked/>
                <Label className="form-check-label" for="flexSwitchCheckRightDisabled">Switch Right input</Label>
            </div>
        </div>

        <div>
            <div className="form-check form-switch form-check-right">
                <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckCheckedDisabled2" disabled/>
                <Label className="form-check-label" for="flexSwitchCheckCheckedDisabled2">Disabled checked switch input</Label>
            </div>
        </div>
        
    </div>
</Col>

<Col lg={4} md={6}>
    <div className="mt-3">
        <p className="text-muted fw-medium">Switch sizes</p>
        <p className="text-muted">Use <code>form-switch-md</code> class to set a medium size switch button and
            <code>form-switch-lg</code> class to form-check class to set a large size switch button respectively.
            No such class is required for small size switch button.</p>

        <div className="form-check form-switch mb-3" dir="ltr">
            <Input type="checkbox" className="form-check-input" id="customSwitchsizesm" defaultChecked=""/>
            <Label className="form-check-label" for="customSwitchsizesm">Small Size Switch</Label>
        </div>

        <div className="form-check form-switch form-switch-md mb-3" dir="ltr">
            <Input type="checkbox" className="form-check-input" id="customSwitchsizemd"/>
            <Label className="form-check-label" for="customSwitchsizemd">Medium Size Switch</Label>
        </div>

        <div className="form-check form-switch form-switch-lg" dir="ltr">
            <Input type="checkbox" className="form-check-input" id="customSwitchsizelg" defaultChecked=""/>
            <Label className="form-check-label" for="customSwitchsizelg">Large Size Switch</Label>
        </div>
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

//Switch Color
const SwitchColor = () => {
    const code = `<Col md={6}>
<div>
        <div className="form-check form-switch mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck1" defaultChecked/>
            <Label className="form-check-label" for="SwitchCheck1">Switch Default</Label>
        </div>

        <div className="form-check form-switch form-switch-secondary mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck2" defaultChecked/>
            <Label className="form-check-label" for="SwitchCheck2">Switch Secondary</Label>
        </div>

        <div className="form-check form-switch form-switch-success mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck3" defaultChecked/>
            <Label className="form-check-label" for="SwitchCheck3">Switch Success</Label>
        </div>

        <div className="form-check form-switch form-switch-warning mb-2 mb-md-0">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck4" defaultChecked/>
            <Label className="form-check-label" for="SwitchCheck4">Switch Warning</Label>
        </div>
    </div>
</Col>

<Col md={6}>
    <div>
        <div className="form-check form-switch form-switch-danger mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck5" defaultChecked/>
            <Label className="form-check-label" for="SwitchCheck5">Switch Danger</Label>
        </div>

        <div className="form-check form-switch form-switch-info mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck6" defaultChecked/>
            <Label className="form-check-label" for="SwitchCheck6">Switch Info</Label>
        </div>

        <div className="form-check form-switch form-switch-dark mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck7" defaultChecked/>
            <Label className="form-check-label" for="SwitchCheck7">Switch Dark</Label>
        </div>
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

//Custom Switches
const CustomSwitches = () => {
    const code = `<Col md={6}>
<div>
        <div className="form-check form-switch form-switch-custom form-switch-primary mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck9" defaultChecked/>
            <Label className="form-check-label" htmlFor="SwitchCheck9">Switch Primary</Label>
        </div>

        <div className="form-check form-switch form-switch-custom form-switch-secondary mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck10" defaultChecked/>
            <Label className="form-check-label" htmlFor="SwitchCheck10">Switch Secondary</Label>
        </div>

        <div className="form-check form-switch form-switch-custom form-switch-success mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck11" defaultChecked/>
            <Label className="form-check-label" htmlFor="SwitchCheck11">Switch Success</Label>
        </div>

        <div className="form-check form-switch form-switch-custom form-switch-warning mb-2 mb-md-0">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck12" defaultChecked/>
            <Label className="form-check-label" htmlFor="SwitchCheck12">Switch Warning</Label>
        </div>
    </div>
</Col>

<Col md={6}>
    <div className="mt-4 mt-md-0">
        <div className="form-check form-switch form-switch-custom form-switch-danger mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck13" defaultChecked/>
            <Label className="form-check-label" htmlFor="SwitchCheck13">Switch Danger</Label>
        </div>

        <div className="form-check form-switch form-switch-custom form-switch-info mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck14" defaultChecked/>
            <Label className="form-check-label" htmlFor="SwitchCheck14">Switch Info</Label>
        </div>

        <div className="form-check form-switch form-switch-custom form-switch-dark mb-3">
            <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck15" defaultChecked/>
            <Label className="form-check-label" htmlFor="SwitchCheck15">Switch Dark</Label>
        </div>
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

//Inline Checkbox & Radio
const InlineCheckboxRadio = () => {
    const code = `<Col lg={4}>
<div className="mt-4 mt-lg-0">
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="checkbox" id="inlineCheckbox6" value="option1"/>
            <Label className="form-check-label" htmlFor="inlineCheckbox6">1</Label>
        </div>
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="checkbox" id="inlineCheckbox7" value="option2"/>
            <Label className="form-check-label" htmlFor="inlineCheckbox7">2</Label>
        </div>
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="checkbox" id="inlineCheckbox8" value="option3" disabled/>
            <Label className="form-check-label" htmlFor="inlineCheckbox8">3 (disabled)</Label>
        </div>
    </div>
</Col>

<Col lg={4}>
    <div className="mt-4 mt-lg-0">
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="radio" name="inlineRadioOptions1" id="inlineRadio1" value="option1"/>
            <Label className="form-check-label" htmlFor="inlineRadio1">1</Label>
        </div>
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="radio" name="inlineRadioOptions2" id="inlineRadio2" value="option2"/>
            <Label className="form-check-label" htmlFor="inlineRadio2">2</Label>
        </div>
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="radio" name="inlineRadioOptions3" id="inlineRadio3" value="option3" disabled/>
            <Label className="form-check-label" htmlFor="inlineRadio3">3 (disabled)</Label>
        </div>
    </div>
</Col>

<Col lg={4}>
    <div className="mt-4 mt-lg-0">
        <div className="form-check form-switch form-check-inline" dir="ltr">
            <Input type="checkbox" className="form-check-input" id="inlineswitch5"/>
            <Label className="form-check-label" htmlFor="inlineswitch5">1</Label>
        </div>
        <div className="form-check form-switch form-check-inline" dir="ltr">
            <Input type="checkbox" className="form-check-input" id="inlineswitch6"/>
            <Label className="form-check-label" htmlFor="inlineswitch6">2</Label>
        </div>
        <div className="form-check form-switch form-check-inline" dir="ltr">
            <Input type="checkbox" className="form-check-input" id="inlineswitchdisabled2" disabled/>
            <Label className="form-check-label" htmlFor="inlineswitchdisabled2">2</Label>
        </div>
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

//Without Labels
const WithoutLabels = () => {
    const code = `<Col lg={4}>
    <div className="mt-4 mt-lg-0">
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="checkbox" id="inlineCheckbox1" value="option1"/>
        </div>
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="checkbox" id="inlineCheckbox2" value="option2"/>
        </div>
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="checkbox" id="inlineCheckbox3" value="option3" disabled/>
        </div>
    </div>
</Col>


<Col lg={4}>
    <div className="mt-4 mt-lg-0">
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="radio" name="inlineRadioOptions" id="WithoutinlineRadio1" value="option1"/>
        </div>
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="radio" name="inlineRadioOptions" id="WithoutinlineRadio2" value="option2"/>
        </div>
        <div className="form-check form-check-inline">
            <Input className="form-check-input" type="radio" name="inlineRadioOptions" id="WithoutinlineRadio3" value="option3" disabled/>
        </div>
    </div>
</Col>


<Col lg={4}>
    <div className="mt-4 mt-lg-0">
        <div className="form-check form-switch form-check-inline" dir="ltr">
            <Input type="checkbox" className="form-check-input" id="inlineswitch"/>
        </div>
        <div className="form-check form-switch form-check-inline" dir="ltr">
            <Input type="checkbox" className="form-check-input" id="inlineswitch1"/>
        </div>
        <div className="form-check form-switch form-check-inline" dir="ltr">
            <Input type="checkbox" className="form-check-input" id="inlineswitchdisabled" disabled/>
        </div>
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

//Radio Toggle Buttons
const RadioToggleButtons = () => {
    const code = `<div className="hstack gap-2 flex-wrap">
<Input type="radio" className="btn-check" name="options" id="option1" defaultChecked/>
<Label className="btn btn-secondary" for="option1">Checked</Label>

<Input type="radio" className="btn-check" name="options" id="option2"/>
<Label className="btn btn-secondary" for="option2">Radio</Label>

<Input type="radio" className="btn-check" name="options" id="option3" disabled/>
<Label className="btn btn-secondary" for="option3">Disabled</Label>

<Input type="radio" className="btn-check" name="options" id="option4"/>
<Label className="btn btn-secondary" for="option4">Radio</Label>
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

//Outlined Styles
const OutlinedStyles = () => {
    const code = `<div className="hstack gap-2 flex-wrap">
<Input type="radio" className="btn-check" name="options" id="option1" defaultChecked/>
<Label className="btn btn-secondary" for="option1">Checked</Label>

<Input type="radio" className="btn-check" name="options" id="option2"/>
<Label className="btn btn-secondary" for="option2">Radio</Label>

<Input type="radio" className="btn-check" name="options" id="option3" disabled/>
<Label className="btn btn-secondary" for="option3">Disabled</Label>

<Input type="radio" className="btn-check" name="options" id="option4"/>
<Label className="btn btn-secondary" for="option4">Radio</Label>
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

export {
    Checkbox,
    CustomCheckbox,
    Radio,
    CustomRadio,
    Switches,
    SwitchColor,
    CustomSwitches,
    InlineCheckboxRadio,
    WithoutLabels,
    RadioToggleButtons,
    OutlinedStyles
};
