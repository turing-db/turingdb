import PrismCode from "../../../Components/Common/Prism";

// Bootstrap Toasts

const bootstrapToastsCode =
`<!-- Default Toast --> 
const [toast1, setToast1] = useState(true);

<Toast isOpen={toast1}>
    <ToastHeader toggle={() => setToast1(!toast1)}>
        <img src={logosm} className="rounded me-2" alt="..." height="20" />
        <span className="fw-semibold me-auto">Velzon</span>
        <small style={{marginLeft:"140px"}}>06 mins ago</small>
    </ToastHeader>
    <ToastBody>
        Hello, world! This is a toast message.
    </ToastBody>
</Toast>

<!-- Translucent Toast --> 
const [toast2, setToast2] = useState(true);

<Toast isOpen={toast2}>
    <ToastHeader toggle={() => setToast2(!toast2)}>
        <img src={logosm} className="rounded me-2" alt="..." height="20" />
    <span className="fw-semibold me-auto">Velzon</span>
    <small style={{marginLeft:"150px"}}>11 mins ago</small>
    </ToastHeader>
    <ToastBody>
        Hello, world! This is a toast message.
    </ToastBody>
</Toast>

<!-- Stacking Toast --> 
const [toast3, setToast3] = useState(true);

<Toast isOpen={toast3}>
    <ToastHeader toggle={() => setToast3(!toast3)}>
        <img src={logosm} className="rounded me-2" alt="..." height="20" />
        <span className="fw-semibold me-auto">Velzon</span>
        <small style={{marginLeft:"165px"}}>Just now</small>
    </ToastHeader>
    <ToastBody>
        See? Just like this.
    </ToastBody>
</Toast>

<!-- Placement Toast --> 
const [toast5, setToast5] = useState(true);

<div className="bg-light" style={{ position: "relative", minHeight: "200px" }}>
    <Toast isOpen={toast5} style={{ position: "absolute", top: "16px", right: "16px" }}>
        <ToastHeader toggle={() => setToast5(!toast5)}>
            <img src={logosm} className="rounded me-2" alt="..." height="20" />
            <span className="fw-semibold me-auto">Velzon</span>
            <small style={{marginLeft:"140px"}}>06 mins ago</small>
        </ToastHeader>
        <ToastBody>
            Hello, world! This is a toast message.
        </ToastBody>
    </Toast>
</div>

 <!-- Flexbox container for aligning the toasts -->
const [toast6, setToast6] = useState(true);

<div className="bg-light d-flex justify-content-center align-items-center" style={{ height: "200px" }}>
    <Toast isOpen={toast6}>
        <ToastHeader toggle={() => setToast6(!toast6)}>
             <img src={logosm} className="rounded me-2" alt="..." height="20" />
             <span className="fw-semibold me-auto">Velzon</span>
             <small style={{marginLeft:"150px"}}>11 mins ago</small>
        </ToastHeader>
        <ToastBody>
             Hello, world! This is a toast message.
        </ToastBody>
    </Toast>
</div>
`;

const BootstrapToastsExample = () => (
    <PrismCode
        code={bootstrapToastsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Toast Placement
const toastPlacementCode =
    `
<!-- Toast Placement -->

const [toast11, setToast11] = useState(true);
const [position, setPosition] = useState();

<Form>
    <div className="mb-3">
        <select className="form-select mt-2" onChange={(e) => setPosition(e.currentTarget.value)}>
            <option value="" defaultValue>Select a position...</option>
            <option value="top-0 start-0">Top left</option>
            <option value="top-0 start-50 translate-middle-x">Top center</option>
            <option value="top-0 end-0">Top right</option>
            <option value="top-50 start-0 translate-middle-y">Middle left</option>
            <option value="top-50 start-50 translate-middle">Middle center</option>
            <option value="top-50 end-0 translate-middle-y">Middle right</option>
            <option value="bottom-0 start-0">Bottom left</option>
            <option value="bottom-0 start-50 translate-middle-x">Bottom center</option>
            <option value="bottom-0 end-0">Bottom right</option>
        </select>
    </div>
</Form>
<div aria-live="polite" aria-atomic="true" className="bd-example bg-light position-relative" style={{ minHeight: "300px" }}>
    <div className={"toast-container position-absolute p-3 " + position} id="toastPlacement">
        <Toast isOpen={toast11}>
            <ToastHeader toggle={() => setToast11(!toast11)}>
                <img src={logosm} className="rounded me-2"
                    alt="..." height="20" />
                <strong className="me-auto">Velzon</strong>
                <small style={{ marginLeft: "150px" }}>11 mins ago</small>
            </ToastHeader>
            <ToastBody>
                Hello, world! This is a toast message.
            </ToastBody>
        </Toast>
    </div>
</div>
`;

const ToastPlacementExample = () => (
    <PrismCode
        code={toastPlacementCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Bordered with Icon Toast
const borderIconCode =
    `
<!-- Bordered With Icon Toast -->
<div className="hstack flex-wrap gap-2">
    <Button className="btn btn-primary" id="borderedToast1Btn" onClick={() => setToast7(!toast7)} >Primary Toast</Button>
    <Button className="btn btn-success" id="borderedToast2Btn" onClick={() => setToast8(!toast8)} >Success Toast</Button>
    <Button className="btn btn-warning" id="borderedTost3Btn" onClick={() => setToast9(!toast9)} >Warning Toast</Button>
    <Button className="btn btn-danger" id="borderedToast4Btn" onClick={() => setToast10(!toast10)} >Danger Toast</Button>
</div>
    
<div style={{ zIndex: "11" }}>
    <Toast isOpen={toast7} id="borderedToast1" className="toast-border-primary overflow-hidden mt-3">
        <ToastBody>
            <div className="d-flex align-items-center">
                <div className="flex-shrink-0 me-2">
                    <i className="ri-user-smile-line align-middle"></i>
                </div>
                <div className="flex-grow-1">
                    <h6 className="mb-0">Your application was successfully sent.</h6>
                </div>
            </div>
        </ToastBody>
    </Toast>
</div>
    
<div style={{ zIndex: "11" }}>
    <Toast isOpen={toast8} id="borderedToast2" className="toast-border-success overflow-hidden mt-3">
        <ToastBody>
            <div className="d-flex align-items-center">
                <div className="flex-shrink-0 me-2">
                        <i className="ri-checkbox-circle-fill align-middle"></i>
                </div>
                <div className="flex-grow-1">
                        <h6 className="mb-0">Yey! Everything worked!</h6>
                </div>
            </div>
        </ToastBody>
    </Toast>
</div>
    
<div style={{ zIndex: "11" }}>
    <Toast isOpen={toast9} id="borderedToast3" className="toast-border-warning overflow-hidden mt-3">
        <ToastBody>
            <div className="d-flex align-items-center">
                <div className="flex-shrink-0 me-2">
                    <i className="ri-notification-off-line align-middle"></i>
                </div>
                <div className="flex-grow-1">
                    <h6 className="mb-0">Something went wrong, try again.</h6>
                </div>
            </div>
        </ToastBody>
    </Toast>
</div>
    
<div style={{ zIndex: "11" }}>
    <Toast isOpen={toast10} id="borderedToast4" className="toast-border-danger overflow-hidden mt-3">
        <ToastBody>
            <div className="d-flex align-items-center">
                <div className="flex-shrink-0 me-2">
                    <i className="ri-alert-line align-middle"></i>
                </div>
                <div className="flex-grow-1">
                    <h6 className="mb-0">Something is very wrong!  <Link to="#" className="text-decoration-underline">See detailed report.</Link></h6>
                </div>
            </div>
        </ToastBody>
    </Toast>
</div>
`;

const BorderIconExample = () => (
    <PrismCode
        code={borderIconCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Toastify JS
const toastifyCode =
`<!-- Toast -->
const defaultnotify = () => toast("Welcome Back! This is a Toast Notification", { position: "top-right", hideProgressBar: true, className: 'bg-primary text-white' });
const successnotify = () => toast("Your application was successfully sent", { position: "top-center", hideProgressBar: true, closeOnClick: false, className: 'bg-success text-white' });
const warningnotify = () => toast("Warning ! Something went wrong try again", { position: "top-center", hideProgressBar: true, closeOnClick: false, className: 'bg-warning text-white' });
const errornotify = () => toast("Error ! An error occurred.", { position: "top-center", hideProgressBar: true, closeOnClick: false, className: 'bg-danger text-white' });

<Button color="light" className="w-xs" onClick={defaultnotify}>Default</Button>
<Button color="light" className="w-xs" onClick={successnotify}>Success</Button>
<Button color="light" className="w-xs" onClick={warningnotify}>Warning</Button>
<Button color="light" className="w-xs" onClick={errornotify}>Error</Button>
<ToastContainer />

<!-- Display Position -->
const topleftnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "top-left", hideProgressBar: true, className: 'bg-success text-white' });
const topcenternotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "top-center", hideProgressBar: true, className: 'bg-success text-white' });
const toprightnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "top-right", hideProgressBar: true, className: 'bg-success text-white' });
const bottomleftnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "bottom-left", hideProgressBar: true, className: 'bg-success text-white' });
const bottomcenternotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "bottom-center", hideProgressBar: true, className: 'bg-success text-white' });
const bottomrightnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "bottom-right", hideProgressBar: true, className: 'bg-success text-white' });

<Button color="light" className="w-xs" onClick={topleftnotify}>Top Left</Button>
<Button color="light" className="w-xs" onClick={topcenternotify}>Top Center</Button>
<Button color="light" className="w-xs" onClick={toprightnotify}>Top Right</Button>
<Button color="light" className="w-xs" onClick={bottomleftnotify}>Bottom Left</Button>
<Button color="light" className="w-xs" onClick={bottomcenternotify}>Bottom Center</Button>
<Button color="light" className="w-xs" onClick={bottomrightnotify}>Bottom Right</Button>

<!-- Offset Position -->
const offsetnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "top-right", hideProgressBar: true, className: 'bg-success text-white m-5' });
<Button color="light" className="w-xs" onClick={offsetnotify} >Click Me</Button>

<!-- Close icon Display -->
const closeiconnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "top-right", hideProgressBar: true, className: 'bg-success text-white' });
<Button color="light" className="w-xs" onClick={closeiconnotify} >Click Me</Button>

<!-- Duration -->
const durationnotify = () => toast("Toast Duration 5s", { position: "top-right", hideProgressBar: false, className: 'bg-success text-white' });
<Button color="light" className="w-xs" onClick={durationnotify} >Click Me</Button>
`;

const ToastifyExample = () => (
    <PrismCode
        code={toastifyCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { BootstrapToastsExample, ToastPlacementExample, BorderIconExample, ToastifyExample };