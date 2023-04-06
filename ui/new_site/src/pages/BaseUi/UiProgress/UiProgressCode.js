import PrismCode from "../../../Components/Common/Prism";

// Default Progress

const defaultProgressCode =
    `
<!-- Base Examples -->
<div className="mb-4">
    <Progress value={0} />
</div>

<div className="mb-4">
    <Progress value={25} />
</div>

<div className="mb-4">
    <Progress value={50} />
</div>

<div className="mb-4">
    <Progress value={75} />
</div>

<div>
    <Progress value={100} />
</div>`;

const DefaultProgressExample = () => (
    <PrismCode
        code={defaultProgressCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Progress with background color

const backgroundColorCode =
    `
<!-- Backgrounds -->
<div className="mb-4">
    <Progress color="primary" value={15} />
</div>

<div className="mb-4">
    <Progress color="success" value={25} />
</div>

<div className="mb-4">
    <Progress color="info" value={50} />
</div>

<div className="mb-4">
    <Progress color="warning" value={75} />
</div>

<div>
    <Progress color="danger" value={100} />
</div>`;

const BackgroundColorExample = () => (
    <PrismCode
        code={backgroundColorCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Progress with Label

const labelCode =
    `
<!-- Labels Example -->
<Progress color="primary" value={25}> 25% </Progress>
`;

const LabelExample = () => (
    <PrismCode
        code={labelCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Multiple Bars
const multipleBarsCode =
    `
<!-- Multiple Bars -->
<Progress multi>
    <Progress bar value="15" />
    <Progress bar color="success" value="30" />
    <Progress bar color="info" value="20" />
</Progress>
`;

const MultipleBarsExample = () => (
    <PrismCode
        code={multipleBarsCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Height
const heightCode =
    `
<!-- Prgress sm -->
<div className="mb-4">
    <h5 className="fs-13">Small Progress</h5>
    <Progress color="primary" value={25} className="progress-sm" />
</div>

<!-- Prgress Default -->
<div className="mb-4">
    <h5 className="fs-13">Default Progress </h5>
    <Progress color="success" value={40} className="progress-md" />
</div>

<!-- Prgress lg -->
<div className="mb-4">
    <h5 className="fs-13">Large Progress</h5>
    <Progress color="warning" value={50} className="progress-lg" />
</div>

<!-- Prgress xl -->
<div>
    <h5 className="fs-13">Extra Large Progress</h5>
    <Progress color="danger" value={70} className="progress-xl" />
</div>
`;

const HeightExample = () => (
    <PrismCode
        code={heightCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Striped Progress
const stripedCode =
    `
<!-- Striped Prgress -->
<div className="mb-4">
    <Progress striped value={25} />
</div>
<div>
    <Progress color="success" striped value={40} />
</div>
`;

const StripedExample = () => (
    <PrismCode
        code={stripedCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Animated Striped Progress
const animatedStripedCode =
    `
<!-- Animated Striped Progress -->
<div>
    <Progress value={75} striped animated />
</div>
`;

const AnimatedStripedExample = () => (
    <PrismCode
        code={animatedStripedCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Gradient Progress
const gradientCode =
    `
<!-- Gradient -->
<div className="mb-4">
    <Progress value={15} className="bg-gradient" />
</div>
<div className="mb-4">
    <Progress color="success" value={25} className="bg-gradient" />
</div>
<div className="mb-4">
    <Progress color="info" value={50} className="bg-gradient" />
</div>
<div className="mb-4">
    <Progress color="warning" value={75} className="bg-gradient" />
</div>
<div>
    <Progress color="danger" value={100} className="bg-gradient" />
</div>
`;

const GradientExample = () => (
    <PrismCode
        code={gradientCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Animated Progress
const animatedCode =
    `
<!-- Animated Progress -->
<div className="mb-4">
    <Progress value={15} className="animated-progess" />
</div>
<div className="mb-4">
    <Progress value={25} color="success" className="animated-progess" />
</div>
<div className="mb-4">
    <Progress value={50} color="info" className="animated-progess" />
</div>
<div className="mb-4">
    <Progress value={75} color="warning" className="animated-progess" />
</div>
<div>
    <Progress value={100} color="danger" className="animated-progess" />
</div>
`;

const AnimatedExample = () => (
    <PrismCode
        code={animatedCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Custom Progress
const customCode =
    `
<!-- Custom Progress -->
<div className="mb-4">
    <Progress value={15} color="primary" className="animated-progess custom-progress" />
</div>
<div className="mb-4">
    <Progress value={25} color="success" className="animated-progess custom-progress" />
</div>
<div className="mb-4">
    <Progress value={50} color="info" className="animated-progess custom-progress" />
</div>
<div className="mb-4">
    <Progress value={75} color="warning" className="animated-progess custom-progress" />
</div>
<div>
    <Progress value={100} color="danger" className="animated-progess custom-progress" />
</div>
`;

const CustomExample = () => (
    <PrismCode
        code={customCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Custom Progress with Label
const customProgressCode =
    `
<!-- Custom Progress with Label -->
<div className="d-flex align-items-center pb-2 mt-4">
    <div className="flex-shrink-0 me-3">
        <div className="avatar-xs">
            <div className="avatar-title bg-light rounded-circle text-muted fs-16">
                <i className="mdi mdi-facebook"></i>
            </div>
        </div>
    </div>
    <div className="flex-grow-1">
        <div>
            <Progress value={15} color="primary" className="animated-progess custom-progress progress-label" ><div className="label">15%</div> </Progress>
        </div>
    </div>
</div>

<div className="d-flex align-items-center py-2">
    <div className="flex-shrink-0 me-3">
        <div className="avatar-xs">
            <div className="avatar-title bg-light rounded-circle text-muted fs-16">
                <i className="mdi mdi-twitter"></i>
            </div>
        </div>
    </div>
    <div className="flex-grow-1">
        <div>
            <Progress value={25} color="success" className="animated-progess custom-progress progress-label" ><div className="label">25%</div> </Progress>
        </div>
    </div>
</div>

<div className="d-flex align-items-center py-2">
    <div className="flex-shrink-0 me-3">
        <div className="avatar-xs">
            <div className="avatar-title bg-light rounded-circle text-muted fs-16">
                <i className="mdi mdi-github"></i>
            </div>
        </div>
    </div>
    <div className="flex-grow-1">
        <div>
            <Progress value={50} color="info" className="animated-progess custom-progress progress-label" ><div className="label">30%</div> </Progress>
        </div>
    </div>
</div>
`;

const CustomProgressExample = () => (
    <PrismCode
        code={customProgressCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Content Progress
const contentCode =
    `
<!-- Content Progress -->
<Card className="bg-light overflow-hidden shadow-none">
    <CardBody>
        <div className="d-flex">
            <div className="flex-grow-1">
                <h6 className="mb-0"><b className="text-secondary">30%</b> Update in
                    progress...</h6>
            </div>
            <div className="flex-shrink-0">
                <h6 className="mb-0">1 min left</h6>
            </div>
        </div>
    </CardBody>
    <div >
        <Progress value={30} color="info" className="bg-soft-info rounded-0" />
    </div>
</Card>

<Card className="bg-light overflow-hidden shadow-none">
    <CardBody>
        <div className="d-flex">
            <div className="flex-grow-1">
                <h6 className="mb-0"><b className="text-success">60%</b> Update in
                    progress...</h6>
            </div>
            <div className="flex-shrink-0">
                <h6 className="mb-0">45s left</h6>
            </div>
        </div>
    </CardBody>
    <div>
        <Progress value={60} color="success" className="bg-soft-success rounded-0" />
    </div>
</Card>

<Card className="bg-light overflow-hidden shadow-none">
    <CardBody>
        <div className="d-flex">
            <div className="flex-grow-1">
                <h6 className="mb-0"><b className="text-danger">82%</b> Update in
                    progress...</h6>
            </div>
            <div className="flex-shrink-0">
                <h6 className="mb-0">25s left</h6>
            </div>
        </div>
    </CardBody>
    <div>
        <Progress value={82} color="danger" className="bg-soft-danger rounded-0" />
    </div>
</Card>
`;



const ContentExample = () => (
    <PrismCode
        code={contentCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Progress with Steps
const progressWithStepCode =
    `
<!-- Progress with Steps -->
<div className="position-relative m-4">
    <Progress value={50} style={{ height: "1px" }} />
    <Button size="sm" color="primary" className="position-absolute top-0 start-0 translate-middle rounded-pill" style={{ width: "2rem", height: "2rem" }}>1</Button>
    <Button size="sm" color="primary" className="position-absolute top-0 start-50 translate-middle rounded-pill" style={{ width: "2rem", height: "2rem" }}>2</Button>
    <Button size="sm" color="light" className="position-absolute top-0 start-100 translate-middle rounded-pill" style={{ width: "2rem", height: "2rem" }}>3</Button>
</div>
`;

const ProgressWithStepExample = () => (
    <PrismCode
        code={progressWithStepCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Step Progress with Arrow
const stepProgressArrowCode =
    `
<!-- Step Progress with Arrow -->
<Progress multi className='progress-step-arrow progress-info'>
    <Progress bar value="35"> Step 1 </Progress>
    <Progress bar value="35"> Step 2 </Progress>
    <Progress bar value="35" color="light" className="text-dark"> Step 3 </Progress>
</Progress>
`;

const StepProgressArrowExample = () => (
    <PrismCode
        code={stepProgressArrowCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { DefaultProgressExample, BackgroundColorExample, LabelExample, MultipleBarsExample, HeightExample, StripedExample, AnimatedStripedExample, GradientExample, AnimatedExample, CustomExample, CustomProgressExample, ContentExample, ProgressWithStepExample, StepProgressArrowExample };