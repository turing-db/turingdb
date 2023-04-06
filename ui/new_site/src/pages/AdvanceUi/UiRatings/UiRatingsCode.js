import PrismCode from "../../../Components/Common/Prism";

// Basic Rater	

const basicRaterCode =
    `
<Rating
initialRating={3}
emptySymbol="mdi mdi-star-outline text-muted "
fullSymbol="mdi mdi-star text-warning "
/>
`;

const BasicRaterExample = () => (
    <PrismCode
        code={basicRaterCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Rater with Step
const raterWithStepCode =
    `
<Rating
initialRating={1.5}
fractions={2}
emptySymbol="mdi mdi-star-outline text-muted "
fullSymbol="mdi mdi-star text-warning "
/>
`;

const RaterWithStepExample = () => (
    <PrismCode
        code={raterWithStepCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Custom Messages
const customMsgCode =
    `
<Rating
emptySymbol="mdi mdi-star-outline text-muted "
fullSymbol="mdi mdi-star text-warning "
/>
`;

const CustomMsgExample = () => (
    <PrismCode
        code={customMsgCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Example with unlimited number of stars. readOnly option is set to true.
const readOnlyRaterCode =
    `
<Rating
stop={16}
emptySymbol="mdi mdi-star-outline text-muted fa-1x"
fullSymbol="mdi mdi-star text-warning"
initialRating={4.5}
readonly
/>
`;

const ReadOnlyRaterExample = () => (
    <PrismCode
        code={readOnlyRaterCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// On Hover Event
const onHoverCode =
    `
const [customize, setcustomize] = useState("");

<Rating
stop={5}
emptySymbol="mdi mdi-star-outline text-muted "
fullSymbol="mdi mdi-star text-warning "
onChange={(customize) => setcustomize(customize)}
/>
`;

const OnHoverExample = () => (
    <PrismCode
        code={onHoverCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);


// Clear/Reset Rater	
const reasetRaterCode =
    `
const [reset, setreset] = useState("");

<Rating
emptySymbol="mdi mdi-star-outline text-muted"
fullSymbol={reset}
onHover={() => setreset("mdi mdi-star text-warning")}
/>
`;

const ReasetRaterExample = () => (
    <PrismCode
        code={reasetRaterCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { BasicRaterExample, RaterWithStepExample, CustomMsgExample, ReadOnlyRaterExample, OnHoverExample, ReasetRaterExample };