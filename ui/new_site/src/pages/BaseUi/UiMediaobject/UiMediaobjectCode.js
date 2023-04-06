import PrismCode from "../../../Components/Common/Prism";

// Examples

const defultCode =
    `
<div className="d-flex align-items-start text-muted mb-4">
    <div className="flex-shrink-0 me-3">
        <img src={avatar2} className="avatar-sm rounded" alt="...">
    </div>
    
    <div className="flex-grow-1">
        <h5 className="fs-14">Media heading</h5>
        This is some content from a media component. You can replace this with any content and adjust it as needed.
    </div>
</div>

<div className="d-flex align-items-start text-muted mb-4">
    <div className="flex-grow-1">
        <h5 className="fs-14">Media heading</h5>
        This is some content from a media component. You can replace this with any content and adjust it as needed.
    </div>
    <div className="flex-shrink-0 ms-3">
        <img src={avatar3} className="avatar-sm rounded" alt="...">
    </div>
</div>

<div className="d-flex align-items-start text-muted">
    <div className="flex-shrink-0 me-3">
        <img src={avatar2} className="avatar-sm rounded" alt="...">
    </div>
    <div className="flex-grow-1">
        <h5 className="fs-14">Media heading</h5>
        This is some content from a media component. You can replace this with any content and adjust it as needed.
    </div>
</div>
`;

const DefultExample = () => (
    <PrismCode
        code={defultCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Nesting Example

const nestingCode =
    `
<!-- Nesting Example -->
<div className="d-flex align-items-start text-muted mb-4">
    <div className="flex-shrink-0 me-3">
        <img src={avatar2} className="avatar-sm rounded" alt="...">
    </div>
    <div className="flex-grow-1">
        <h5 className="fs-14">Media heading</h5>
        This is some content from a media component. You can replace this with any content and adjust it as needed.
        <div className="d-flex align-items-start text-muted mt-3">
            <div className="flex-shrink-0 me-3">
                <img src={avatar3} className="avatar-sm rounded" alt="...">
            </div>
            <div className="flex-grow-1">
                <h5 className="fs-14">Media heading</h5>
                This is some content from a media component. You can replace this with any content and adjust it as needed.
            </div>
        </div>
    </div>
</div>

<div className="d-flex align-items-start text-muted">
    <div className="flex-shrink-0 me-3">
        <img src={avatar4} className="avatar-sm rounded" alt="...">
    </div>
    <div className="flex-grow-1">
        <h5 className="fs-14">Media heading</h5>
        This is some content from a media component. You can replace this with any content and adjust it as needed.
    </div>
</div>

`;

const NestingExample = () => (
    <PrismCode
        code={nestingCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Media Alignment

const mediaCode =
    `
<!-- Media Alignment -->
<div className="d-flex align-items-start text-muted mb-4">
    <div className="flex-shrink-0 me-3">
    <img src={avatar2} className="avatar-sm rounded" alt="...">
    </div>
    <div className="flex-grow-1">
    <h5 className="fs-14">Top Aligned media</h5>
    <p className="mb-1">Cras sit amet nibh libero, in gravida nulla. Nulla vel metus scelerisque ante sollicitudin. Cras purus odio, vestibulum in vulputate at, tempus viverra turpis. Fusce condimentum nunc ac nisi vulputate fringilla. Donec lacinia congue felis in faucibus.</p>
    <p className="mb-0">Donec sed odio dui. Nullam quis risus eget urna mollis ornare vel eu leo. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.</p>
    </div>
</div>

<div className="d-flex align-items-center text-muted mb-4">
    <div className="flex-shrink-0 me-3">
    <img src={avatar6} className="avatar-sm rounded" alt="...">
    </div>
    <div className="flex-grow-1">
    <h5 className="fs-14">Center Aligned media</h5>
    <p className="mb-1">Cras sit amet nibh libero, in gravida nulla. Nulla vel metus scelerisque ante sollicitudin. Cras purus odio, vestibulum in vulputate at, tempus viverra turpis. Fusce condimentum nunc ac nisi vulputate fringilla. Donec lacinia congue felis in faucibus.</p>
    <p className="mb-0">Donec sed odio dui. Nullam quis risus eget urna mollis ornare vel eu leo. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.</p>
    </div>
</div>

<div className="d-flex align-items-end text-muted">
    <div className="flex-shrink-0 me-3">
    <img src={avatar8} className="avatar-sm rounded" alt="...">
    </div>
    <div className="flex-grow-1">
    <h5 className="fs-14">Bottom Aligned media</h5>
    <p className="mb-1">Cras sit amet nibh libero, in gravida nulla. Nulla vel metus scelerisque ante sollicitudin. Cras purus odio, vestibulum in vulputate at, tempus viverra turpis. Fusce condimentum nunc ac nisi vulputate fringilla. Donec lacinia congue felis in faucibus.</p>
    <p className="mb-0">Donec sed odio dui. Nullam quis risus eget urna mollis ornare vel eu leo. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.</p>
    </div>
</div>
`;

const MediaExample = () => (
    <PrismCode
        code={mediaCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { DefultExample, NestingExample, MediaExample };
