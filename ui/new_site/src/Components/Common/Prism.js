import React, { useEffect, useRef } from "react";
import Prism from "prismjs";

const PrismCode = (props) => {
    const ref = useRef();

    useEffect(() => {
        highlight();
    }, []);

    const highlight = () => {
        if (ref && ref.current) {
            Prism.highlightElement(ref.current);
        }
    };

    const { code, language } = props;
    return (
        <React.Fragment>
            <pre className="line-numbers">
                <code ref={ref} className={`language-${language}`}>
                    {code.trim()}
                </code>
            </pre>
        </React.Fragment>
    );
};

export default PrismCode;

