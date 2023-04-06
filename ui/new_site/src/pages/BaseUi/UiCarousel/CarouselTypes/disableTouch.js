import React from "react";
import { UncontrolledCarousel } from "reactstrap";

// Carousel images
import img7 from "../../../../assets/images/small/img-7.jpg";
import img8 from "../../../../assets/images/small/img-8.jpg";
import img9 from "../../../../assets/images/small/img-9.jpg";

const DisableTouch = () => {
    return (
        <React.Fragment>
            <UncontrolledCarousel
                interval={false}
                indicators={false}
                enableTouch={false}
                items={[
                    {
                        altText: " ",
                        caption: " ",
                        key: 1,
                        src: img9,
                    },
                    {
                        altText: " ",
                        caption: " ",
                        key: 2,
                        src: img8,
                    },
                    {
                        altText: " ",
                        caption: " ",
                        key: 3,
                        src: img7,
                    },
                ]}
            />
        </React.Fragment>
    );
};

export default DisableTouch;
