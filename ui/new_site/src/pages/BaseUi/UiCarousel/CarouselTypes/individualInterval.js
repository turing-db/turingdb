import React from "react";
import { UncontrolledCarousel } from "reactstrap";

// Carousel images
import img10 from "../../../../assets/images/small/img-10.jpg";
import img11 from "../../../../assets/images/small/img-11.jpg";
import img12 from "../../../../assets/images/small/img-12.jpg";

const IndividualInterval = () => {
  return (
    <React.Fragment>
      <UncontrolledCarousel
        interval={4000}
        indicators={false}
        items={[
          {
            altText: " ",
            caption: " ",
            key: 1,
            src: img12,
          },
          {
            altText: " ",
            caption: " ",
            key: 2,
            src: img11,
          },
          {
            altText: " ",
            caption: " ",
            key: 3,
            src: img10,
          },
        ]}
      />
    </React.Fragment>
  );
};

export default IndividualInterval;
