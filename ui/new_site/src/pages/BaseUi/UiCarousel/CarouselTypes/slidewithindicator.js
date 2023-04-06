import React from "react";
import { UncontrolledCarousel } from "reactstrap";

// Carousel images
import img1 from "../../../../assets/images/small/img-1.jpg";
import img2 from "../../../../assets/images/small/img-2.jpg";
import img3 from "../../../../assets/images/small/img-3.jpg";


const Slidewithindicator = () => {
  return (
    <React.Fragment>
      <UncontrolledCarousel
      interval={4000}
        items={[
          {
            altText: " ",
            caption: " ",
            key: 1,
            src: img3,
          },
          {
            altText: " ",
            caption: " ",
            key: 2,
            src: img2,
          },
          {
            altText: " ",
            caption: " ",
            key: 3,
            src: img1,
          },
        ]}
      />
    </React.Fragment>
  );
};

export default Slidewithindicator;
