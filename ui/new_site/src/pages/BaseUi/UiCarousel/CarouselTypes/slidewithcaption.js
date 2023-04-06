import React from "react";
import { UncontrolledCarousel } from "reactstrap";

// Carousel images
import img7 from "../../../../assets/images/small/img-7.jpg";
import img2 from "../../../../assets/images/small/img-2.jpg";
import img9 from "../../../../assets/images/small/img-9.jpg";

const Slidewithcaption = () => {
  return (
    <React.Fragment>
      <UncontrolledCarousel
        interval={4000}
        items={[
          {
            altText: "First slide label ",
            caption: "First slide label",
            key: 1,
            src: img7,
          },
          {
            altText: "Second slide label",
            caption: "Second slide label",
            key: 2,
            src: img2,
          },
          {
            altText: "Third slide label",
            caption: "Third slide label",
            key: 3,
            src: img9,
          },
        ]}
      />
    </React.Fragment>
  );
};

export default Slidewithcaption;
