import React from "react";
import { UncontrolledCarousel } from "reactstrap";

// Carousel images
import img1 from "../../../../assets/images/small/img-4.jpg";
import img2 from "../../../../assets/images/small/img-5.jpg";
import img3 from "../../../../assets/images/small/img-6.jpg";

const Slidewithcontrol = () => {
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
            src: img1,
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
            src: img3,
          },
        ]}
      />
    </React.Fragment>
  );
};

export default Slidewithcontrol;
