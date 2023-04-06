import React from "react";
import { UncontrolledCarousel } from "reactstrap";

// Carousel images
import img1 from "../../../../assets/images/small/img-4.jpg";
import img2 from "../../../../assets/images/small/img-5.jpg";
import img3 from "../../../../assets/images/small/img-6.jpg";

const SlideDark = () => {
  return (
    <React.Fragment>
      <UncontrolledCarousel
        dark={true}
        interval={false}
        items={[
          {
            altText: " ",
            caption: "Drawing a sketch",
            key: 1,
            src: img1,
          },
          {
            altText: " ",
            caption: "Blue clock on a pastel background",
            key: 2,
            src: img2,
          },
          {
            altText: " ",
            caption: "Working at a coffee shop",
            key: 3,
            src: img3,
          },
        ]}
      />
    </React.Fragment>
  );
};

export default SlideDark;
