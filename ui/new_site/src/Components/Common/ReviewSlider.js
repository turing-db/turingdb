import React from "react";
import { Swiper, SwiperSlide } from "swiper/react";
import { Card, CardBody } from "reactstrap";

import companies1 from "../../assets/images/companies/img-1.png";
import companies8 from "../../assets/images/companies/img-8.png";
import avatar2 from "../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../assets/images/users/avatar-3.jpg";

import "swiper/css";
import "swiper/css/pagination";

// import required modules
import { Autoplay, Mousewheel } from "swiper";

const ReviewSlider = () => {
  return (
    <Swiper
      direction={"vertical"}
      slidesPerView={2}
      spaceBetween={10}
      mousewheel={true}
      loop={true}
      autoplay={{
        delay: 2000,
        disableOnInteraction: false,
      }}
      modules={[Autoplay, Mousewheel]}
      className="mySwiper vertical-swiper"
      style={{ height: "246px" }}
    >
      <SwiperSlide>
        <Card className="border border-dashed shadow-none">
          <CardBody>
            <div className="d-flex">
              <div className="flex-shrink-0 avatar-sm">
                <div className="avatar-title bg-light rounded">
                  <img src={companies1} alt="" height="30" />
                </div>
              </div>
              <div className="flex-grow-1 ms-3">
                <div>
                  <p className="text-muted mb-1 fst-italic text-truncate-two-lines">
                    " Great product and looks great, lots of features. "
                  </p>
                  <div className="fs-11 align-middle text-warning">
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                  </div>
                </div>
                <div className="text-end mb-0 text-muted">
                  - by <cite title="Source Title">Force Medicines</cite>
                </div>
              </div>
            </div>
          </CardBody>
        </Card>
      </SwiperSlide>
      <SwiperSlide>
        <Card className="border border-dashed shadow-none">
          <CardBody>
            <div className="d-flex">
              <div className="flex-shrink-0">
                <img src={avatar3} alt="" className="avatar-sm rounded" />
              </div>
              <div className="flex-grow-1 ms-3">
                <div>
                  <p className="text-muted mb-1 fst-italic text-truncate-two-lines">
                    " Amazing template, very easy to understand and manipulate.
                    "
                  </p>
                  <div className="fs-11 align-middle text-warning">
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-half-fill"></i>
                  </div>
                </div>
                <div className="text-end mb-0 text-muted">
                  - by <cite title="Source Title">Henry Baird</cite>
                </div>
              </div>
            </div>
          </CardBody>
        </Card>
      </SwiperSlide>
      <SwiperSlide>
        <Card className="border border-dashed shadow-none">
          <CardBody>
            <div className="d-flex">
              <div className="flex-shrink-0 avatar-sm">
                <div className="avatar-title bg-light rounded">
                  <img src={companies8} alt="" height="30" />
                </div>
              </div>
              <div className="flex-grow-1 ms-3">
                <div>
                  <p className="text-muted mb-1 fst-italic text-truncate-two-lines">
                    "Very beautiful product and Very helpful customer service."
                  </p>
                  <div className="fs-11 align-middle text-warning">
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-line"></i>
                    <i className="ri-star-line"></i>
                  </div>
                </div>
                <div className="text-end mb-0 text-muted">
                  - by <cite title="Source Title">Zoetic Fashion</cite>
                </div>
              </div>
            </div>
          </CardBody>
        </Card>
      </SwiperSlide>
      <SwiperSlide>
        <Card className="border border-dashed shadow-none">
          <CardBody>
            <div className="d-flex">
              <div className="flex-shrink-0">
                <img src={avatar2} alt="" className="avatar-sm rounded" />
              </div>
              <div className="flex-grow-1 ms-3">
                <div>
                  <p className="text-muted mb-1 fst-italic text-truncate-two-lines">
                    " The product is very beautiful. I like it. "
                  </p>
                  <div className="fs-11 align-middle text-warning">
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-fill"></i>
                    <i className="ri-star-half-fill"></i>
                    <i className="ri-star-line"></i>
                  </div>
                </div>
                <div className="text-end mb-0 text-muted">
                  - by <cite title="Source Title">Nancy Martino</cite>
                </div>
              </div>
            </div>
          </CardBody>
        </Card>
      </SwiperSlide>
    </Swiper>
  );
};

export default ReviewSlider;
