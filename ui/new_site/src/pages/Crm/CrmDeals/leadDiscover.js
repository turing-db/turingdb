import React from "react";
import { Link } from "react-router-dom";
import {
  Card,
  CardBody,
  UncontrolledCollapse,
  CardFooter,
  Button
} from "reactstrap";

const leadDiscover = (props) => {
  return (
    <div className="col">
      <Card>
        <Link
          to="#"
          className={"card-header bg-soft-" + props.deal.bgColor}
          id={"leadDiscovered" + props.index}
        >
          <h5 className="card-title text-uppercase fw-semibold mb-1 fs-15">
            {props.deal.title}
          </h5>
          <p className="text-muted mb-0">
            {props.deal.price}{" "}
            <span className="fw-medium">{props.deal.deals} Deals</span>
          </p>
        </Link>
      </Card>
      <UncontrolledCollapse
        toggler={"#leadDiscovered" + props.index}
        defaultOpen={true}
      >
        {props.deal.subItems.map((subitem, subitemkey) => (
          <React.Fragment key={subitemkey}>
            <Card
              className={
                subitem.isRibbon
                  ? "mb-1 ribbon-box ribbon-fill ribbon-sm"
                  : "mb-1"
              }
            >
              {subitem.isRibbon && (
                <div className="ribbon ribbon-info">
                  <i className="ri-flashlight-fill"></i>
                </div>
              )}
              <CardBody>
                <Link
                  to="#"
                  className="d-flex align-items-center"
                  id={"leadInnerDiscovered" + props.index + subitemkey}
                >
                  <div className="flex-shrink-0">
                    <img
                      src={subitem.img}
                      alt=""
                      className="avatar-xs rounded-circle"
                    />
                  </div>
                  <div className="flex-grow-1 ms-3">
                    <h6 className="fs-15 mb-1">{subitem.title}</h6>
                    <p className="text-muted mb-0">
                      {subitem.price} - {subitem.date}
                    </p>
                  </div>
                </Link>
              </CardBody>
              <UncontrolledCollapse
                className="border-top border-top-dashed"
                toggler={"#leadInnerDiscovered" + props.index + subitemkey}
                defaultOpen={subitem.isOpen === true ? true : false}
              >
                <CardBody>
                  <h6 className="fs-15 mb-1">
                    {subitem.subTitle}{" "}
                    <small
                      className={
                        "badge badge-soft-" + subitem.timeDurationClass
                      }
                    >
                      {subitem.timeDuration}
                    </small>
                  </h6>
                  <p className="text-muted">
                    As a company grows however, you find it's not as easy to
                    shout across
                  </p>
                  <ul className="list-unstyled vstack gap-2 mb-0">
                    <li>
                      <div className="d-flex">
                        <div className="flex-shrink-0 avatar-xxs text-muted">
                          <i className="ri-question-answer-line"></i>
                        </div>
                        <div className="flex-grow-1">
                          <h6 className="mb-0">Meeting with Thomas</h6>
                          <small className="text-muted fs-12">
                            Yesterday at 9:12AM
                          </small>
                        </div>
                      </div>
                    </li>
                    <li>
                      <div className="d-flex">
                        <div className="flex-shrink-0 avatar-xxs text-muted">
                          <i className="ri-mac-line"></i>
                        </div>
                        <div className="flex-grow-1">
                          <h6 className="mb-0">Product Demo</h6>
                          <small className="text-muted">
                            Monday at 04:41PM
                          </small>
                        </div>
                      </div>
                    </li>
                    <li>
                      <div className="d-flex">
                        <div className="flex-shrink-0 avatar-xxs text-muted">
                          <i className="ri-earth-line"></i>
                        </div>
                        <div className="flex-grow-1">
                          <h6 className="mb-0">Marketing Team Meeting</h6>
                          <small className="text-muted">
                            Monday at 04:41PM
                          </small>
                        </div>
                      </div>
                    </li>
                  </ul>
                </CardBody>
                <CardFooter className="hstack gap-2">
                  <Button color="warning" className="btn-sm w-100">
                    <i className="ri-phone-line align-bottom me-1"></i> Call
                  </Button>
                  <Button color="info" className="btn-sm w-100">
                    <i className="ri-question-answer-line align-bottom me-1"></i>{" "}
                    Message
                  </Button>
                </CardFooter>
              </UncontrolledCollapse>
            </Card>
          </React.Fragment>
        ))}
      </UncontrolledCollapse>
    </div>
  );
};

export default leadDiscover;
