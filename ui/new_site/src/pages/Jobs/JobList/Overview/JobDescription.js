import React from "react";
import { Link, NavLink } from "react-router-dom";
import { Card, CardBody } from "reactstrap";

const JobDescription = () => {
  return (
    <React.Fragment>
      <Card>
        <CardBody>
          <h5 className="mb-3">Job Description</h5>

          <p className="text-muted mb-2">
            A Product Designer of a company is responsible for the user
            experience of a product, which usually includes taking direction on
            the business goals and objectives from product management. Product
            Designers are typically associated with the visual or tactile
            aspects of a product but sometimes they can also play a role in the
            information architecture and system design of a product.
          </p>
          <p className="text-muted mb-4">
            Product designer is an exceptional career choice. The demand for
            product designers is increasing with each passing day but there is a
            huge shortage for a skilful product designer in the market. With
            hard work and an apt skill set, a product designer can make a
            handsome amount of money.
          </p>
          <div>
            <h5 className="mb-3">Responsibilities of Product Designer</h5>
            <p className="text-muted">
              Provided below are the responsibilities of a Product Designer:
            </p>
            <ul className="text-muted vstack gap-2">
              <li>
                To visualise and create drawings and design concepts to
                determine the best product.
              </li>
              <li>
                To present ideas and concepts to relevant team members for
                brainstorming.
              </li>
              <li>To employ design concepts into functional prototypes.</li>
              <li>
                To analyse, modify and revise existing designs or products and
                meet customer expectations.
              </li>
              <li>
                To coordinate with team members and to ensure accurate
                efficiency in the design phase.
              </li>
              <li>Excellent attention to detail</li>
              <li>Meticulous and diligent</li>
            </ul>
          </div>

          <div>
            <h5 className="mb-3">Skill & Experience</h5>
            <ul className="text-muted vstack gap-2">
              <li>Communication skills</li>
              <li>Sound visualisation abilities</li>
              <li>Observation skills</li>
              <li>Problem-solving abilities</li>
              <li>
                Eye for aesthetic design and understanding of customer appeal
              </li>
              <li>Artistic & innovative flair</li>
              <li>Strong knowledge of the industry & market trends</li>
              <li>
                Relevant work experience in a particular field is mandatory
              </li>
            </ul>
          </div>

          <ul className="list-inline mb-0">
            <li className="list-inline-item">
              <h5 className="mb-0">Share this job:</h5>
            </li>
            <li className="list-inline-item">
              <Link to="#!" className="btn btn-icon btn-soft-info">
                <i className="ri-facebook-line"></i>
              </Link>
            </li>
            <li className="list-inline-item">
              <Link to="#!" className="btn btn-icon btn-soft-success">
                <i className="ri-whatsapp-line"></i>
              </Link>
            </li>
            <li className="list-inline-item">
              <Link to="#!" className="btn btn-icon btn-soft-secondary">
                <i className="ri-twitter-line"></i>
              </Link>
            </li>
            <li className="list-inline-item">
              <Link to="#!" className="btn btn-icon btn-soft-danger">
                <i className="ri-mail-line"></i>
              </Link>
            </li>
          </ul>
        </CardBody>
      </Card>
    </React.Fragment>
  );
};

export default JobDescription;
