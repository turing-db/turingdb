import React, { Component } from "react";

import { Row, Col, Card, CardBody, CardTitle } from "reactstrap";
import DualListBox from "react-dual-listbox";
import "react-dual-listbox/lib/react-dual-listbox.css";

const options = [
  { value: "luna", label: "Moon" },
  { value: "phobos", label: "Phobos" },
  { value: "deimos", label: "Deimos" },
  { value: "io", label: "Io" },
  { value: "europa", label: "Europa" },
  { value: "ganymede", label: "Ganymede" },
  { value: "callisto", label: "Callisto" },
  { value: "mimas", label: "Mimas" },
  { value: "enceladus", label: "Enceladus" },
  { value: "tethys", label: "Tethys" },
  { value: "rhea", label: "Rhea" },
  { value: "titan", label: "Titan" },
];

const Optgroup = [
  {
    label: "Earth",
    options: [{ value: "luna", label: "Moon" }],
  },
  {
    label: "Mars",
    options: [
      { value: "phobos", label: "Phobos" },
      { value: "deimos", label: "Deimos" },
    ],
  },
  {
    label: "Jupiter",
    options: [
      { value: "io", label: "Io" },
      { value: "europa", label: "Europa" },
      { value: "ganymede", label: "Ganymede" },
      { value: "callisto", label: "Callisto" },
    ],
  },
];

class DualListbox extends Component {
  constructor(props) {
    super(props);
    this.state = {
      selected: ["phobos", "titan", "deimos"],
      selectedOptGroup: ["luna"],
      selectedFilter: ["luna"],
      selectedFilterAlign: ["luna"],
      selectedPreserve: ["luna"],
      selectedRestrict: ["luna"],
      selectedDuplicate: ["luna"],
    };
  }
  onChange = (selected) => {
    this.setState({ selected });
  };

  onOptGroupChange = (selectedOptGroup) => {
    this.setState({ selectedOptGroup });
  };

  onFilterChange = (selectedFilter) => {
    this.setState({ selectedFilter });
  };

  onFilterAlignChange = (selectedFilterAlign) => {
    this.setState({ selectedFilterAlign });
  };

  onPreserveChange = (selectedPreserve) => {
    this.setState({ selectedPreserve });
  };

  onRestrictChange = (selectedRestrict) => {
    this.setState({ selectedRestrict });
  };

  onDuplicateChange = (selectedDuplicate) => {
    this.setState({ selectedDuplicate });
  };

  render() {
    const {
      selected,
      selectedOptGroup,
      selectedFilter,
      selectedFilterAlign,
      selectedPreserve,
      selectedDuplicate,
      selectedRestrict,
    } = this.state;
    const available = ["io", "europa", "ganymede", "callisto"];
    return (
      <React.Fragment>
        <Row>
          <Col md={6}>
            <Card>
              <CardBody>
                <CardTitle className="h4">Basic example</CardTitle>
                <DualListBox
                  options={options}
                  selected={selected}
                  onChange={this.onChange}
                  icons={{
                    moveLeft: <span className="mdi mdi-chevron-left" key="key" />,
                    moveAllLeft: [
                        <span className="mdi mdi-chevron-double-left" key="key" />,
                    ],
                    moveRight: <span className="mdi mdi-chevron-right" key="key" />,
                    moveAllRight: [
                        <span className="mdi mdi-chevron-double-right" key="key" />
                    ],
                    moveDown: <span className="mdi mdi-chevron-down" key="key" />,
                    moveUp: <span className="mdi mdi-chevron-up" key="key" />,
                    moveTop: <span className="mdi mdi-chevron-double-up" key="key" />,
                    moveBottom: <span className="mdi mdi-chevron-double-down" key="key" />,
                }}
                />
              </CardBody>
            </Card>
          </Col>
          <Col md={6}>
            <Card>
              <CardBody>
                <CardTitle className="h4">Optgroup Example</CardTitle>
                <DualListBox
                  options={Optgroup}
                  selected={selectedOptGroup}
                  onChange={this.onOptGroupChange}
                  icons={{
                    moveLeft: <span className="mdi mdi-chevron-left"  key="key" />,
                    moveAllLeft: [
                        <span className="mdi mdi-chevron-double-left"  key="key" />
                    ],
                    moveRight: <span className="mdi mdi-chevron-right"  key="key" />,
                    moveAllRight: [
                        <span className="mdi mdi-chevron-double-right"  key="key" />
                    ],
                    moveDown: <span className="mdi mdi-chevron-down"  key="key" />,
                    moveUp: <span className="mdi mdi-chevron-up"  key="key" />,
                    moveTop: <span className="mdi mdi-chevron-double-up"  key="key" />,
                    moveBottom: <span className="mdi mdi-chevron-double-down"  key="key" />,
                }}
                />
              </CardBody>
            </Card>
          </Col>
        </Row>
        <Row>
          <Col md={6}>
            <Card>
              <CardBody>
                <CardTitle className="h4">Filter Example</CardTitle>
                <DualListBox
                  canFilter
                  filterCallback={(Optgroup, filterInput) => {
                    if (filterInput === "") {
                      return true;
                    }

                    return new RegExp(filterInput, "i").test(Optgroup.label);
                  }}
                  filterPlaceholder="Search..."
                  options={Optgroup}
                  selected={selectedFilter}
                  onChange={this.onFilterChange}
                  icons={{
                    moveLeft: <span className="mdi mdi-chevron-left"  key="key" />,
                    moveAllLeft: [
                        <span className="mdi mdi-chevron-double-left"  key="key" />
                    ],
                    moveRight: <span className="mdi mdi-chevron-right"  key="key" />,
                    moveAllRight: [
                        <span className="mdi mdi-chevron-double-right"  key="key" />
                    ],
                    moveDown: <span className="mdi mdi-chevron-down"  key="key" />,
                    moveUp: <span className="mdi mdi-chevron-up"  key="key" />,
                    moveTop: <span className="mdi mdi-chevron-double-up"  key="key" />,
                    moveBottom: <span className="mdi mdi-chevron-double-down"  key="key" />,
                }}
                />
              </CardBody>
            </Card>
          </Col>
          <Col md={6}>
            <Card>
              <CardBody>
                <CardTitle className="h4">
                  Align Actions to Top Example
                </CardTitle>
                <DualListBox
                  canFilter
                  filterCallback={(Optgroup, filterInput) => {
                    if (filterInput === "") {
                      return true;
                    }

                    return new RegExp(filterInput, "i").test(Optgroup.label);
                  }}
                  filterPlaceholder="Search..."
                  alignActions="top"
                  options={Optgroup}
                  selected={selectedFilterAlign}
                  onChange={this.onFilterAlignChange}
                  icons={{
                    moveLeft: <span className="mdi mdi-chevron-left"  key="key" />,
                    moveAllLeft: [
                        <span className="mdi mdi-chevron-double-left"  key="key" />
                    ],
                    moveRight: <span className="mdi mdi-chevron-right"  key="key" />,
                    moveAllRight: [
                        <span className="mdi mdi-chevron-double-right"  key="key" />
                    ],
                    moveDown: <span className="mdi mdi-chevron-down"  key="key" />,
                    moveUp: <span className="mdi mdi-chevron-up"  key="key" />,
                    moveTop: <span className="mdi mdi-chevron-double-up"  key="key" />,
                    moveBottom: <span className="mdi mdi-chevron-double-down"  key="key" />,
                }}
                />
              </CardBody>
            </Card>
          </Col>
        </Row>
        <Row>
          <Col md={6}>
            <Card>
              <CardBody>
                <CardTitle className="h4">Disabled Example</CardTitle>
                <DualListBox
                  canFilter
                  filterCallback={(Optgroup, filterInput) => {
                    if (filterInput === "") {
                      return true;
                    }

                    return new RegExp(filterInput, "i").test(Optgroup.label);
                  }}
                  filterPlaceholder="Search..."
                  disabled={true}
                  options={Optgroup}
                  selected={selectedFilter}
                  onChange={this.onFilterChange}
                  icons={{
                    moveLeft: <span className="mdi mdi-chevron-left"  key="key" />,
                    moveAllLeft: [
                        <span className="mdi mdi-chevron-double-left"  key="key" />
                    ],
                    moveRight: <span className="mdi mdi-chevron-right"  key="key" />,
                    moveAllRight: [
                        <span className="mdi mdi-chevron-double-right"  key="key" />
                    ],
                    moveDown: <span className="mdi mdi-chevron-down"  key="key" />,
                    moveUp: <span className="mdi mdi-chevron-up"  key="key" />,
                    moveTop: <span className="mdi mdi-chevron-double-up"  key="key" />,
                    moveBottom: <span className="mdi mdi-chevron-double-down"  key="key" />,
                }}
                />
              </CardBody>
            </Card>
          </Col>
          <Col md={6}>
            <Card>
              <CardBody>
                <CardTitle className="h4">
                  Preserve Select Order Example
                </CardTitle>
                <DualListBox
                  canFilter
                  filterCallback={(Optgroup, filterInput) => {
                    if (filterInput === "") {
                      return true;
                    }

                    return new RegExp(filterInput, "i").test(Optgroup.label);
                  }}
                  filterPlaceholder="Search..."
                  options={Optgroup}
                  selected={selectedPreserve}
                  onChange={this.onPreserveChange}
                  preserveSelectOrder
                  showOrderButtons
                  icons={{
                    moveLeft: <span className="mdi mdi-chevron-left"  key="key" />,
                    moveAllLeft: [
                        <span className="mdi mdi-chevron-double-left"  key="key" />
                    ],
                    moveRight: <span className="mdi mdi-chevron-right"  key="key" />,
                    moveAllRight: [
                        <span className="mdi mdi-chevron-double-right"  key="key" />
                    ],
                    moveDown: <span className="mdi mdi-chevron-down"  key="key" />,
                    moveUp: <span className="mdi mdi-chevron-up"  key="key" />,
                    moveTop: <span className="mdi mdi-chevron-double-up"  key="key" />,
                    moveBottom: <span className="mdi mdi-chevron-double-down"  key="key" />,
                }}
                />
              </CardBody>
            </Card>
          </Col>
        </Row>
        <Row>
          <Col md={6}>
            <Card>
              <CardBody>
                <CardTitle className="h4">Allow Duplicates Example</CardTitle>
                <DualListBox
                  canFilter
                  filterCallback={(Optgroup, filterInput) => {
                    if (filterInput === "") {
                      return true;
                    }

                    return new RegExp(filterInput, "i").test(Optgroup.label);
                  }}
                  filterPlaceholder="Search..."
                  allowDuplicates
                  preserveSelectOrder
                  options={Optgroup}
                  selected={selectedDuplicate}
                  onChange={this.onDuplicateChange}
                  icons={{
                    moveLeft: <span className="mdi mdi-chevron-left"  key="key" />,
                    moveAllLeft: [
                        <span className="mdi mdi-chevron-double-left"  key="key" />
                    ],
                    moveRight: <span className="mdi mdi-chevron-right"  key="key" />,
                    moveAllRight: [
                        <span className="mdi mdi-chevron-double-right"  key="key" />
                    ],
                    moveDown: <span className="mdi mdi-chevron-down"  key="key" />,
                    moveUp: <span className="mdi mdi-chevron-up"  key="key" />,
                    moveTop: <span className="mdi mdi-chevron-double-up"  key="key" />,
                    moveBottom: <span className="mdi mdi-chevron-double-down"  key="key" />,
                }}
                />
              </CardBody>
            </Card>
          </Col>
          <Col md={6}>
            <Card>
              <CardBody>
                <CardTitle className="h4">
                  Preserve Select Order Example
                </CardTitle>
                <DualListBox
                  options={Optgroup}
                  selected={selectedRestrict}
                  available={available}
                  onChange={this.onRestrictChange}
                  icons={{
                    moveLeft: <span className="mdi mdi-chevron-left"  key="key" />,
                    moveAllLeft: [
                        <span className="mdi mdi-chevron-double-left"  key="key" />
                    ],
                    moveRight: <span className="mdi mdi-chevron-right"  key="key" />,
                    moveAllRight: [
                        <span className="mdi mdi-chevron-double-right"  key="key" />
                    ],
                    moveDown: <span className="mdi mdi-chevron-down"  key="key" />,
                    moveUp: <span className="mdi mdi-chevron-up"  key="key" />,
                    moveTop: <span className="mdi mdi-chevron-double-up"  key="key" />,
                    moveBottom: <span className="mdi mdi-chevron-double-down"  key="key" />,
                }}
                />
              </CardBody>
            </Card>
          </Col>
        </Row>
      </React.Fragment>
    );
  }
}

export default DualListbox;