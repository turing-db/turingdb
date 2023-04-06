import PropTypes from 'prop-types';
import React from "react";
import { VectorMap } from "react-jvectormap";
import "../DashboardAnalytics/jquery-jvectormap.scss";

const map = React.createRef(null);
const Vectormap = props => {
    return (
        <div style={{ width: props.width, height: 350 }}>
            <VectorMap
                map={props.value}
                zoomOnScroll={false}
                zoomButtons={false}
                markersSelectable={true}
                markerStyle={{
                    initial: {
                        fill: "#6ada7d",
                    },
                    selected: {
                        fill: "#5ea3cb",
                    },
                }}
                labels={{
                    markers: {
                        render: function (marker) {
                            return marker.name;
                        },
                    },
                }}
                backgroundColor="transparent"
                ref={map}
                containerStyle={{
                    width: "100%",
                    height: "80%",
                }}
                regionStyle={{
                    initial: {
                        stroke: "#9599ad",
                        strokeWidth: 0.25,
                        fill: "#f3f6f9",
                        fillOpacity: 1,
                    },
                }}
            />
        </div>
    );
};

Vectormap.propTypes = {
    color: PropTypes.string,
    value: PropTypes.any,
    width: PropTypes.any
};

export default Vectormap;
