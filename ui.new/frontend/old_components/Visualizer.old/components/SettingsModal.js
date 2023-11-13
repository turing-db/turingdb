import React from 'react';
import { Modal, Box, Typography, Slider, Tooltip, FormControlLabel, Checkbox, FormGroup } from "@mui/material";
import { VisualizerContext } from "../context";

const FilterCheckbox = (props) => {
    const visualizer = React.useContext(VisualizerContext);

    return (
        <Tooltip title={props.tooltip}>
            <FormControlLabel
                control={
                    <Checkbox
                        checked={props.propValue}
                        onChange={e =>
                            visualizer.state.filters.setFilters({ [props.propName]: e.target.checked })}
                    />}
                label={props.label}
            />
        </Tooltip>
    );
};

const SettingsModal = (props) => {
    const visualizer = React.useContext(VisualizerContext);
    const onClose = () => props.setOpen(false);
    const style = {
        position: 'absolute',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
        width: 400,
        bgcolor: 'background.paper',
        border: '2px solid #000',
        boxShadow: 24,
        p: 4,
    };

    return (
        <Modal open={props.open} onClose={onClose}>
            <Box sx={style}>
                <Typography gutterBottom>Edge length</Typography>
                <Slider
                    valueLabelDisplay="auto"
                    aria-label="Edge length"
                    value={visualizer.state.layouts.cyLayout.edgeLengthVal}
                    min={5}
                    max={800}
                    onChange={(_e, v) => visualizer.state.layouts.setCyLayout({
                        ...visualizer.state.layouts.cyLayout,
                        edgeLengthVal: v
                    })}
                />

                <FormGroup>
                    <FilterCheckbox
                        label="Hide publications"
                        propValue={visualizer.state.filters.hidePublications}
                        propName="hidePublications"
                    />
                    <FilterCheckbox
                        label="Hide compartments"
                        propValue={visualizer.state.filters.hideCompartments}
                        propName="hideCompartments"
                        tooltip='Hide compartments such as "extracellular region" which can significantly complicate the visualization'
                    />
                    <FilterCheckbox
                        label="Hide species"
                        propValue={visualizer.state.filters.hideSpecies}
                        propName="hideSpecies"
                        tooltip='Hide species nodes such as "Homo sapiens" which can significantly complicate the visualization'
                    />
                    <FilterCheckbox
                        label="Hide database references"
                        propValue={visualizer.state.filters.hideDatabaseReferences}
                        propName="hideDatabaseReferences"
                    />
                </FormGroup>
            </Box>
        </Modal>
    );
};

export default SettingsModal;
