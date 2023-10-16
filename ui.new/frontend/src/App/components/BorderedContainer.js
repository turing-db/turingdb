import { useTheme } from '@emotion/react';
import { Box, Typography, Divider } from '@mui/material'

export function BorderedContainerTitle(props) {
    const theme = useTheme();
    const { noDivider, title, children } = props;

    return <>
        <Box display="flex" alignItems="center">
            <Typography
                variant="h6"
                pl={3}
                color={theme.palette.primary.main}
                bgcolor={theme.palette.background.paper}
                m={1}
            >
                {title}
            </Typography>
            {children}
        </Box>
        {!noDivider && <Divider />}
    </>
}

export default function BorderedContainer(props) {
    const theme = useTheme();
    const { disablePadding, title, children, sx, ...style} = props;

    return <Box
        border={2}
        borderRadius={1}
        m={disablePadding ? 0 : "1em 1em 1em 1em"}
        p={0}
        borderColor={theme.palette.background.paper}
        bgcolor={theme.palette.background.paper}
        {...style}
        sx={{
            display: "flex",
            flexDirection: "column",
            maxHeight: "40vh",
            ...sx,
        }}
    >
        {title}
        {children &&
            <Box
                p={disablePadding ? 0 : 1}
                overflow="auto"
            >
                {children}
            </Box>}
    </Box >;
}
