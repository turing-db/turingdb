import { useTheme } from '@emotion/react';
import CachedIcon from '@mui/icons-material/Cached'
import { Grid, Stack, Box, Typography, Divider, IconButton, Autocomplete, TextField } from '@mui/material'

export default function BorderedContainer({ children }) {
    const theme = useTheme();

    return <Box p={0}
        border={1}
        borderRadius={3}
        m={1}
        borderColor={theme.palette.background.paper}
        bgcolor={theme.palette.background.paper}
        overflow="hidden"
        sx={{
            display: "flex",
            flexDirection: "column",
            maxHeight: "40vh"
        }}>
        {children}
    </Box >;
}
