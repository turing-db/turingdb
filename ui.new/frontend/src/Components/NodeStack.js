import { Stack } from "@mui/material"

export default function NodeStack({ children }) {
    return <Stack
        display="flex"
        p={"1em 2em 2em 1em"}
        flexDirection="row"
        alignItems="flex-start"
        justifyContent="flex-start"
        overflow="auto"
        sx={{
            flexWrap: "wrap"
        }}
    >
        {children}
    </Stack>
}
