import { Stack } from "@mui/material";

export default function NodeStack({ children }) {
  return (
    <Stack
      display="flex"
      flexDirection="row"
      alignItems="flex-start"
      justifyContent="flex-start"
      overflow="auto"
      sx={{
        flexWrap: "wrap",
      }}>
      {children}
    </Stack>
  );
}
