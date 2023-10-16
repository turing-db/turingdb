import { Typography } from '@mui/material'

const Span = ({children, ...props}) => {
    return <Typography display="inline" component="span" {...props}>
        {children}
    </Typography>
}

const Primary = ({children, ...props}) => {
    return <Span color="primary" {...props}>
        {children}
    </Span>
}

const Secondary = ({children, ...props}) => {
    return <Span color="secondary" {...props}>
        {children}
    </Span>
}

export default Span;
export { Primary, Secondary };
