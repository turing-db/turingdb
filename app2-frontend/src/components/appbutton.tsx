import { Button, ButtonSharedProps } from "@blueprintjs/core";

interface Props extends ButtonSharedProps{
  highlight?: boolean;
  borderless?: boolean;
}

export default function TAppButton(props: Props) {
  const { highlight, children, className, borderless, ...rest } = props;
  return (
    <Button
      {...rest}
      className={`app-button ${highlight && "is-highlighted"} \
                 ${borderless && "is-borderless"} ${className}`}
    >
      {children}
    </Button>
  );
}
