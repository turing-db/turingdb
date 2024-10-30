import React from 'react'

import { Popover, PopoverInteractionKind, PopoverProps } from '@blueprintjs/core'

export interface AppTooltipProps extends PopoverProps {
  label?: string | React.ReactNode
}

export default function AppPopover(props: AppTooltipProps) {
  const { label, children, placement, interactionKind = PopoverInteractionKind.HOVER, ...rest } = props
  return (
    <Popover
      interactionKind={interactionKind}
      popoverClassName="app-tooltip"
      hoverCloseDelay={50}
      placement={placement}
      content={
        props.content || (
          <div className="px-2 py-[0.4375rem]">
            <span>{label}</span>
          </div>
        )
      }
      {...rest}
    >
      {children}
    </Popover>
  )
}

