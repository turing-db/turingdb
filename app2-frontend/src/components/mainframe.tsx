import React from "react";

import { useAppStore } from "@/appstore";

export declare interface AppProps {
  children?: React.ReactNode;
}

export const TMainFrame: React.FC<AppProps> = (props) => {
  const theme = useAppStore((state) => state.theme);

  return <div className={`bp5-${theme} flex flex-row absolute w-full h-full`}>
    {props.children}
  </div>
}

