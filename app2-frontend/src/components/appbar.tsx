import React from "react";
import { Icon } from "@blueprintjs/core";

import logo from "@assets/imgs/logo.svg";

import AppTooltip from "@components/apptooltip";
import { PageType, useAppStore } from "@/appstore";

type TAppBarItemProps = {
  name: string;
  page: PageType;
  icon: string;
};

export const TAppBarItem: React.FC<TAppBarItemProps> = (props) => {
  const currentPage = useAppStore((state) => state.page);
  const setCurrentPage = useAppStore((state) => state.setPage);

  const className = React.useMemo(
    () =>
      (props.page == currentPage
        ? "sidebar-item-bg-gradient bg-grey-600 !text-content-primary "
        : "bg-grey-800 !text-content-fourth hover:bg-grey-700 ") +
      "flex h-10 w-10 items-center justify-center rounded-[4px] transition-colors cursor-pointer outline-none focus:outline-gray-800",
    [props.page, currentPage],
  );

  return (
    <AppTooltip label={props.name} interactionKind="hover-target" placement="right">
      <div className={className} onClick={() => setCurrentPage(props.page)}>
        <Icon icon={props.icon} large />
      </div>
    </AppTooltip>
  );
};

const TAppBar: React.FC = () => {
  return (
    <div>
      <div className="flex flex-col items-center w-[64px] space-y-1 border-grey-900 border bg-grey-800 h-full">
        <img src={logo} className="w-[36px] h-[36px] m-3" />
        <TAppBarItem icon="search" page="explore" name="Explore" />
        <TAppBarItem icon="graph" page="viewer" name="Viewer" />
      </div>
    </div>
  );
};

export { TAppBar };
