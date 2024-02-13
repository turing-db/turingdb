'use client'

import Image from 'next/image'
import Link from 'next/link'
import LayoutSidebarItem from '@/components/layout/LayoutSidebarItem'
import IconBarChart from '@/assets/icons/bar-chart.svg'
import IconDatabase from '@/assets/icons/database.svg'
import IconSearch from '@/assets/icons/search-lg.svg'
import IconDataflow from '@/assets/icons/dataflow.svg'
import IconNetwork from '@/assets/icons/network.svg'
import IconFolder from '@/assets/icons/folder.svg'
import IconHelpCircle from '@/assets/icons/help-circle.svg'

const navItems = [
  /*
  {
    name: 'Dashboard',
    href: '/dashboard',
    icon: <IconBarChart />,
  },
  {
    name: 'Data',
    href: '/data',
    icon: <IconDatabase />,
  },
*/
  {
    name: 'Search',
    href: '/',
    icon: <IconSearch />,
  },
  /*
  {
    name: 'Flow',
    href: '/flow',
    icon: <IconDataflow />,
  },
  {
    name: 'Network',
    href: '/network',
    icon: <IconNetwork />,
  },
  {
    name: 'Folder',
    href: '/folder',
    icon: <IconFolder />,
  },
*/
]

const footerLinks = [
  {
    name: 'Help',
    href: '/help',
    icon: <IconHelpCircle />,
  },
  /*
  {
    name: 'Profile',
    href: '/profile',
    icon: 'user-circle',
  },
*/
]

/*
export default function LayoutSidebar() {
  return (
    <nav className=" flex w-16 flex-shrink-0 flex-col justify-between gap-8 bg-grey-800 px-3.5 py-4">
      <div className="flex flex-col gap-6">
        <Link href="/">
          <Image src="/logo.svg" width="36" height="36" alt="Turing" priority />
        </Link>
        <ul className="flex flex-col gap-2">
          {navItems.map(({ name, href, icon }) => (
            <li key={name}>
              <LayoutSidebarItem name={name} href={href} icon={icon} />
            </li>
          ))}
        </ul>
      </div>
      <ul className="flex flex-col gap-2">
        <li>
          <LayoutSidebarItem name="Help" href="/help" icon={<IconHelpCircle />} />
        </li>
        <li>
          <LayoutSidebarItem name="Profile" href="/profile" img="/img/profile.png" />
        </li>
      </ul>
    </nav>
  )
}
*/

export default function LayoutSidebar() {
  return (
    <nav className=" flex w-16 flex-shrink-0 flex-col justify-between gap-8 bg-grey-800 px-3.5 py-4">
      <div className="flex flex-col gap-6">
        <Link href="/">
          <Image src="/logo.svg" width="36" height="36" alt="Turing" priority />
        </Link>
        <ul className="flex flex-col gap-2">
          {navItems.map(({ name, href, icon }) => (
            <li key={name}>
              <LayoutSidebarItem name={name} href={href} icon={icon} />
            </li>
          ))}
        </ul>
      </div>
      <ul className="flex flex-col gap-2">
        <li>
          <LayoutSidebarItem name="Help" href="/help" icon={<IconHelpCircle />} />
        </li>
      </ul>
    </nav>
  )
}
