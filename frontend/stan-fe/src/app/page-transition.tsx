"use client"

import { usePathname } from "next/navigation"
import { useEffect, useState, type ReactNode } from "react"

interface PageTransitionProps {
  children: ReactNode
}

export default function PageTransition({ children }: PageTransitionProps) {
  const pathname = usePathname()
  const [isTransitioning, setIsTransitioning] = useState(false)
  const [displayChildren, setDisplayChildren] = useState(children)

  useEffect(() => {
    if (displayChildren === children) return

    setIsTransitioning(true)

    const timeout = setTimeout(() => {
      setDisplayChildren(children)
      setIsTransitioning(false)
    }, 150)

    return () => clearTimeout(timeout)
  }, [children, displayChildren])

  return (
    <div
      className={`page-transition-wrapper ${isTransitioning ? "opacity-0" : "opacity-100"}`}
      style={{ transition: "opacity 0.15s ease" }}
    >
      {displayChildren}
    </div>
  )
}
