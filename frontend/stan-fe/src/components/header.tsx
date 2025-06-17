"use client"

import type React from "react"

import Image from "next/image"
import { Button } from "@/components/ui/button"
import { useSession, signIn, signOut } from "next-auth/react"
import { useState, useRef, useEffect } from "react"
import { LogOut } from "lucide-react"
import { usePathname, useRouter } from "next/navigation"
import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

// Define the cn utility function inline to avoid import issues
function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export default function Header() {
  const { data: session } = useSession()
  const [isDropdownOpen, setIsDropdownOpen] = useState(false)
  const [isNavigating, setIsNavigating] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)
  const pathname = usePathname()
  const router = useRouter()
  const navigationTimerRef = useRef<NodeJS.Timeout | null>(null)
  const isHomePage = pathname === "/"

  const disableHoverEffects = () => {
    document.body.classList.add("navigation-in-progress")

    if (navigationTimerRef.current) {
      clearTimeout(navigationTimerRef.current)
    }

    setIsNavigating(true)

    navigationTimerRef.current = setTimeout(() => {
      document.body.classList.remove("navigation-in-progress")
      setIsNavigating(false)
    }, 500) 
  }

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        dropdownRef.current &&
        buttonRef.current &&
        !dropdownRef.current.contains(event.target as Node) &&
        !buttonRef.current.contains(event.target as Node)
      ) {
        setIsDropdownOpen(false)
      }
    }

    document.addEventListener("mousedown", handleClickOutside)
    return () => {
      document.removeEventListener("mousedown", handleClickOutside)
    }
  }, [])

  useEffect(() => {
    const style = document.createElement("style")
    style.innerHTML = `
      .navigation-in-progress * {
        pointer-events: none !important;
      }
      .navigation-in-progress button:hover,
      .navigation-in-progress a:hover {
        background-color: transparent !important;
        color: inherit !important;
      }
    `
    document.head.appendChild(style)

    return () => {
      document.head.removeChild(style)
    }
  }, [])

  const handleNavigation = (href: string) => {
    if (href === pathname) return
    disableHoverEffects()
    router.push(href)
  }

  const NavButton = ({ href, children }: { href: string; children: React.ReactNode }) => {
    const isActive = pathname === href

    return (
      <Button
        variant="ghost"
        className={cn(
          "text-white/90 px-4 py-2 h-10 font-medium transition-all duration-200 header-nav-button",
          isActive && "bg-white/10 text-white",
        )}
        onClick={(e) => {
          e.preventDefault()
          handleNavigation(href)
        }}
      >
        {children}
      </Button>
    )
  }

  return (
    <header
      className="w-full bg-transparent backdrop-blur-md border-b border-white/10 fixed top-0 left-0 right-0 z-50"
      style={{ backdropFilter: "blur(8px)", WebkitBackdropFilter: "blur(8px)" }}
    >
      <div className="max-w-7xl mx-auto px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-6">
            <div className="flex items-center cursor-pointer" onClick={() => handleNavigation("/")}>
              <Image src="/images/logo.png" alt="StAn Logo" width={160} height={80} className="h-16 w-auto" />
            </div>

            <NavButton href="/">Home</NavButton>

            <div className="flex items-center gap-4">
              <NavButton href="/algorithms">Algorithms</NavButton>
              <NavButton href="/evaluate">Evaluate</NavButton>
              <NavButton href="/visualize">Visualize</NavButton>
            </div>
          </div>

          <div className="flex items-center gap-4">
            <NavButton href="/faq">Guide</NavButton>
            <NavButton href="/about">About</NavButton>

            {session ? (
              <div className="relative">
                <Button
                  ref={buttonRef}
                  variant="ghost"
                  onClick={() => setIsDropdownOpen(!isDropdownOpen)}
                  className="p-2 h-14 w-14 transition-all duration-200 rounded-lg header-nav-button"
                  title="Account"
                >
                  <Image src="/images/person.png" alt="Account" width={40} height={40} className="h-10 w-10" />
                </Button>

                {isDropdownOpen && (
                  <div
                    ref={dropdownRef}
                    className="absolute right-0 mt-2 w-48 rounded-md shadow-lg bg-slate-800 border border-slate-700 overflow-hidden z-50 origin-top-right animate-in fade-in-5 zoom-in-95 duration-100"
                  >
                    <div className="py-1">
                      <div className="px-4 py-3 border-b border-slate-700">
                        <p className="text-sm font-medium text-white truncate">
                          {session.user?.name || "Signed In User"}
                        </p>
                        <p className="text-xs text-slate-400 truncate">{session.user?.email || ""}</p>
                      </div>

                      <div className="border-t border-slate-700 mt-1 pt-1">
                        <button
                          onClick={() => signOut({ callbackUrl: "/" })}
                          className="flex w-full items-center px-4 py-2 text-sm text-red-400 hover:bg-red-500/20 hover:text-red-300 transition-colors"
                        >
                          <LogOut className="mr-2 h-4 w-4" />
                          Sign Out
                        </button>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <Button
                variant="ghost"
                onClick={() => signIn("google")}
                className="text-white/90 px-4 py-2 h-10 font-medium transition-all duration-200 header-nav-button"
              >
                Sign In
              </Button>
            )}
          </div>
        </div>
      </div>
    </header>
  )
}
