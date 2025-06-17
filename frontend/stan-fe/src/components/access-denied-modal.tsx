"use client"

import { LogIn, X } from "lucide-react"
import { signIn } from "next-auth/react"

interface AccessDeniedModalProps {
  blockedPath: string
  onClose: () => void
}

export default function AccessDeniedModal({ blockedPath, onClose }: AccessDeniedModalProps) {
  const handleSignIn = async () => {
    if (blockedPath) {
      sessionStorage.setItem("redirectAfterLogin", blockedPath)
    }
    onClose()

    await signIn("google", {
      callbackUrl: blockedPath || "/",
    })
  }

  return (
    <div className="fixed inset-0 bg-black/30 backdrop-blur-[1px] flex items-center justify-center z-50 p-4">
      <div className="bg-slate-800 border border-slate-600 rounded-lg p-4 max-w-xs w-full shadow-xl relative">
        <button
          onClick={onClose}
          className="absolute top-2 right-2 p-1 text-slate-400 hover:text-white transition-colors"
        >
          <X className="h-3 w-3" />
        </button>

        <div className="text-center pr-6">
          <p className="text-white text-sm mb-4">You must sign in to access this page.</p>
          <div className="flex justify-center">
            <button
              onClick={handleSignIn}
              className="bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-4 py-2 text-sm rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] relative overflow-hidden group inline-flex items-center"
            >
              <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700"></div>
              <LogIn className="mr-2 h-3 w-3 relative z-10" />
              <span className="relative z-10">Sign In</span>
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
