"use client"

import { Button } from "@/components/ui/button"
import { useRouter } from "next/navigation"

export default function HomePage() {
  const router = useRouter()
  return (
    <div className="relative overflow-hidden">
      <div className="relative z-10 flex items-center justify-center px-6 pt-40 pb-20 lg:pt-56 lg:pb-28 w-full">

        <div className="max-w-7xl mx-auto w-full">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 lg:gap-10 items-center">
            <div className="relative space-y-6 lg:pl-4">
            <h1
  className="font-light text-white tracking-wide leading-tight relative z-10 pl-7"
  style={{ fontSize: "4.75rem" }} 
>
                <span className="text-teal-400 font-medium">Your</span> Personal
                <br />
                Trading Sandbox
              </h1>
            </div>

            <div className="space-y-8 lg:pl-8">
              <div className="relative">
                <div className="absolute inset-0 bg-gradient-to-br from-white/5 to-transparent rounded-lg blur-sm"></div>
                <p className="relative text-lg md:text-xl text-white leading-relaxed font-light max-w-lg border-l-2 border-teal-400/40 pl-6 py-4">
                  Test your own trading algorithms based on your trade set ups. Create new algorithms or import your
                  existing ones. Then enter position metrics to evaluate how well they perform.
                </p>
              </div>

              <div className="flex justify-start gap-4 pt-2">
                <Button
                  onClick={() => router.push("/evaluate")}
                  size="default"
                  className="bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-8 py-3 text-base h-12 rounded-xl shadow-2xl shadow-teal-500/25 border border-teal-400/30 transition-all duration-300 hover:scale-105 hover:shadow-teal-400/40"
                >
                  Start Testing
                </Button>
                <Button
                  onClick={() => router.push("/all")}
                  size="default"
                  className="bg-white hover:bg-gray-100 text-black font-medium px-8 py-3 text-base h-12 rounded-xl shadow-2xl shadow-white/25 border border-gray-200 transition-all duration-300 hover:scale-105 hover:shadow-white/40"
                >
                  View Evaluations
                </Button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
