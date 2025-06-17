"use client"

import HomePage from "@/components/homepage/HomePage"
import Footer from "@/components/footer"

export default function Page() {
  return (
    <div className="flex flex-col min-h-screen">
      <div className="flex-grow">
        <HomePage />
      </div>
      <Footer />
    </div>
  )
}
