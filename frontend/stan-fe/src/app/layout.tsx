import type React from "react"
import "./globals.css"
import Image from "next/image"
import ClientLayout from "./clientLayout"




export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <head>
        <link rel="icon" type="image/x-icon" href="/favicon.ico" />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href="https://fonts.googleapis.com/css2?family=Audiowide&family=Bebas+Neue&family=Chakra+Petch:wght@400;600&family=Explora&family=Kanit:wght@400;700&family=Kode+Mono:wght@400;700&family=Megrim&family=Monoton&family=Orbitron:wght@400;500;600;700&family=Oxanium:wght@400;700&family=Playfair+Display:wght@400;500;600&family=Poppins:wght@300;400;500;600&family=Rajdhani:wght@300;400;500;600&family=Saira:wght@400;600;700&family=Saira+Stencil+One&family=Sriracha&family=Titillium+Web:wght@400;700&family=Ubuntu+Mono:wght@400;700&display=swap"
          rel="stylesheet"
        />
        <link
          href="https://fonts.googleapis.com/css2?family=Explora&family=Permanent+Marker&family=Sriracha&display=swap"
          rel="stylesheet"
        />
      </head>
      <body className="bg-slate-900 text-white relative overflow-x-hidden">
        <div className="absolute inset-0 -z-10">
          <Image src="/images/aurora-background.jpg" alt="Aurora Background" fill className="object-cover" priority />
          <div className="absolute inset-0 bg-black/30" />
        </div>

        <ClientLayout>{children}</ClientLayout>
      </body>
    </html>
  )
}
