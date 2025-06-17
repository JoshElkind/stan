"use client"

import Image from "next/image"
import { Github, Linkedin } from "lucide-react"

export default function Footer() {
  return (
    <footer className="relative bg-slate-900/95 backdrop-blur-sm border-t border-slate-700/50">
      <div className="max-w-7xl mx-auto px-6 py-2">
        <div className="flex items-center justify-between">
          
          <div className="flex items-center gap-1">
            <div className="text-white font-elegant text-xs tracking-wide" style={{ fontFamily: "Permanent Marker, cursive" }}>A JE Production</div>
            
          </div>

      
          <div className="flex items-center gap-4">
          
            <div className="flex items-center gap-1.5">
              <Github className="h-4 w-4 text-white hover:text-white transition-colors duration-200" />
              <a
                href="https://github.com/JoshElkind"
                target="_blank"
                rel="noopener noreferrer"
                className="text-white hover:text-white/75 transition-colors duration-200 text-xs font-medium"
              >
                GitHub
              </a>
            </div>


            
           
            <div className="flex items-center gap-1.5">
              <Linkedin className="h-4 w-4 text-white hover:text-white transition-colors duration-200" />
              <a
                href="https://www.linkedin.com/in/joshua-elkind-565014345/"
                target="_blank"
                rel="noopener noreferrer"
                className="text-white hover:text-white/75 transition-colors duration-200 text-xs font-medium"
              >
                LinkedIn
              </a>
            </div>
          </div>
        </div>
      </div>
    </footer>
  )
}