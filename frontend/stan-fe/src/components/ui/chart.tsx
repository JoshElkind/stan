"use client"

import * as React from "react"
import * as RechartsPrimitive from "recharts"

import { cn } from "@/lib/utils"

// Chart container component
const ChartContainer = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement> & {
    config: Record<string, any>
  }
>(({ className, config, children, ...props }, ref) => {
  return (
    <div ref={ref} className={cn("", className)} {...props}>
      {children}
    </div>
  )
})
ChartContainer.displayName = "ChartContainer"

// Chart tooltip component
const ChartTooltip = RechartsPrimitive.Tooltip

const ChartTooltipContent = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<typeof RechartsPrimitive.Tooltip> & {
    hideLabel?: boolean
    hideIndicator?: boolean
    indicator?: "line" | "dot" | "dashed"
    nameKey?: string
    labelKey?: string
  }
>(({ active, payload, label, hideLabel, hideIndicator, indicator = "dot", nameKey, labelKey }, ref) => {
  if (!active || !payload?.length) {
    return null
  }

  return (
    <div ref={ref} className="rounded-lg border bg-background p-2 shadow-md">
      {!hideLabel && (
        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col">
            <span className="text-[0.70rem] uppercase text-muted-foreground">{labelKey || "Label"}</span>
            <span className="font-bold text-muted-foreground">{label}</span>
          </div>
        </div>
      )}
      {payload.map((item, index) => (
        <div key={index} className="flex items-center gap-2">
          {!hideIndicator && (
            <div
              className={cn(
                "h-2.5 w-2.5 shrink-0 rounded-[2px]",
                indicator === "dot" && "rounded-full",
                indicator === "line" && "w-1",
              )}
              style={{
                backgroundColor: item.color,
              }}
            />
          )}
          <div className="flex flex-col">
            <span className="text-[0.70rem] uppercase text-muted-foreground">
              {nameKey ? item.payload[nameKey] : item.name}
            </span>
            <span className="font-bold">
              {typeof item.value === "number" ? item.value.toLocaleString() : item.value}
            </span>
          </div>
        </div>
      ))}
    </div>
  )
})
ChartTooltipContent.displayName = "ChartTooltipContent"

// Chart legend component
const ChartLegend = RechartsPrimitive.Legend

const ChartLegendContent = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div"> & {
    payload?: Array<any>
    nameKey?: string
  }
>(({ className, payload, nameKey, ...props }, ref) => {
  if (!payload?.length) {
    return null
  }

  return (
    <div ref={ref} className={cn("flex items-center justify-center gap-4", className)} {...props}>
      {payload.map((item, index) => (
        <div key={index} className="flex items-center gap-1.5">
          <div
            className="h-2 w-2 shrink-0 rounded-[2px]"
            style={{
              backgroundColor: item.color,
            }}
          />
          <span className="text-sm text-muted-foreground">{nameKey ? item.payload[nameKey] : item.value}</span>
        </div>
      ))}
    </div>
  )
})
ChartLegendContent.displayName = "ChartLegendContent"

export { ChartContainer, ChartTooltip, ChartTooltipContent, ChartLegend, ChartLegendContent }
