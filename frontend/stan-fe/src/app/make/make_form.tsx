"use client"

import type React from "react"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Plus, Minus, BarChart3 } from "lucide-react"
import { useRouter } from "next/navigation"
import { useSession } from "next-auth/react"
import { CheckCircle, AlertCircle } from "lucide-react"

function smartConvert(str: string) {
  if (str.includes(".")) {
    return Number.parseFloat(str) // to float
  } else {
    return Number.parseInt(str, 10) // to int
  }
}

function capitalAction(str: string) {
  if (str == "sell") {
    return "Sell" // to float
  } else {
    return "Buy" // to int
  }
}

interface OuterConstant {
  id: string
  variableName: string
  valueType: "constant" | "expression"
  constantValue: string
  expressionLeftType: "variable" | "constant"
  expressionLeft: string
  operator: string
  expressionRightType: "variable" | "constant"
  expressionRight: string
}

interface RowWiseVariable {
  id: string
  type: "expression" | "window"
  name: string
  variableName: string
  // For expression type
  expressionLeftType: "variable" | "constant"
  expressionLeft: string
  operator: string
  expressionRightType: "variable" | "constant"
  expressionRight: string
  // For window type
  combiningFunction: string
  windowStart: string
  windowEnd: string
  windowVariableName: string
  innerVariables: Array<{
    id: string
    name: string
    leftType: "variable" | "constant"
    left: string
    operator: string
    rightType: "variable" | "constant"
    right: string
    isDefault: boolean
  }>
}

interface BooleanCondition {
  id: string
  leftValueType: "variable" | "constant"
  leftValue: string
  comparisonOperator: string
  rightValueType: "variable" | "constant"
  rightValue: string
}

interface BuySellAction {
  id: string
  action: "buy" | "sell"
  conditions: BooleanCondition[]
}

export default function MakeForm() {
  const [algorithmName, setAlgorithmName] = useState("")
  const [algorithmDescription, setAlgorithmDescription] = useState("")
  const [outerConstants, setOuterConstants] = useState<OuterConstant[]>([])
  const [rowWiseVariables, setRowWiseVariables] = useState<RowWiseVariable[]>([])
  const [buySellActions, setBuySellActions] = useState<BuySellAction[]>([])

  const [notification, setNotification] = useState<{
    type: "success" | "error"
    message: string
  } | null>(null)
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [nameError, setNameError] = useState(false)

  const router = useRouter()
  const { data: session } = useSession()

  // Auto-hide notification after 3 seconds
  useEffect(() => {
    if (notification) {
      const timer = setTimeout(() => {
        setNotification(null)
      }, 3000)
      return () => clearTimeout(timer)
    }
  }, [notification])

  const operators = ["*", "/", "^", "%", "^/", "//"]
  const comparisonOperators = ["==", ">=", "<=", "!=", "<", ">"]
  const combiningFunctions = ["avg", "sum", "min", "max"]

  // Section 2: Outer Constants
  const addOuterConstant = () => {
    const newConstant: OuterConstant = {
      id: Date.now().toString(),
      variableName: "",
      valueType: "constant",
      constantValue: "",
      expressionLeftType: "variable",
      expressionLeft: "",
      operator: "*",
      expressionRightType: "variable",
      expressionRight: "",
    }
    setOuterConstants([...outerConstants, newConstant])
  }

  const removeOuterConstant = (id: string) => {
    setOuterConstants(outerConstants.filter((item) => item.id !== id))
  }

  const updateOuterConstant = (id: string, field: string, value: any) => {
    setOuterConstants(outerConstants.map((item) => (item.id === id ? { ...item, [field]: value } : item)))
  }

  // Section 3: Row Wise Variables
  const addRowWiseVariable = (type: "expression" | "window") => {
    const newVariable: RowWiseVariable = {
      id: Date.now().toString(),
      type,
      name: "",
      variableName: "",
      expressionLeftType: "variable",
      expressionLeft: "",
      operator: "*",
      expressionRightType: "variable" | "constant",
      expressionRight: "",
      combiningFunction: "avg",
      windowStart: "",
      windowEnd: "",
      windowVariableName: "",
      innerVariables: [], // Start with empty array for both types
    }
    setRowWiseVariables([...rowWiseVariables, newVariable])
  }

  const removeRowWiseVariable = (id: string) => {
    setRowWiseVariables(rowWiseVariables.filter((item) => item.id !== id))
  }

  const updateRowWiseVariable = (id: string, field: string, value: any) => {
    setRowWiseVariables(rowWiseVariables.map((item) => (item.id === id ? { ...item, [field]: value } : item)))
  }

  const updateInnerVariable = (variableId: string, innerVarId: string, field: string, value: string) => {
    setRowWiseVariables(
      rowWiseVariables.map((item) =>
        item.id === variableId
          ? {
              ...item,
              innerVariables: item.innerVariables.map((inner) =>
                inner.id === innerVarId ? { ...inner, [field]: value } : inner,
              ),
            }
          : item,
      ),
    )
  }

  const addInnerVariable = (variableId: string) => {
    const newInnerVar = {
      id: Date.now().toString(),
      name: "",
      leftType: "variable" as const,
      left: "",
      operator: "*",
      rightType: "variable" as const,
      right: "",
      isDefault: false,
    }
    setRowWiseVariables(
      rowWiseVariables.map((item) =>
        item.id === variableId ? { ...item, innerVariables: [...item.innerVariables, newInnerVar] } : item,
      ),
    )
  }

  const removeInnerVariable = (variableId: string, innerVarId: string) => {
    setRowWiseVariables(
      rowWiseVariables.map((item) =>
        item.id === variableId
          ? {
              ...item,
              innerVariables: item.innerVariables.filter((inner) => inner.id !== innerVarId),
            }
          : item,
      ),
    )
  }

  // Section 4: Buy/Sell Actions
  const addBuySellAction = () => {
    const newAction: BuySellAction = {
      id: Date.now().toString(),
      action: "buy",
      conditions: [
        {
          id: Date.now().toString(),
          leftValueType: "variable",
          leftValue: "",
          comparisonOperator: "==",
          rightValueType: "variable",
          rightValue: "",
        },
      ],
    }
    setBuySellActions([...buySellActions, newAction])
  }

  const removeBuySellAction = (id: string) => {
    setBuySellActions(buySellActions.filter((item) => item.id !== id))
  }

  const updateBuySellAction = (id: string, field: string, value: any) => {
    setBuySellActions(buySellActions.map((item) => (item.id === id ? { ...item, [field]: value } : item)))
  }

  const addCondition = (actionId: string) => {
    const newCondition: BooleanCondition = {
      id: Date.now().toString(),
      leftValueType: "variable",
      leftValue: "",
      comparisonOperator: "==",
      rightValueType: "variable",
      rightValue: "",
    }
    setBuySellActions(
      buySellActions.map((action) =>
        action.id === actionId ? { ...action, conditions: [...action.conditions, newCondition] } : action,
      ),
    )
  }

  const removeCondition = (actionId: string, conditionId: string) => {
    setBuySellActions(
      buySellActions.map((action) =>
        action.id === actionId
          ? { ...action, conditions: action.conditions.filter((cond) => cond.id !== conditionId) }
          : action,
      ),
    )
  }

  const updateCondition = (actionId: string, conditionId: string, field: string, value: string) => {
    setBuySellActions(
      buySellActions.map((action) =>
        action.id === actionId
          ? {
              ...action,
              conditions: action.conditions.map((cond) =>
                cond.id === conditionId ? { ...cond, [field]: value } : cond,
              ),
            }
          : action,
      ),
    )
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!algorithmName.trim()) {
      setNameError(true)
      // Scroll to the algorithm name field
      document.getElementById("algorithmName")?.scrollIntoView({ behavior: "smooth", block: "center" })
      return
    }

    if (!session?.accessToken) {
      setNotification({
        type: "error",
        message: "No authentication token available",
      })
      return
    }

    setIsSubmitting(true)

    // Process form data as before
    const outer_const_send = []
    for (let i = 0; i < outerConstants.length; i++) {
      const curr_const = outerConstants[i]
      const single_send = []
      const Variable_name = curr_const.variableName
      const Variable_type = curr_const.valueType

      if (Variable_type == "constant") {
        single_send.push(0)
        single_send.push(Variable_name)
        single_send.push(smartConvert(curr_const.constantValue))
        outer_const_send.push(single_send)
      } else {
        single_send.push(1)
        single_send.push(Variable_name)
        const data_expression = []
        const bools_type = []
        const expression_three = []
        if (curr_const.expressionLeftType == "constant") {
          bools_type.push(0)
          expression_three.push(smartConvert(curr_const.expressionLeft))
        } else {
          bools_type.push(1)
          expression_three.push(curr_const.expressionLeft)
        }
        if (curr_const.expressionRightType == "constant") {
          bools_type.push(0)
          expression_three.push(smartConvert(curr_const.expressionRight))
        } else {
          bools_type.push(1)
          expression_three.push(curr_const.expressionRight)
        }
        data_expression.push(bools_type)
        data_expression.push(expression_three)
        data_expression.push(curr_const.operator)
        single_send.push(data_expression)
        outer_const_send.push(single_send)
      }
    }

    const row_wise_send = []
    for (let i = 0; i < rowWiseVariables.length; i++) {
      const single_send = []
      const curr_top_var = rowWiseVariables[i]
      const name = curr_top_var.windowVariableName
      const type = curr_top_var.type

      if (type == "expression") {
        single_send.push(0)
        single_send.push(name)
        const data_expression = []
        const bools_type = []
        const expression_three = []
        if (curr_top_var.expressionLeftType == "constant") {
          bools_type.push(0)
          expression_three.push(smartConvert(curr_top_var.expressionLeft))
        } else {
          bools_type.push(1)
          expression_three.push(curr_top_var.expressionLeft)
        }
        if (curr_top_var.expressionRightType == "constant") {
          bools_type.push(0)
          expression_three.push(smartConvert(curr_top_var.expressionRight))
        } else {
          bools_type.push(1)
          expression_three.push(curr_top_var.expressionRight)
        }
        data_expression.push(bools_type)
        data_expression.push(expression_three)
        data_expression.push(curr_top_var.operator)
        single_send.push(data_expression)
        row_wise_send.push(single_send)
      } else {
        single_send.push(1)
        single_send.push(curr_top_var.name)
        const data_expression = []
        const bounds_window = [[0, 0]]
        const bound_start_end = [smartConvert(curr_top_var.windowStart), smartConvert(curr_top_var.windowEnd)]
        bounds_window.push(bound_start_end)
        data_expression.push(curr_top_var.combiningFunction)
        data_expression.push(bounds_window)
        const all_vars = curr_top_var.innerVariables
        const small_vars_converted = []
        for (let k = 0; k < all_vars.length; k++) {
          const curr_small = all_vars[k]
          const single_small = []
          const single_bools = []
          const single_vals = []
          const single_operator = curr_small.operator
          if (curr_small.leftType == "constant") {
            single_bools.push(0)
            single_vals.push(smartConvert(curr_small.left))
          } else {
            single_bools.push(1)
            single_vals.push(curr_small.left)
          }
          if (curr_small.rightType == "constant") {
            single_bools.push(0)
            single_vals.push(smartConvert(curr_small.right))
          } else {
            single_bools.push(1)
            single_vals.push(curr_small.right)
          }
          single_small.push(single_bools)
          single_small.push(single_vals)
          single_small.push(single_operator)
          const with_name = [1, curr_small.name, single_small]
          small_vars_converted.push(with_name)
        }
        data_expression.push(small_vars_converted)
        data_expression.push(name)
        single_send.push(data_expression)
        row_wise_send.push(single_send)
      }
    }

    const buy_sell_send = []
    for (let i = 0; i < buySellActions.length; i++) {
      const curr_action = buySellActions[i]
      const single_send = []
      single_send.push(capitalAction(curr_action.action))
      const arr_and_conds = []
      const all_conds = curr_action.conditions
      for (let j = 0; j < all_conds.length; j++) {
        const curr_cond_single_sub = []
        const curr_cond = all_conds[j]
        const curr_cond_bool = []
        const curr_cond_three = []
        if (curr_cond.leftValueType == "constant") {
          curr_cond_bool.push(0)
          curr_cond_three.push(smartConvert(curr_cond.leftValue))
        } else {
          curr_cond_bool.push(1)
          curr_cond_three.push(curr_cond.leftValue)
        }
        curr_cond_three.push(curr_cond.comparisonOperator)
        if (curr_cond.rightValueType == "constant") {
          curr_cond_bool.push(0)
          curr_cond_three.push(smartConvert(curr_cond.rightValue))
        } else {
          curr_cond_bool.push(1)
          curr_cond_three.push(curr_cond.rightValue)
        }
        curr_cond_single_sub.push(curr_cond_bool)
        curr_cond_single_sub.push(curr_cond_three)
        arr_and_conds.push(curr_cond_single_sub)
      }
      single_send.push(arr_and_conds)
      buy_sell_send.push(single_send)
    }

    // Prepare API payload
    const payload = {
      algoname: algorithmName,
      algodescription: algorithmDescription,
      outer_consts: outer_const_send,
      row_wise_consts: row_wise_send,
      deciders: buy_sell_send,
    }
    

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/scripts/user/add/`, {
        method: "POST",
        credentials: 'include',
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${session.accessToken}`,
        },
        body: JSON.stringify(payload),
      })

      const data = await response.json()

      if (!response.ok) {
        const errorMessage = data.error || "Algorithm creation failed"
        setNotification({
          type: "error",
          message: errorMessage,
        })
        return
      }

      // Success case - show notification and redirect
      setNotification({
        type: "success",
        message: "Algorithm created successfully!",
      })

      // Redirect to my algorithms page after a short delay
      setTimeout(() => {
        router.push("/algorithms")
      }, 1500)
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Algorithm creation failed"
      setNotification({
        type: "error",
        message: errorMessage,
      })
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <div className="w-full min-h-[calc(100vh-64px)]" style={{ backgroundColor: "#1c1e21" }}>
      {/* Notification Banner */}
      {notification && (
        <div
          className={`fixed top-4 left-1/2 transform -translate-x-1/2 z-50 px-6 py-3 rounded-lg shadow-lg border transition-all duration-300 ${
            notification.type === "success"
              ? "bg-green-900/90 border-green-700 text-green-200"
              : "bg-red-900/90 border-red-700 text-red-200"
          }`}
        >
          <div className="flex items-center gap-2">
            {notification.type === "success" ? (
              <CheckCircle className="h-4 w-4" />
            ) : (
              <AlertCircle className="h-4 w-4" />
            )}
            <span className="font-medium">{notification.message}</span>
          </div>
        </div>
      )}
      <div className="p-8 h-full">
        {/* Header section with title */}
        <div className="flex items-center justify-between mb-8">
          <div className="relative">
            <div className="relative">
              <div className="flex items-center gap-4 mb-3">
                {/* Logo */}
                <div className="w-12 h-12 bg-gradient-to-br from-teal-500 to-cyan-500 rounded-xl flex items-center justify-center shadow-lg shadow-teal-500/25">
                  <BarChart3 className="h-6 w-6 text-white" />
                </div>

                <h1 className="text-5xl font-bold text-white tracking-tight">Create Algorithm</h1>
              </div>

              <div className="flex items-center">
                <p className="text-lg text-white font-medium">Build your custom trading algorithm</p>
              </div>

              {/* Decorative line */}
              <div className="mt-4 h-1 w-32 bg-gradient-to-r from-teal-500 to-cyan-500 rounded-full"></div>
            </div>
          </div>
        </div>

        {/* Clean separator with gradient */}
        <div className="relative mb-8">
          <div className="h-px bg-gradient-to-r from-transparent via-slate-600 to-transparent"></div>
          <div className="absolute left-1/2 top-0 transform -translate-x-1/2 -translate-y-1/2 w-3 h-3 bg-gradient-to-r from-teal-400 to-cyan-400 rounded-full"></div>
        </div>

        <div className="max-w-5xl mx-auto space-y-10">
          <form onSubmit={handleSubmit} className="space-y-10">
            {/* Section 1: Algorithm Name & Description - Compact */}
            <div className="space-y-4">
              <div>
                <Label htmlFor="algorithmName" className="text-sm text-white font-medium mb-2 block">
                  Algorithm Name
                </Label>
                <div className="relative">
                  <Input
                    id="algorithmName"
                    value={algorithmName}
                    onChange={(e) => {
                      setAlgorithmName(e.target.value)
                      if (e.target.value.trim()) {
                        setNameError(false)
                      }
                    }}
                    onBlur={() => {
                      if (algorithmName.trim()) {
                        setNameError(false)
                      }
                    }}
                    placeholder="Enter algorithm name"
                    className={`bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 focus:ring-teal-400/20 h-10 ${
                      nameError ? "border-red-500 focus:border-red-500 focus:ring-red-500/20" : ""
                    }`}
                  />
                </div>
              </div>
              <div>
                <Label htmlFor="algorithmDescription" className="text-sm text-white font-medium mb-2 block">
                  Description
                </Label>
                <div className="relative">
                  <textarea
                    id="algorithmDescription"
                    value={algorithmDescription}
                    onChange={(e) => {
                      if (e.target.value.length <= 500) {
                        setAlgorithmDescription(e.target.value)
                      }
                    }}
                    placeholder="Brief description..."
                    rows={2}
                    className="w-full bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 focus:ring-teal-400/20 text-sm p-3 rounded-lg resize-none"
                  />
                  <div className="absolute bottom-2 right-2 text-xs text-slate-400">
                    {algorithmDescription.length}/500
                  </div>
                </div>
              </div>
            </div>

            {/* Divider */}
            <div className="h-px bg-gradient-to-r from-transparent via-slate-600 to-transparent"></div>

            {/* Section 2: Outer Constant Values */}
            <div className="space-y-6">
              <h2 className="text-xl font-semibold text-white tracking-wide">Outer Constant Values</h2>
              <div className="space-y-5">
                {outerConstants.map((constant) => (
                  <div
                    key={constant.id}
                    className="border border-slate-700/50 rounded-xl p-6 space-y-4 bg-slate-800/30 shadow-xl"
                  >
                    <div className="flex justify-start">
                      <Select
                        value={constant.valueType}
                        onValueChange={(value) => updateOuterConstant(constant.id, "valueType", value)}
                      >
                        <SelectTrigger className="w-44 font-bold border-slate-700 bg-slate-800/50 text-white h-11 hover:bg-slate-700/70 hover:border-slate-600">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="bg-slate-800 border-slate-700">
                          <SelectItem value="constant" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                            Constant
                          </SelectItem>
                          <SelectItem value="expression" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                            Expression
                          </SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="flex items-center gap-3">
                      <Input
                        placeholder="Name"
                        value={constant.variableName}
                        onChange={(e) => updateOuterConstant(constant.id, "variableName", e.target.value)}
                        className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11"
                      />
                      <span className="text-xl font-mono text-slate-300">:</span>
                      <div className="flex-[2]">
                        {constant.valueType === "constant" ? (
                          <Input
                            placeholder="Const Value"
                            value={constant.constantValue}
                            onChange={(e) => updateOuterConstant(constant.id, "constantValue", e.target.value)}
                            className="bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11"
                          />
                        ) : (
                          <div className="flex items-center gap-3">
                            <div className="flex items-center gap-2 flex-1">
                              <Select
                                value={constant.expressionLeftType}
                                onValueChange={(value) => updateOuterConstant(constant.id, "expressionLeftType", value)}
                              >
                                <SelectTrigger className="w-20 bg-slate-800/50 border-slate-700 text-white h-11 hover:bg-slate-700/70 hover:border-slate-600">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent className="bg-slate-800 border-slate-700">
                                  <SelectItem
                                    value="variable"
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    Var
                                  </SelectItem>
                                  <SelectItem
                                    value="constant"
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    Const
                                  </SelectItem>
                                </SelectContent>
                              </Select>
                              <Input
                                placeholder={constant.expressionLeftType === "variable" ? "Var Name" : "Const Value"}
                                value={constant.expressionLeft}
                                onChange={(e) => updateOuterConstant(constant.id, "expressionLeft", e.target.value)}
                                className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11"
                              />
                            </div>
                            <Select
                              value={constant.operator}
                              onValueChange={(value) => updateOuterConstant(constant.id, "operator", value)}
                            >
                              <SelectTrigger className="w-20 bg-slate-800/50 border-slate-700 text-white h-11 hover:bg-slate-700/70 hover:border-slate-600">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent className="bg-slate-800 border-slate-700">
                                {operators.map((op) => (
                                  <SelectItem
                                    key={op}
                                    value={op}
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    {op}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                            <div className="flex items-center gap-2 flex-1">
                              <Select
                                value={constant.expressionRightType}
                                onValueChange={(value) =>
                                  updateOuterConstant(constant.id, "expressionRightType", value)
                                }
                              >
                                <SelectTrigger className="w-20 bg-slate-800/50 border-slate-700 text-white h-11 hover:bg-slate-700/70 hover:border-slate-600">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent className="bg-slate-800 border-slate-700">
                                  <SelectItem
                                    value="variable"
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    Var
                                  </SelectItem>
                                  <SelectItem
                                    value="constant"
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    Const
                                  </SelectItem>
                                </SelectContent>
                              </Select>
                              <Input
                                placeholder={constant.expressionRightType === "variable" ? "Var Name" : "Const Value"}
                                value={constant.expressionRight}
                                onChange={(e) => updateOuterConstant(constant.id, "expressionRight", e.target.value)}
                                className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11"
                              />
                            </div>
                          </div>
                        )}
                      </div>
                      <Button
                        type="button"
                        variant="destructive"
                        size="sm"
                        onClick={() => removeOuterConstant(constant.id)}
                        className="h-11 w-11 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 hover:border-red-500/50"
                      >
                        <Minus className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                ))}
                <Button
                  type="button"
                  onClick={addOuterConstant}
                  className="w-full bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-6 py-2.5 h-12 rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] relative overflow-hidden group"
                >
                  <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700"></div>
                  <Plus className="h-4 w-4 mr-2 relative z-10" />
                  <span className="relative z-10">Add Outer Constant</span>
                </Button>
              </div>
            </div>

            {/* Divider */}
            <div className="h-px bg-gradient-to-r from-transparent via-slate-600 to-transparent"></div>

            {/* Section 3: Row Wise Variables */}
            <div className="space-y-6">
              <h2 className="text-xl font-semibold text-white tracking-wide">Row Wise Variables</h2>
              <div className="flex items-center gap-2 flex-wrap">
                <span className="bg-teal-500/20 border border-teal-400/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm font-medium">
                  Available Row Values:
                </span>
                <span className="bg-teal-500/20 border border-teal-400/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm">
                  Open
                </span>
                <span className="bg-teal-500/20 border border-teal-400/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm">
                  High
                </span>
                <span className="bg-teal-500/20 border border-teal-400/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm">
                  Low
                </span>
                <span className="bg-teal-500/20 border border-teal-400/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm">
                  Close
                </span>
                <span className="bg-teal-500/20 border border-teal-400/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm">
                  Volume
                </span>
              </div>
              <div className="space-y-5">
                {rowWiseVariables.map((variable) => (
                  <div
                    key={variable.id}
                    className="border border-slate-700/50 rounded-xl p-6 space-y-5 bg-slate-800/30 shadow-xl"
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex gap-3">
                        <Button
                          type="button"
                          variant={variable.type === "expression" ? "default" : "outline"}
                          size="sm"
                          onClick={() => updateRowWiseVariable(variable.id, "type", "expression")}
                          className={
                            variable.type === "expression"
                              ? "bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white border-2 border-teal-400/30 shadow-lg h-10"
                              : "bg-slate-800/50 border-slate-700 text-slate-300 hover:bg-slate-700/70 hover:text-white hover:border-slate-600 h-10"
                          }
                        >
                          Expression
                        </Button>
                        <Button
                          type="button"
                          variant={variable.type === "window" ? "default" : "outline"}
                          size="sm"
                          onClick={() => updateRowWiseVariable(variable.id, "type", "window")}
                          className={
                            variable.type === "window"
                              ? "bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white border-2 border-teal-400/30 shadow-lg h-10"
                              : "bg-slate-800/50 border-slate-700 text-slate-300 hover:bg-slate-700/70 hover:text-white hover:border-slate-600 h-10"
                          }
                        >
                          Window Calculated Value
                        </Button>
                      </div>
                      <Button
                        type="button"
                        variant="destructive"
                        size="sm"
                        onClick={() => removeRowWiseVariable(variable.id)}
                        className="h-10 w-10 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 hover:border-red-500/50"
                      >
                        <Minus className="h-4 w-4" />
                      </Button>
                    </div>
                    {variable.type === "window" && (
                      <div className="flex items-center gap-3">
                        <Label htmlFor={`name-${variable.id}`} className="text-sm text-white font-medium w-12">
                          Name:
                        </Label>
                        <Input
                          id={`name-${variable.id}`}
                          placeholder="Variable Name"
                          value={variable.name}
                          onChange={(e) => updateRowWiseVariable(variable.id, "name", e.target.value)}
                          className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11"
                        />
                      </div>
                    )}
                    {variable.type === "window" && (
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="bg-cyan-500/20 border border-cyan-400/30 px-2 py-1 rounded-lg text-cyan-200 text-xs font-medium">
                          Available Window Row Values:
                        </span>
                        <span className="bg-cyan-500/20 border border-cyan-400/30 px-2 py-1 rounded-lg text-cyan-200 text-xs">
                          Window_Open
                        </span>
                        <span className="bg-cyan-500/20 border border-cyan-400/30 px-2 py-1 rounded-lg text-cyan-200 text-xs">
                          Window_High
                        </span>
                        <span className="bg-cyan-500/20 border border-cyan-400/30 px-2 py-1 rounded-lg text-cyan-200 text-xs">
                          Window_Low
                        </span>
                        <span className="bg-cyan-500/20 border border-cyan-400/30 px-2 py-1 rounded-lg text-cyan-200 text-xs">
                          Window_Close
                        </span>
                        <span className="bg-cyan-500/20 border border-cyan-400/30 px-2 py-1 rounded-lg text-cyan-200 text-xs">
                          Window_Volume
                        </span>
                      </div>
                    )}

                    {variable.type === "expression" ? (
                      <div className="flex items-center gap-3">
                        <Input
                          placeholder="Name"
                          value={variable.variableName}
                          onChange={(e) => updateRowWiseVariable(variable.id, "variableName", e.target.value)}
                          className="w-24 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11"
                        />
                        <span className="text-xl font-mono text-slate-300">:</span>
                        <div className="flex items-center gap-2 flex-1">
                          <Select
                            value={variable.expressionLeftType}
                            onValueChange={(value) => updateRowWiseVariable(variable.id, "expressionLeftType", value)}
                          >
                            <SelectTrigger className="w-20 bg-slate-800/50 border-slate-700 text-white h-11 hover:bg-slate-700/70 hover:border-slate-600">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent className="bg-slate-800 border-slate-700">
                              <SelectItem value="variable" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                                Var
                              </SelectItem>
                              <SelectItem value="constant" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                                Const
                              </SelectItem>
                            </SelectContent>
                          </Select>
                          <Input
                            placeholder={variable.expressionLeftType === "variable" ? "Var Name" : "Const Value"}
                            value={variable.expressionLeft}
                            onChange={(e) => updateRowWiseVariable(variable.id, "expressionLeft", e.target.value)}
                            className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11"
                          />
                        </div>
                        <Select
                          value={variable.operator}
                          onValueChange={(value) => updateRowWiseVariable(variable.id, "operator", value)}
                        >
                          <SelectTrigger className="w-20 bg-slate-800/50 border-slate-700 text-white h-11 hover:bg-slate-700/70 hover:border-slate-600">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent className="bg-slate-800 border-slate-700">
                            {operators.map((op) => (
                              <SelectItem
                                key={op}
                                value={op}
                                className="text-white hover:bg-slate-700 focus:bg-slate-700"
                              >
                                {op}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        <div className="flex items-center gap-2 flex-1">
                          <Select
                            value={variable.expressionRightType}
                            onValueChange={(value) => updateRowWiseVariable(variable.id, "expressionRightType", value)}
                          >
                            <SelectTrigger className="w-20 bg-slate-800/50 border-slate-700 text-white h-11 hover:bg-slate-700/70 hover:border-slate-600">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent className="bg-slate-800 border-slate-700">
                              <SelectItem value="variable" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                                Var
                              </SelectItem>
                              <SelectItem value="constant" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                                Const
                              </SelectItem>
                            </SelectContent>
                          </Select>
                          <Input
                            placeholder={variable.expressionRightType === "variable" ? "Var Name" : "Const Value"}
                            value={variable.expressionRight}
                            onChange={(e) => updateRowWiseVariable(variable.id, "expressionRight", e.target.value)}
                            className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11"
                          />
                        </div>
                      </div>
                    ) : (
                      <div className="space-y-4">
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                          <div>
                            <Label className="text-sm text-white font-medium">Combining Function</Label>
                            <Select
                              value={variable.combiningFunction}
                              onValueChange={(value) => updateRowWiseVariable(variable.id, "combiningFunction", value)}
                            >
                              <SelectTrigger className="bg-slate-800/50 border-slate-700 text-white h-11 mt-1 hover:bg-slate-700/70 hover:border-slate-600">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent className="bg-slate-800 border-slate-700">
                                {combiningFunctions.map((func) => (
                                  <SelectItem
                                    key={func}
                                    value={func}
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    {func}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </div>
                          <div>
                            <Label className="text-sm text-white font-medium">Extract Variable</Label>
                            <Input
                              placeholder="Name"
                              value={variable.windowVariableName}
                              onChange={(e) => updateRowWiseVariable(variable.id, "windowVariableName", e.target.value)}
                              className="bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11 mt-1"
                            />
                          </div>
                          <div>
                            <Label className="text-sm text-white font-medium">Window Start</Label>
                            <Input
                              type="number"
                              min="1"
                              placeholder="Start"
                              value={variable.windowStart}
                              onChange={(e) => updateRowWiseVariable(variable.id, "windowStart", e.target.value)}
                              className="bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11 mt-1"
                            />
                          </div>
                          <div>
                            <Label className="text-sm text-white font-medium">Window End</Label>
                            <Input
                              type="number"
                              min="1"
                              placeholder="End"
                              value={variable.windowEnd}
                              onChange={(e) => updateRowWiseVariable(variable.id, "windowEnd", e.target.value)}
                              className="bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11 mt-1"
                            />
                          </div>
                        </div>

                        <div className="space-y-3">
                          <div className="flex items-center justify-between">
                            <Label className="text-sm font-medium text-white">Inner Window Variables</Label>
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              onClick={() => addInnerVariable(variable.id)}
                              className="bg-slate-800/50 border-slate-700 text-slate-300 hover:bg-slate-700/70 hover:text-white hover:border-slate-600 h-9"
                            >
                              <Plus className="h-4 w-4 mr-1" />
                              Add Variable
                            </Button>
                          </div>
                          {variable.innerVariables.map((innerVar) => (
                            <div
                              key={innerVar.id}
                              className="bg-slate-800/50 p-3 rounded-lg border border-slate-700/40 flex items-center gap-3"
                            >
                              <div className="flex items-center gap-2 w-24">
                                <Input
                                  placeholder="Name"
                                  value={innerVar.name}
                                  onChange={(e) =>
                                    updateInnerVariable(variable.id, innerVar.id, "name", e.target.value)
                                  }
                                  className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-10"
                                />
                                <span className="text-sm text-slate-300">:</span>
                              </div>
                              <div className="flex items-center gap-2 flex-1">
                                <Select
                                  value={innerVar.leftType}
                                  onValueChange={(value) =>
                                    updateInnerVariable(variable.id, innerVar.id, "leftType", value)
                                  }
                                >
                                  <SelectTrigger className="w-16 bg-slate-800/50 border-slate-700 text-white h-10 hover:bg-slate-700/70 hover:border-slate-600">
                                    <SelectValue />
                                  </SelectTrigger>
                                  <SelectContent className="bg-slate-800 border-slate-700">
                                    <SelectItem
                                      value="variable"
                                      className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                    >
                                      Var
                                    </SelectItem>
                                    <SelectItem
                                      value="constant"
                                      className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                    >
                                      Const
                                    </SelectItem>
                                  </SelectContent>
                                </Select>
                                <Input
                                  placeholder={innerVar.leftType === "variable" ? "Var Name" : "Const Value"}
                                  value={innerVar.left}
                                  onChange={(e) =>
                                    updateInnerVariable(variable.id, innerVar.id, "left", e.target.value)
                                  }
                                  className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-10"
                                />
                              </div>
                              <Select
                                value={innerVar.operator}
                                onValueChange={(value) =>
                                  updateInnerVariable(variable.id, innerVar.id, "operator", value)
                                }
                              >
                                <SelectTrigger className="w-20 bg-slate-800/50 border-slate-700 text-white h-10 hover:bg-slate-700/70 hover:border-slate-600">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent className="bg-slate-800 border-slate-700">
                                  {operators.map((op) => (
                                    <SelectItem
                                      key={op}
                                      value={op}
                                      className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                    >
                                      {op}
                                    </SelectItem>
                                  ))}
                                </SelectContent>
                              </Select>
                              <div className="flex items-center gap-2 flex-1">
                                <Select
                                  value={innerVar.rightType}
                                  onValueChange={(value) =>
                                    updateInnerVariable(variable.id, innerVar.id, "rightType", value)
                                  }
                                >
                                  <SelectTrigger className="w-16 bg-slate-800/50 border-slate-700 text-white h-10 hover:bg-slate-700/70 hover:border-slate-600">
                                    <SelectValue />
                                  </SelectTrigger>
                                  <SelectContent className="bg-slate-800 border-slate-700">
                                    <SelectItem
                                      value="variable"
                                      className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                    >
                                      Var
                                    </SelectItem>
                                    <SelectItem
                                      value="constant"
                                      className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                    >
                                      Const
                                    </SelectItem>
                                  </SelectContent>
                                </Select>
                                <Input
                                  placeholder={innerVar.rightType === "variable" ? "Var Name" : "Const Value"}
                                  value={innerVar.right}
                                  onChange={(e) =>
                                    updateInnerVariable(variable.id, innerVar.id, "right", e.target.value)
                                  }
                                  className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-10"
                                />
                              </div>
                              <Button
                                type="button"
                                variant="destructive"
                                size="sm"
                                onClick={() => removeInnerVariable(variable.id, innerVar.id)}
                                className="h-10 w-10 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 hover:border-red-500/50"
                              >
                                <Minus className="h-4 w-4" />
                              </Button>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                ))}
                <div className="flex gap-3">
                  <Button
                    type="button"
                    onClick={() => addRowWiseVariable("expression")}
                    className="flex-1 bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-6 py-2.5 h-12 rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] relative overflow-hidden group"
                  >
                    <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700"></div>
                    <Plus className="h-4 w-4 mr-2 relative z-10" />
                    <span className="relative z-10">Add Expression Variable</span>
                  </Button>
                  <Button
                    type="button"
                    onClick={() => addRowWiseVariable("window")}
                    className="flex-1 bg-gradient-to-r from-cyan-500 to-teal-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-6 py-2.5 h-12 rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] relative overflow-hidden group"
                  >
                    <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700"></div>
                    <Plus className="h-4 w-4 mr-2 relative z-10" />
                    <span className="relative z-10">Add Window Value Variable</span>
                  </Button>
                </div>
              </div>
            </div>

            {/* Divider */}
            <div className="h-px bg-gradient-to-r from-transparent via-slate-600 to-transparent"></div>

            {/* Section 4: Buy/Sell Actions */}
            <div className="space-y-6">
              <h2 className="text-xl font-semibold text-white tracking-wide">Buy/Sell Actions</h2>
              <div className="space-y-5">
                {buySellActions.map((action) => (
                  <div
                    key={action.id}
                    className="border border-slate-700/50 rounded-xl p-6 space-y-4 bg-slate-800/30 shadow-xl"
                  >
                    <div className="flex items-center justify-between">
                      <Select
                        value={action.action}
                        onValueChange={(value) => updateBuySellAction(action.id, "action", value)}
                      >
                        <SelectTrigger
                          className={`w-36 font-bold h-11 hover:border-opacity-70 ${
                            action.action === "buy"
                              ? "bg-green-500/20 border-green-400/50 text-green-200 hover:bg-green-500/30 hover:border-green-400"
                              : "bg-red-500/20 border-red-400/50 text-red-200 hover:bg-red-500/30 hover:border-red-400"
                          }`}
                        >
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="bg-slate-800 border-slate-700">
                          <SelectItem value="buy" className="text-green-400 hover:bg-slate-700 focus:bg-slate-700">
                            Buy
                          </SelectItem>
                          <SelectItem value="sell" className="text-red-400 hover:bg-slate-700 focus:bg-slate-700">
                            Sell
                          </SelectItem>
                        </SelectContent>
                      </Select>
                      <Button
                        type="button"
                        variant="destructive"
                        size="sm"
                        onClick={() => removeBuySellAction(action.id)}
                        className="h-11 w-11 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 hover:border-red-500/50"
                      >
                        <Minus className="h-4 w-4" />
                      </Button>
                    </div>

                    <div className="space-y-3">
                      <Label className="text-sm font-medium text-white">Conditions</Label>
                      {action.conditions.map((condition, index) => (
                        <div key={condition.id} className="space-y-3">
                          {index > 0 && <div className="text-center text-sm font-medium text-slate-400">AND</div>}
                          <div className="flex items-center gap-3">
                            <div className="flex-1 flex items-center gap-2">
                              <Select
                                value={condition.leftValueType}
                                onValueChange={(value) =>
                                  updateCondition(action.id, condition.id, "leftValueType", value)
                                }
                              >
                                <SelectTrigger className="w-24 bg-slate-800/50 border-slate-700 text-white h-11 hover:bg-slate-700/70 hover:border-slate-600">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent className="bg-slate-800 border-slate-700">
                                  <SelectItem
                                    value="variable"
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    Var
                                  </SelectItem>
                                  <SelectItem
                                    value="constant"
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    Const
                                  </SelectItem>
                                </SelectContent>
                              </Select>
                              <Input
                                placeholder={condition.leftValueType === "variable" ? "Var Name" : "Const Value"}
                                value={condition.leftValue}
                                onChange={(e) => updateCondition(action.id, condition.id, "leftValue", e.target.value)}
                                className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11"
                              />
                            </div>
                            <Select
                              value={condition.comparisonOperator}
                              onValueChange={(value) =>
                                updateCondition(action.id, condition.id, "comparisonOperator", value)
                              }
                            >
                              <SelectTrigger className="w-20 bg-slate-800/50 border-slate-700 text-white h-11 hover:bg-slate-700/70 hover:border-slate-600">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent className="bg-slate-800 border-slate-700">
                                {comparisonOperators.map((op) => (
                                  <SelectItem
                                    key={op}
                                    value={op}
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    {op}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                            <div className="flex-1 flex items-center gap-2">
                              <Select
                                value={condition.rightValueType}
                                onValueChange={(value) =>
                                  updateCondition(action.id, condition.id, "rightValueType", value)
                                }
                              >
                                <SelectTrigger className="w-24 bg-slate-800/50 border-slate-700 text-white h-11 hover:bg-slate-700/70 hover:border-slate-600">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent className="bg-slate-800 border-slate-700">
                                  <SelectItem
                                    value="variable"
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    Var
                                  </SelectItem>
                                  <SelectItem
                                    value="constant"
                                    className="text-white hover:bg-slate-700 focus:bg-slate-700"
                                  >
                                    Const
                                  </SelectItem>
                                </SelectContent>
                              </Select>
                              <Input
                                placeholder={condition.rightValueType === "variable" ? "Var Name" : "Const Value"}
                                value={condition.rightValue}
                                onChange={(e) => updateCondition(action.id, condition.id, "rightValue", e.target.value)}
                                className="flex-1 bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400 h-11"
                              />
                            </div>
                            {action.conditions.length > 1 && (
                              <Button
                                type="button"
                                variant="destructive"
                                size="sm"
                                onClick={() => removeCondition(action.id, condition.id)}
                                className="h-11 w-11 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 hover:border-red-500/50"
                              >
                                <Minus className="h-4 w-4" />
                              </Button>
                            )}
                          </div>
                        </div>
                      ))}
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => addCondition(action.id)}
                        className="w-full bg-slate-800/50 border-slate-700 text-slate-300 hover:bg-slate-700/70 hover:text-white hover:border-slate-600 h-11"
                      >
                        <Plus className="h-4 w-4 mr-2" />
                        Add Condition
                      </Button>
                    </div>
                  </div>
                ))}
                <Button
                  type="button"
                  onClick={addBuySellAction}
                  className="flex-1 bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-6 py-2.5 h-12 rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] relative overflow-hidden group"
                >
                  <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700"></div>
                  <Plus className="h-4 w-4 mr-2" />
                  <span className="relative z-10">Add Buy/Sell Action</span>
                </Button>
              </div>
            </div>

            {/* Submit Button */}
            <div className="flex justify-center pt-6">
              <Button
                type="submit"
                disabled={isSubmitting}
                className="bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-12 py-3 text-lg h-14 rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] relative overflow-hidden group disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700"></div>
                <span className="relative z-10">{isSubmitting ? "Creating Algorithm..." : "Submit Algorithm"}</span>
              </Button>
            </div>
          </form>
        </div>
      </div>
    </div>
  )
}
