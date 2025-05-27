"use client"

import type React from "react"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Plus, Minus, Variable } from "lucide-react"

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

export default function AlgorithmForm() {
  const [algorithmName, setAlgorithmName] = useState("")
  const [outerConstants, setOuterConstants] = useState<OuterConstant[]>([])
  const [rowWiseVariables, setRowWiseVariables] = useState<RowWiseVariable[]>([])
  const [buySellActions, setBuySellActions] = useState<BuySellAction[]>([])

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
      variableName: "",
      expressionLeftType: "variable",
      expressionLeft: "",
      operator: "*",
      expressionRightType: "variable",
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

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const formData = {
      algorithmName,
      outerConstants,
      rowWiseVariables,
      buySellActions,
    }
    // console.log("algorithmName Data:", algorithmName)
    // console.log("outerConstants Data:", outerConstants)
    // console.log("outerConstants Length: ", outerConstants.length)
    // console.log("rowWiseVariables Data:", rowWiseVariables)
    console.log("rowWiseVariables Data:", rowWiseVariables)
    alert("Algorithm form submitted successfully!")

    let outer_const_send = []

    for (let i = 0; i < outerConstants.length; i++) {
      const curr_const = outerConstants[i]
      let single_send = []
      let Variable_name = curr_const.variableName
      let Variable_type = curr_const.valueType // can be either constant or var...
      let Variable_data = []

      if (Variable_type == "constant") {
        single_send.push(0)
        single_send.push(Variable_name)
        single_send.push(curr_const.constantValue)
        outer_const_send.push(single_send)

      } else {
        single_send.push(1)
        single_send.push(Variable_name)
        let data_expression = []
        let bools_type = []
        let expression_three = []
        if (curr_const.expressionLeftType == "constant") {
          bools_type.push(0)
          expression_three.push(curr_const.expressionLeft)
        } else {
          bools_type.push(1)
          expression_three.push(curr_const.expressionLeft)
        }
        expression_three.push(curr_const.operator)
        if (curr_const.expressionRightType == "constant") {
          bools_type.push(0)
          expression_three.push(curr_const.expressionRight)
        } else {
          bools_type.push(1)
          expression_three.push(curr_const.expressionRight)
        }
        data_expression.push(bools_type)
        data_expression.push(expression_three)
        single_send.push(data_expression)
        outer_const_send.push(single_send)
      }
    }

    // ************************************


    let row_wise_send = []

    for (let i = 0; i < rowWiseVariables.length; i++) {

    }

















    // ************************************

    let buy_sell_send = []

    for (let i = 0; i < buySellActions.length; i++) {
      let curr_action = buySellActions[i]
      let single_send = []
      single_send.push(curr_action.action)
      let arr_and_conds = []
      let all_conds = curr_action.conditions
      for (let j = 0; j < all_conds.length; j++) {
        let curr_cond_single_sub = []
        let curr_cond = all_conds[j]
        let curr_cond_bool = []
        let curr_cond_three = []
        if (curr_cond.leftValueType == "constant") {
          curr_cond_bool.push(0)
          curr_cond_three.push(curr_cond.leftValue)
        } else {
          curr_cond_bool.push(1)
          curr_cond_three.push(curr_cond.leftValue)
        }
        curr_cond_three.push(curr_cond.comparisonOperator)
        if (curr_cond.rightValueType == "constant") {
          curr_cond_bool.push(0)
          curr_cond_three.push(curr_cond.rightValue)
        } else {
          curr_cond_bool.push(1)
          curr_cond_three.push(curr_cond.rightValue)
        }
        curr_cond_single_sub.push(curr_cond_bool)
        curr_cond_single_sub.push(curr_cond_three)
        arr_and_conds.push(curr_cond_single_sub)
      }
      single_send.push(all_conds)
      buy_sell_send.push(single_send)
    }

    let form_result = []
    form_result.push(outer_const_send)
    form_result.push(row_wise_send)
    form_result.push(buy_sell_send)
    form_result.push("FILL IN USER ID HERE")
    form_result.push(algorithmName)



  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
      <div className="max-w-5xl mx-auto p-8 space-y-10">
        <form onSubmit={handleSubmit} className="space-y-10">
          {/* Section 1: Algorithm Name */}
          <div className="space-y-6">
            <h2 className="text-xl font-semibold text-slate-300 tracking-wide">Trading Algorithm Name</h2>
            <div className="space-y-3">
              <Input
                id="algorithmName"
                value={algorithmName}
                onChange={(e) => setAlgorithmName(e.target.value)}
                placeholder="enter name"
                required
                className="bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 focus:ring-teal-400/20 h-12 text-lg backdrop-blur-sm"
              />
            </div>
          </div>

          {/* Divider */}
          <div className="h-px bg-gradient-to-r from-transparent via-slate-600 to-transparent"></div>

          {/* Section 2: Outer Constant Values */}
          <div className="space-y-6">
            <h2 className="text-xl font-semibold text-slate-300 tracking-wide">Outer Constant Values</h2>
            <div className="space-y-5">
              {outerConstants.map((constant) => (
                <div
                  key={constant.id}
                  className="border border-slate-700/50 rounded-xl p-6 space-y-4 bg-slate-800/40 backdrop-blur-sm shadow-xl"
                >
                  <div className="flex justify-start">
                    <Select
                      value={constant.valueType}
                      onValueChange={(value) => updateOuterConstant(constant.id, "valueType", value)}
                    >
                      <SelectTrigger className="w-44 font-bold border-slate-600 bg-slate-800/70 text-slate-100 h-11 hover:bg-slate-700 hover:border-slate-500">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent className="bg-slate-800 border-slate-600">
                        <SelectItem value="constant" className="text-slate-100 focus:bg-slate-700">
                          Constant
                        </SelectItem>
                        <SelectItem value="expression" className="text-slate-100 focus:bg-slate-700">
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
                      className="flex-1 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11"
                    />
                    <span className="text-xl font-mono text-slate-300">:</span>
                    <div className="flex-[2]">
                      {constant.valueType === "constant" ? (
                        <Input
                          placeholder="Const Value"
                          value={constant.constantValue}
                          onChange={(e) => updateOuterConstant(constant.id, "constantValue", e.target.value)}
                          className="bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11"
                        />
                      ) : (
                        <div className="flex items-center gap-3">
                          <div className="flex items-center gap-2 flex-1">
                            <Select
                              value={constant.expressionLeftType}
                              onValueChange={(value) => updateOuterConstant(constant.id, "expressionLeftType", value)}
                            >
                              <SelectTrigger className="w-20 bg-slate-800/70 border-slate-600 text-slate-100 h-11 hover:bg-slate-700 hover:border-slate-500">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent className="bg-slate-800 border-slate-600">
                                <SelectItem value="variable" className="text-slate-100 focus:bg-slate-700">
                                  Var
                                </SelectItem>
                                <SelectItem value="constant" className="text-slate-100 focus:bg-slate-700">
                                  Const
                                </SelectItem>
                              </SelectContent>
                            </Select>
                            <Input
                              placeholder={constant.expressionLeftType === "variable" ? "Var Name" : "Const Value"}
                              value={constant.expressionLeft}
                              onChange={(e) => updateOuterConstant(constant.id, "expressionLeft", e.target.value)}
                              className="flex-1 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11"
                            />
                          </div>
                          <Select
                            value={constant.operator}
                            onValueChange={(value) => updateOuterConstant(constant.id, "operator", value)}
                          >
                            <SelectTrigger className="w-20 bg-slate-800/70 border-slate-600 text-slate-100 h-11 hover:bg-slate-700 hover:border-slate-500">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent className="bg-slate-800 border-slate-600">
                              {operators.map((op) => (
                                <SelectItem key={op} value={op} className="text-slate-100 focus:bg-slate-700">
                                  {op}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                          <div className="flex items-center gap-2 flex-1">
                            <Select
                              value={constant.expressionRightType}
                              onValueChange={(value) => updateOuterConstant(constant.id, "expressionRightType", value)}
                            >
                              <SelectTrigger className="w-20 bg-slate-800/70 border-slate-600 text-slate-100 h-11 hover:bg-slate-700 hover:border-slate-500">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent className="bg-slate-800 border-slate-600">
                                <SelectItem value="variable" className="text-slate-100 focus:bg-slate-700">
                                  Var
                                </SelectItem>
                                <SelectItem value="constant" className="text-slate-100 focus:bg-slate-700">
                                  Const
                                </SelectItem>
                              </SelectContent>
                            </Select>
                            <Input
                              placeholder={constant.expressionRightType === "variable" ? "Var Name" : "Const Value"}
                              value={constant.expressionRight}
                              onChange={(e) => updateOuterConstant(constant.id, "expressionRight", e.target.value)}
                              className="flex-1 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11"
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
                      className="h-11 w-11 bg-red-600/80 hover:bg-red-600 border-red-500/50"
                    >
                      <Minus className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              ))}
              <Button
                type="button"
                onClick={addOuterConstant}
                className="w-full bg-teal-600/80 hover:bg-teal-500 text-slate-100 border border-slate-600 h-12 backdrop-blur-sm"
              >
                <Plus className="h-4 w-4 mr-2" />
                Add Outer Constant
              </Button>
            </div>
          </div>

          {/* Divider */}
          <div className="h-px bg-gradient-to-r from-transparent via-slate-600 to-transparent"></div>

          {/* Section 3: Row Wise Variables */}
          <div className="space-y-6">
            <h2 className="text-xl font-semibold text-slate-300 tracking-wide">Row Wise Variables</h2>
            <div className="flex items-center gap-2 flex-wrap">
              <span className="bg-teal-600/20 border border-teal-500/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm font-medium">
                Available Row Values:
              </span>
              <span className="bg-teal-600/20 border border-teal-500/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm">
                Open
              </span>
              <span className="bg-teal-600/20 border border-teal-500/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm">
                High
              </span>
              <span className="bg-teal-600/20 border border-teal-500/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm">
                Low
              </span>
              <span className="bg-teal-600/20 border border-teal-500/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm">
                Close
              </span>
              <span className="bg-teal-600/20 border border-teal-500/30 px-3 py-1.5 rounded-lg text-teal-200 text-sm">
                Volume
              </span>
            </div>
            <div className="space-y-5">
              {rowWiseVariables.map((variable) => (
                <div
                  key={variable.id}
                  className="border border-slate-700/50 rounded-xl p-6 space-y-5 bg-slate-800/40 backdrop-blur-sm shadow-xl"
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
                            ? "bg-teal-600 hover:bg-teal-500 text-white border-2 border-teal-400 shadow-lg h-10"
                            : "bg-slate-700/50 border-slate-600 text-slate-300 hover:bg-slate-600 hover:text-slate-100 hover:border-slate-500 h-10"
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
                            ? "bg-teal-600 hover:bg-teal-500 text-white border-2 border-teal-400 shadow-lg h-10"
                            : "bg-slate-700/50 border-slate-600 text-slate-300 hover:bg-slate-600 hover:text-slate-100 hover:border-slate-500 h-10"
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
                      className="h-10 w-10 bg-red-600/80 hover:bg-red-600 border-red-500/50"
                    >
                      <Minus className="h-4 w-4" />
                    </Button>
                  </div>
                  {variable.type === "window" && (
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="bg-cyan-600/20 border border-cyan-500/30 px-2 py-1 rounded-lg text-cyan-200 text-xs font-medium">
                        Available Window Row Values:
                      </span>
                      <span className="bg-cyan-600/20 border border-cyan-500/30 px-2 py-1 rounded-lg text-cyan-200 text-xs">
                        Window_Open
                      </span>
                      <span className="bg-cyan-600/20 border border-cyan-500/30 px-2 py-1 rounded-lg text-cyan-200 text-xs">
                        Window_High
                      </span>
                      <span className="bg-cyan-600/20 border border-cyan-500/30 px-2 py-1 rounded-lg text-cyan-200 text-xs">
                        Window_Low
                      </span>
                      <span className="bg-cyan-600/20 border border-cyan-500/30 px-2 py-1 rounded-lg text-cyan-200 text-xs">
                        Window_Close
                      </span>
                      <span className="bg-cyan-600/20 border border-cyan-500/30 px-2 py-1 rounded-lg text-cyan-200 text-xs">
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
                        className="w-24 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11"
                      />
                      <span className="text-xl font-mono text-slate-300">:</span>
                      <div className="flex items-center gap-2 flex-1">
                        <Select
                          value={variable.expressionLeftType}
                          onValueChange={(value) => updateRowWiseVariable(variable.id, "expressionLeftType", value)}
                        >
                          <SelectTrigger className="w-20 bg-slate-800/70 border-slate-600 text-slate-100 h-11 hover:bg-slate-700 hover:border-slate-500">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent className="bg-slate-800 border-slate-600">
                            <SelectItem value="variable" className="text-slate-100 focus:bg-slate-700">
                              Var
                            </SelectItem>
                            <SelectItem value="constant" className="text-slate-100 focus:bg-slate-700">
                              Const
                            </SelectItem>
                          </SelectContent>
                        </Select>
                        <Input
                          placeholder={variable.expressionLeftType === "variable" ? "Var Name" : "Const Value"}
                          value={variable.expressionLeft}
                          onChange={(e) => updateRowWiseVariable(variable.id, "expressionLeft", e.target.value)}
                          className="flex-1 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11"
                        />
                      </div>
                      <Select
                        value={variable.operator}
                        onValueChange={(value) => updateRowWiseVariable(variable.id, "operator", value)}
                      >
                        <SelectTrigger className="w-20 bg-slate-800/70 border-slate-600 text-slate-100 h-11 hover:bg-slate-700 hover:border-slate-500">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="bg-slate-800 border-slate-600">
                          {operators.map((op) => (
                            <SelectItem key={op} value={op} className="text-slate-100 focus:bg-slate-700">
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
                          <SelectTrigger className="w-20 bg-slate-800/70 border-slate-600 text-slate-100 h-11 hover:bg-slate-700 hover:border-slate-500">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent className="bg-slate-800 border-slate-600">
                            <SelectItem value="variable" className="text-slate-100 focus:bg-slate-700">
                              Var
                            </SelectItem>
                            <SelectItem value="constant" className="text-slate-100 focus:bg-slate-700">
                              Const
                            </SelectItem>
                          </SelectContent>
                        </Select>
                        <Input
                          placeholder={variable.expressionRightType === "variable" ? "Var Name" : "Const Value"}
                          value={variable.expressionRight}
                          onChange={(e) => updateRowWiseVariable(variable.id, "expressionRight", e.target.value)}
                          className="flex-1 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11"
                        />
                      </div>
                    </div>
                  ) : (
                    <div className="space-y-4">
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                        <div>
                          <Label className="text-sm text-slate-300 font-medium">Combining Function</Label>
                          <Select
                            value={variable.combiningFunction}
                            onValueChange={(value) => updateRowWiseVariable(variable.id, "combiningFunction", value)}
                          >
                            <SelectTrigger className="bg-slate-800/70 border-slate-600 text-slate-100 h-11 mt-1 hover:bg-slate-700 hover:border-slate-500">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent className="bg-slate-800 border-slate-600">
                              {combiningFunctions.map((func) => (
                                <SelectItem key={func} value={func} className="text-slate-100 focus:bg-slate-700">
                                  {func}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div>
                          <Label className="text-sm text-slate-300 font-medium">Extract Variable</Label>
                          <Input
                            placeholder="Name"
                            value={variable.windowVariableName}
                            onChange={(e) => updateRowWiseVariable(variable.id, "windowVariableName", e.target.value)}
                            className="bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11 mt-1"
                          />
                        </div>
                        <div>
                          <Label className="text-sm text-slate-300 font-medium">Window Start</Label>
                          <Input
                            type="number"
                            min="1"
                            placeholder="Start"
                            value={variable.windowStart}
                            onChange={(e) => updateRowWiseVariable(variable.id, "windowStart", e.target.value)}
                            className="bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11 mt-1"
                          />
                        </div>
                        <div>
                          <Label className="text-sm text-slate-300 font-medium">Window End</Label>
                          <Input
                            type="number"
                            min="1"
                            placeholder="End"
                            value={variable.windowEnd}
                            onChange={(e) => updateRowWiseVariable(variable.id, "windowEnd", e.target.value)}
                            className="bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11 mt-1"
                          />
                        </div>
                      </div>

                      <div className="space-y-3">
                        <div className="flex items-center justify-between">
                          <Label className="text-sm font-medium text-slate-300">Inner Window Variables</Label>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={() => addInnerVariable(variable.id)}
                            className="bg-slate-700/50 border-slate-600 text-slate-300 hover:bg-slate-600 hover:text-slate-100 hover:border-slate-500 h-9"
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
                                onChange={(e) => updateInnerVariable(variable.id, innerVar.id, "name", e.target.value)}
                                className="flex-1 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-10"
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
                                <SelectTrigger className="w-16 bg-slate-800/70 border-slate-600 text-slate-100 h-10 hover:bg-slate-700 hover:border-slate-500">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent className="bg-slate-800 border-slate-600">
                                  <SelectItem value="variable" className="text-slate-100 focus:bg-slate-700">
                                    Var
                                  </SelectItem>
                                  <SelectItem value="constant" className="text-slate-100 focus:bg-slate-700">
                                    Const
                                  </SelectItem>
                                </SelectContent>
                              </Select>
                              <Input
                                placeholder={innerVar.leftType === "variable" ? "Var Name" : "Const Value"}
                                value={innerVar.left}
                                onChange={(e) => updateInnerVariable(variable.id, innerVar.id, "left", e.target.value)}
                                className="flex-1 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-10"
                              />
                            </div>
                            <Select
                              value={innerVar.operator}
                              onValueChange={(value) =>
                                updateInnerVariable(variable.id, innerVar.id, "operator", value)
                              }
                            >
                              <SelectTrigger className="w-20 bg-slate-800/70 border-slate-600 text-slate-100 h-10 hover:bg-slate-700 hover:border-slate-500">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent className="bg-slate-800 border-slate-600">
                                {operators.map((op) => (
                                  <SelectItem key={op} value={op} className="text-slate-100 focus:bg-slate-700">
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
                                <SelectTrigger className="w-16 bg-slate-800/70 border-slate-600 text-slate-100 h-10 hover:bg-slate-700 hover:border-slate-500">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent className="bg-slate-800 border-slate-600">
                                  <SelectItem value="variable" className="text-slate-100 focus:bg-slate-700">
                                    Var
                                  </SelectItem>
                                  <SelectItem value="constant" className="text-slate-100 focus:bg-slate-700">
                                    Const
                                  </SelectItem>
                                </SelectContent>
                              </Select>
                              <Input
                                placeholder={innerVar.rightType === "variable" ? "Var Name" : "Const Value"}
                                value={innerVar.right}
                                onChange={(e) => updateInnerVariable(variable.id, innerVar.id, "right", e.target.value)}
                                className="flex-1 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-10"
                              />
                            </div>
                            <Button
                              type="button"
                              variant="destructive"
                              size="sm"
                              onClick={() => removeInnerVariable(variable.id, innerVar.id)}
                              className="h-10 w-10 bg-red-600/80 hover:bg-red-600 border-red-500/50"
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
                  className="flex-1 bg-teal-600 hover:bg-teal-500 text-white h-12 backdrop-blur-sm border border-teal-500/30"
                >
                  <Plus className="h-4 w-4 mr-2" />
                  Add Expression Variable
                </Button>
                <Button
                  type="button"
                  onClick={() => addRowWiseVariable("window")}
                  className="flex-1 bg-teal-600 hover:bg-teal-500 text-white h-12 backdrop-blur-sm border border-teal-500/30"
                >
                  <Plus className="h-4 w-4 mr-2" />
                  Add Window Value Variable
                </Button>
              </div>
            </div>
          </div>

          {/* Divider */}
          <div className="h-px bg-gradient-to-r from-transparent via-slate-600 to-transparent"></div>

          {/* Section 4: Buy/Sell Actions */}
          <div className="space-y-6">
            <h2 className="text-xl font-semibold text-slate-300 tracking-wide">Buy/Sell Actions</h2>
            <div className="space-y-5">
              {buySellActions.map((action) => (
                <div
                  key={action.id}
                  className="border border-slate-700/50 rounded-xl p-6 space-y-4 bg-slate-800/40 backdrop-blur-sm shadow-xl"
                >
                  <div className="flex items-center justify-between">
                    <Select
                      value={action.action}
                      onValueChange={(value) => updateBuySellAction(action.id, "action", value)}
                    >
                      <SelectTrigger
                        className={`w-36 font-bold h-11 hover:border-opacity-70 ${
                          action.action === "buy"
                            ? "bg-green-600/20 border-green-500/50 text-green-200 hover:bg-green-600/30 hover:border-green-400"
                            : "bg-red-600/20 border-red-500/50 text-red-200 hover:bg-red-600/30 hover:border-red-400"
                        }`}
                      >
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent className="bg-slate-800 border-slate-600">
                        <SelectItem value="buy" className="text-green-400 focus:bg-slate-700">
                          Buy
                        </SelectItem>
                        <SelectItem value="sell" className="text-red-400 focus:bg-slate-700">
                          Sell
                        </SelectItem>
                      </SelectContent>
                    </Select>
                    <Button
                      type="button"
                      variant="destructive"
                      size="sm"
                      onClick={() => removeBuySellAction(action.id)}
                      className="h-11 w-11 bg-red-600/80 hover:bg-red-600 border-red-500/50"
                    >
                      <Minus className="h-4 w-4" />
                    </Button>
                  </div>

                  <div className="space-y-3">
                    <Label className="text-sm font-medium text-slate-300">Conditions</Label>
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
                              <SelectTrigger className="w-24 bg-slate-800/70 border-slate-600 text-slate-100 h-11 hover:bg-slate-700 hover:border-slate-500">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent className="bg-slate-800 border-slate-600">
                                <SelectItem value="variable" className="text-slate-100 focus:bg-slate-700">
                                  Var
                                </SelectItem>
                                <SelectItem value="constant" className="text-slate-100 focus:bg-slate-700">
                                  Const
                                </SelectItem>
                              </SelectContent>
                            </Select>
                            <Input
                              placeholder={condition.leftValueType === "variable" ? "Var Name" : "Const Value"}
                              value={condition.leftValue}
                              onChange={(e) => updateCondition(action.id, condition.id, "leftValue", e.target.value)}
                              className="flex-1 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11"
                            />
                          </div>
                          <Select
                            value={condition.comparisonOperator}
                            onValueChange={(value) =>
                              updateCondition(action.id, condition.id, "comparisonOperator", value)
                            }
                          >
                            <SelectTrigger className="w-20 bg-slate-800/70 border-slate-600 text-slate-100 h-11 hover:bg-slate-700 hover:border-slate-500">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent className="bg-slate-800 border-slate-600">
                              {comparisonOperators.map((op) => (
                                <SelectItem key={op} value={op} className="text-slate-100 focus:bg-slate-700">
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
                              <SelectTrigger className="w-24 bg-slate-800/70 border-slate-600 text-slate-100 h-11 hover:bg-slate-700 hover:border-slate-500">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent className="bg-slate-800 border-slate-600">
                                <SelectItem value="variable" className="text-slate-100 focus:bg-slate-700">
                                  Var
                                </SelectItem>
                                <SelectItem value="constant" className="text-slate-100 focus:bg-slate-700">
                                  Const
                                </SelectItem>
                              </SelectContent>
                            </Select>
                            <Input
                              placeholder={condition.rightValueType === "variable" ? "Var Name" : "Const Value"}
                              value={condition.rightValue}
                              onChange={(e) => updateCondition(action.id, condition.id, "rightValue", e.target.value)}
                              className="flex-1 bg-slate-800/50 border-slate-600 text-slate-100 placeholder:text-slate-400 focus:border-teal-400 h-11"
                            />
                          </div>
                          {action.conditions.length > 1 && (
                            <Button
                              type="button"
                              variant="destructive"
                              size="sm"
                              onClick={() => removeCondition(action.id, condition.id)}
                              className="h-11 w-11 bg-red-600/80 hover:bg-red-600 border-red-500/50"
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
                      className="w-full bg-slate-700/50 border-slate-600 text-slate-300 hover:bg-slate-600 hover:text-slate-100 hover:border-slate-500 h-11"
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
                className="flex-1 bg-teal-600 hover:bg-teal-500 text-white h-12 backdrop-blur-sm border border-teal-500/30"
              >
                <Plus className="h-4 w-4 mr-2" />
                Add Buy/Sell Action
              </Button>
            </div>
          </div>

          {/* Submit Button */}
          <div className="flex justify-center pt-6">
            <Button
              type="submit"
              className="bg-teal-600 hover:bg-teal-500 text-white px-12 py-3 text-lg h-14 backdrop-blur-sm border border-teal-500/30 shadow-lg"
            >
              Submit Algorithm
            </Button>
          </div>
        </form>
      </div>
    </div>
  )
}
