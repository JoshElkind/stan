export default function Blog() {
    return (
      <div className="min-h-screen bg-white">
        {/* Header */}
        <div className="px-65 py-12">
          <h1 className="text-5xl font-bold text-black text-left">
            Guide
          </h1>
        </div>
  
        {/* FAQ Frames */}
        <div className="flex flex-col items-center gap-8 mt-8 pb-6">
          <div className="max-w-2xl">
            <div className="bg-gray-100 rounded-lg p-8">
              <h3 className="text-xl font-semibold text-black mb-4">How do I get started?</h3>
              <div className="text-gray-700 leading-relaxed">
                <p>To get started, go to the Algorithm tab where you can either upload your own pre-made trading algorithm or use our built-in Algorithm Maker to create one from scratch. Once your algorithm is ready, head over to the Evaluate tab to test its performance by entering key metrics such as position length, win percentage, and other relevant parameters. After running the evaluation, you’ll receive a detailed breakdown of your algorithm’s performance to help assess its potential in real trading scenarios. Please ensure that any uploaded algorithms follow our submission guidelines, which are designed to ensure compatibility with our testing system.</p>
              </div>
            </div>
          </div>
          <div className="max-w-2xl">
            <div className="bg-gray-100 rounded-lg p-8">
              <h3 className="text-xl font-semibold text-black mb-4">How do I upload my scripts?</h3>
              <div className="text-gray-700 leading-relaxed">
                <p>To upload your script, go to the Algorithm tab, click the Add Algorithm dropdown, and select Upload Algorithm. Then, choose your Python file to upload.

                <br/> <br/>Make sure your script meets the following requirements: <br/><br/>

1. The file must be a valid Python .py file.<br/><br/>
2. Only the following libraries (imports) are allowed: pandas (import as pd), NumPy (import as np), SciPy (import as scipy), ta (import as ta), Statsmodels (import as statsmodels), math (import as math), and statistics (import as statistics).<br/><br/>
3. The file must contain exactly one function, and the function's name must match the file name (excluding the .py extension).<br/><br/>
4. The function must take a single pandas.DataFrame as input, with the following columns: "date", "open", "high", "low", "close", and "volume".<br/><br/>
5. The function must return a list (or array/numpy.array) of strings, where each string is one of "Hold", "Buy", or "Sell"—representing the action for each minute. The returned list must match the number of rows in the DataFrame, as there needs to be one action per data point.</p><br/>
              </div>
            </div>
          </div>
          <div className="max-w-2xl">
            <div className="bg-gray-100 rounded-lg p-8">
              <h3 className="text-xl font-semibold text-black mb-4">How do I create a new script?</h3>
              <div className="text-gray-700 leading-relaxed">
                <p>To create a new trading algorithm, go to the Algorithms tab and click the "Add Algorithm" dropdown. From there, select "Create Algorithm", and you’ll be directed to a form where you’ll enter your algorithm's name and description. Once submitted, you’ll be able to start designing your strategy. Keep in mind, your algorithm will be tested on millions of rows of historical minute-level stock data with columns: "open", "high", "low", "close", and "volume".

<br /><br />Here’s how the form works:<br /><br />

1. Outer Constants<br />

Start by defining constants that remain fixed throughout the evaluation:

Simple constants: Direct number values like 10 or 3.14.
Expression constants: Math expressions using other variables or constants (e.g., var1 + 8, var9 * 2). For these, specify whether each side of the expression is a constant or a previously defined variable.
<br /><br />2. Row-wise Variables<br />

In this section, you define values calculated at each individual row:

Use row-level data like "open", "high", "low", "close", or "volume" to build new expressions.
You can also define window variables: These are aggregates (like avg, sum, min, max) over a backward-looking range of rows. For each window, you can define inner variables (such as window_close / window_volume) and apply a combining function across that range.
<br /><br />3. Buy/Sell Deciders<br />

Here, you decide at each minute whether to issue a "Buy" or "Sell" signal. Each decision rule (called a decider) can have multiple conditions, which must all be true for that action to trigger. For example, one decider could be:
if (var1 &gt; var2 and var3 &lt; 100) then Buy at that row minute.

You can define as many deciders as needed to cover different trading scenarios.

Once your algorithm is fully defined, hit “Submit Algorithm.” It will be sent to our server for validation. If everything checks out, it will be added to your personal collection of algorithms and ready for evaluation on live historical data.</p>
              </div>
            </div>
          </div>
          <div className="max-w-2xl">
            <div className="bg-gray-100 rounded-lg p-8">
              <h3 className="text-xl font-semibold text-black mb-4">How do I start testing?</h3>
              <div className="text-gray-700 leading-relaxed">
                <p>To begin testing your strategy, go to the Evaluate tab. First, select one or more algorithms—either your own or from the public collection. You can evaluate multiple algorithms at once, which is useful since real-world trading often relies on confirmation from several strategies before making a move.

<br /><br />Once your algorithms are selected, proceed to set your trading metrics. <br /><br />Gain Percentage defines the target profit you're aiming for, while Loss Percentage acts as your stop-loss threshold. <br /><br />Clean Range determines how close conflicting actions (like a “Buy” and “Sell”) can be before they’re filtered out. <br /><br />Intercept Needed sets the number of algorithms that must agree on an action for it to be considered—e.g., if you choose 5 out of 10, a “Buy” is only triggered if 5 or more algorithms agree within a given window. <br /><br />That window is defined by Intercept Range, which controls how many minutes of stock data are scanned for consensus.

Once you've configured these parameters, run the evaluation to see detailed results and gain insights into your strategy’s strengths and weaknesses. If your final score is less than 0, it will appear red and is considered a bad trading setup. If it’s greater than 0, it will appear green, indicating a good trading setup.</p>
              </div>
            </div>
          </div>
          <div className="max-w-2xl">
            <div className="bg-gray-100 rounded-lg p-8">
              <h3 className="text-xl font-semibold text-black mb-4">Where can I find past tests and statistics?</h3>
              <div className="text-gray-700 leading-relaxed">
                <p>You can find your past tests under the Evaluate tab by clicking the "See Past Evaluations" button. This will show a complete history of all your algorithm runs, including the settings used and performance outcomes.

<br /><br />To go a step further, you can visually analyze your evaluations and algorithm performance in the Visualize tab. There, your results are plotted and broken down to help you better understand how your strategies behave over time.</p>
              </div>
            </div>
          </div>
          
        </div>
      </div>
    )
  }
  