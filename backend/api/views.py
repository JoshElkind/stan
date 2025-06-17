from django.shortcuts import render
from rest_framework import generics, views, status
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated

from .models import PastEvals, UserSignature
from .serializes import PastEvalsSerializer
import pandas as pd
import numpy as np
import math
import ast
from django.conf import settings
from datetime import date
import boto3
import uuid

from .k8s_runner.test_algo import call_assets  


from .pro_algo import generate_algorithm_code
import os
from rest_framework.parsers import MultiPartParser
from dotenv import load_dotenv
load_dotenv()
from datetime import datetime
import pandas as pd
import numpy as np
import math
import statistics
import scipy
import statsmodels.api as sm
import statsmodels
import ta
# from backend.k8s_jobs.k8s_runner import run_user_algo_k8s

def contains_function_def(code: str, func_name: str) -> bool:
    try:
        tree = ast.parse(code)
        return any(isinstance(node, ast.FunctionDef) and node.name == func_name for node in tree.body)
    except SyntaxError:
        return False


class PastEvalsCreate(generics.ListCreateAPIView):
    queryset = PastEvals.objects.all()
    serializer_class = PastEvalsSerializer
    permission_classes = [IsAuthenticated]

class PastEvalsView(generics.ListAPIView):
    serializer_class = PastEvalsSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return PastEvals.objects.filter(user=self.request.user)

class ListUserAlgorithms(views.APIView):
    print("1authenticate check")
    permission_classes = [IsAuthenticated]
    print("2authenticate check")

    def get(self, request):
        sig, created = UserSignature.objects.get_or_create(user=request.user)

        formatted_algorithms = [
            {
                "algoname": algo.get("algoname"),
                "description": algo.get("summary"),
                "date_added": algo.get("date_added")
            }
            for algo in sig.algorithms
        ]
        return Response({"algorithms": formatted_algorithms})

class AddUserAlgorithm(views.APIView):
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        try:
            sig = UserSignature.objects.get(user=request.user)
            algo_name = request.data.get("algoname")
            algo_description = request.data.get("algodescription")
            outer_consts = request.data.get("outer_consts", [])
            row_wise_consts = request.data.get("row_wise_consts", [])
            deciders = request.data.get("deciders", [])
            
            # check for duplicate algorithm names
            public_curr = [
                "moving_average_crossover", "rsi_mean_reversion", "bollinger_bands",
                "donchian_breakout", "macd_crossover", "obv_strategy",
                "stochastic_oscillator", "adx_trend", "momentum_strategy", "volume_spike"
            ]
            
            if any(a["algoname"] == algo_name for a in sig.algorithms) or algo_name in public_curr:
                return Response({"error": "Algorithm with this name already exists."}, status=400)
            
            # make the function code
            function_code = generate_algorithm_code(
                outer_consts, row_wise_consts, deciders, request.user.id, algo_name, algo_name
            )
            
            # validate
            try:
                compile(function_code, filename=algo_name, mode="exec")
            except SyntaxError as e:
                return Response({"error": f"Invalid Python code generated: {str(e)}"}, status=400)
            
          
            try:
                tree = ast.parse(function_code)
                if not any(isinstance(n, ast.FunctionDef) and n.name == algo_name for n in tree.body):
                    return Response({"error": f"No function named '{algo_name}' found in generated code."}, status=400)
            except Exception:
                return Response({"error": "Failed to parse generated Python code."}, status=400)
            
            # Load test data for validation
            current_dir = os.path.dirname(__file__)
            test_data_path = os.path.join(current_dir, "data", "test_data.csv")
            if not os.path.exists(test_data_path):
                return Response({"error": "Test data file not found."}, status=500)
            df = pd.read_csv(test_data_path)
            
            # Execute generated function with built-in libraries available
            local_ns = {}
            global_env = {
                "pd": pd,
                "np": np,
                "math": math,
                "statistics": statistics,
                "scipy": scipy,
                "sm": sm,
                "ta": ta,
            }
            exec(function_code, global_env, local_ns)
            user_func = local_ns.get(algo_name)
            if not callable(user_func):
                return Response({"error": f"'{algo_name}' is not a callable function."}, status=400)
            
            # test the function execution
            result = user_func(df)
            
            if not isinstance(result, (list, np.ndarray)):
                return Response({"error": "Return must be list or numpy array."}, status=400)
            
            if len(result) != len(df):
                return Response({"error": f"Return length {len(result)} must match input rows {len(df)}."}, status=400)
            
            allowed = {"Buy", "Sell", "Hold"}
            if not all(str(x) in allowed for x in result):
                return Response({"error": "Return values must be 'Buy', 'Sell', or 'Hold' only."}, status=400)
            
            # upload to S3
            filename = f"{algo_name}.py"
            s3 = boto3.client('s3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION_NAME")
            )
            bucket_name = os.getenv("AWS_STORAGE_BUCKET_NAME")
            key = f"{sig.placeholder}{filename}"
            s3.put_object(Bucket=bucket_name, Key=key, Body=function_code.encode("utf-8"))
            
            # add to user's algorithms list via the MySql django project-local db
            sig.algorithms.append({
                "algoname": algo_name,
                "summary": algo_description,
                "date_added": datetime.utcnow().isoformat()
            })
            sig.save()
            
            return Response({"status": "Algorithm added and tested successfully", "s3_key": key})
            
        except Exception as e:
            return Response({"error": str(e)}, status=500)
        
class UploadUserAlgorithm(views.APIView):
    parser_classes = [MultiPartParser]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            file = request.FILES.get("file")
            algo_name = request.data.get("algoname", "").replace(".py", "")
            algo_description = request.data.get("algodescription")

            if not file or not algo_name:
                return Response({"error": "File or algorithm name missing"}, status=400)

            sig = UserSignature.objects.get(user=request.user)

            public_curr = [
                "moving_average_crossover", "rsi_mean_reversion", "bollinger_bands",
                "donchian_breakout", "macd_crossover", "obv_strategy",
                "stochastic_oscillator", "adx_trend", "momentum_strategy", "volume_spike"
            ]

            if any(a["algoname"] == algo_name for a in sig.algorithms) or algo_name in public_curr:
                return Response({"error": "Algorithm with this name already exists."}, status=400)

            # read and validate code
            raw_code = file.read().decode("utf-8")
            try:
                compile(raw_code, filename=algo_name, mode="exec")
            except SyntaxError as e:
                return Response({"error": f"Invalid Python file: {str(e)}"}, status=400)

            # check if function with expected name exists
            try:
                tree = ast.parse(raw_code)
                if not any(isinstance(n, ast.FunctionDef) and n.name == algo_name for n in tree.body):
                    return Response({"error": f"No function named '{algo_name}' found in script."}, status=400)
            except Exception:
                return Response({"error": "Failed to parse Python file."}, status=400)

            current_dir = os.path.dirname(__file__)
            test_data_path = os.path.join(current_dir, "data", "test_data.csv")
            if not os.path.exists(test_data_path):
                return Response({"error": "Test data file not found."}, status=500)
            df = pd.read_csv(test_data_path)

            # execute uploaded function with built-in libraries available
            local_ns = {}
            global_env = {
                "pd": pd,
                "np": np,
                "math": math,
                "statistics": statistics,
                "scipy": scipy,
                "sm": sm,
                "ta": ta,
            }
            exec(raw_code, global_env, local_ns)
            user_func = local_ns.get(algo_name)
            if not callable(user_func):
                return Response({"error": f"'{algo_name}' is not a callable function."}, status=400)

            result = user_func(df)

            if not isinstance(result, (list, np.ndarray)):
                return Response({"error": "Return must be list or numpy array."}, status=400)

            if len(result) != len(df):
                return Response({"error": f"Return length {len(result)} must match input rows {len(df)}."}, status=400)

            allowed = {"Buy", "Sell", "Hold"}
            if not all(str(x) in allowed for x in result):
                return Response({"error": "Return values must be 'Buy', 'Sell', or 'Hold' only."}, status=400)

            # upload validated code to S3
            s3 = boto3.client(
                's3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION_NAME")
            )
            bucket = os.getenv("AWS_STORAGE_BUCKET_NAME")
            key = f"{sig.placeholder}{algo_name}.py"
            file.seek(0)
            s3.upload_fileobj(file, Bucket=bucket, Key=key)

            sig.algorithms.append({
                "algoname": algo_name,
                "summary": algo_description,
                "date_added": datetime.utcnow().isoformat()
            })
            sig.save()

            return Response({"status": "Uploaded and executed successfully", "s3_key": key})

        except Exception as e:
            return Response({"error": str(e)}, status=500)
        
class ListPublicAlgorithms(views.APIView):
    permission_classes = [IsAuthenticated]
    def get(self, request):
        return Response({
            "algorithms": [
    {
        "algoname": "moving_average_crossover",
        "summary": "This is a classic trend-following strategy that uses two simple moving averages to identify trend changes. It calculates a short-term moving average and a long-term moving average of the closing price (for example, 20-period vs. 50-period). A Buy signal is generated when the short-term average crosses above the long-term average (bullish crossover, indicating upward momentum). Conversely, a Sell signal occurs when the short-term average crosses below the long-term average (bearish crossover, indicating a downtrend). If no crossover occurs on a given period, the function returns Hold."
    },
    {
        "algoname": "rsi_mean_reversion",
        "summary": "A contrarian mean-reversion strategy that uses the Relative Strength Index (RSI) momentum oscillator. RSI values range from 0 to 100 and typically, RSI > 70 indicates an overbought market and RSI < 30 indicates an oversold market. This algorithm generates a Buy signal when the RSI rises back above 30 after being below that threshold (signaling the stock was oversold and may rebound). Similarly, it issues a Sell signal when the RSI falls below 70 after being above that level (indicating an overbought condition is reverting downward). At all other times (when RSI is in the neutral range or no crossing of these levels occurs), it returns Hold."
    },
    {
        "algoname": "bollinger_bands",
        "summary": "A mean-reversion strategy based on Bollinger Bands, which consist of a moving average and volatility bands above and below it (typically set at 2 standard deviations). This function computes a 20-period moving average of the closing price and its corresponding upper and lower bands. A Buy signal is produced when the price was below the lower Bollinger Band and then crosses back above it, indicating a rebound from an oversold condition. Conversely, a Sell signal is generated when the price was above the upper band and then drops back below it, indicating a reversal from an overbought condition. If the price is within the bands (no extreme condition), the signal is Hold."
    },
    {
        "algoname": "donchian_breakout",
        "summary": "A breakout strategy based on Donchian channels, which track the recent high and low range. This function looks at a 20-period window to identify breakouts from the recent trading range. If the current closing price breaks above the highest high of the previous 20 periods, it issues a Buy signal (indicating a bullish breakout). If the closing price breaks below the lowest low of the previous 20 periods, it issues a Sell signal (indicating a bearish breakout). When the price remains within the recent 20-period high-low range (no breakout), the signal is Hold."
    },
    {
        "algoname": "macd_crossover",
        "summary": "A momentum strategy using the MACD (Moving Average Convergence Divergence) indicator. MACD is calculated as the difference between a fast exponential moving average and a slow exponential moving average of the closing price (commonly 12-period EMA and 26-period EMA), and it uses a 9-period EMA as a signal line. This function generates a Buy signal when the MACD line crosses above its signal line, indicating bullish momentum. A Sell signal is generated when the MACD line crosses below the signal line, indicating bearish momentum. If the lines do not cross on that period, the output is Hold."
    },
    {
        "algoname": "obv_strategy",
        "summary": "A volume-based strategy that uses On-Balance Volume (OBV) to confirm price trends. OBV is a running total of volume that adds volume on days when price rises and subtracts volume on days when price falls, gauging buying or selling pressure. This function computes the OBV series and then a short moving average of OBV (to smooth it). A Buy signal is generated when OBV crosses above its moving average, indicating increasing volume on upward moves (confirming a bullish trend). A Sell signal is generated when OBV falls below its moving average, indicating volume confirming a bearish move. If no cross occurs, the signal is Hold."
    },
    {
        "algoname": "stochastic_oscillator",
        "summary": "A strategy based on the Stochastic Oscillator, which compares the current price to its range over a recent period (commonly 14 periods). The oscillator produces %K (fast line) and %D (slow line, which is a 3-period moving average of %K). This function issues a Buy signal when %K crosses above %D while in the oversold zone (below 20), indicating upward momentum from a low-price extreme. Conversely, it gives a Sell signal when %K crosses below %D while in the overbought zone (above 80), indicating a downward turn from a high-price extreme. In all other cases (no such cross or in neutral ranges), the signal is Hold."
    },
    {
        "algoname": "adx_trend",
        "summary": "A trend-following strategy that uses the Average Directional Index (ADX) to identify strong trends and the Directional Movement Index (+DI and -DI) for trend direction. ADX values above 25 typically indicate a strong trending market. This function generates a Buy signal when a strong uptrend is detected: ADX is above 25 and the +DI line crosses above the -DI line (meaning bullish trend strength is rising). It generates a Sell signal when ADX > 25 and the -DI crosses above the +DI (indicating a strong downtrend). If the trend is weak (ADX low) or no cross in DI lines occurs, the output is Hold."
    },
    {
        "algoname": "momentum_strategy",
        "summary": "A simple momentum strategy that measures recent price change over a short window and acts if the change is significant. In this implementation, the algorithm looks at a 10-minute window price change (percentage gain or loss). If the current price is more than a certain threshold above its price 10 periods ago (for example, >1% increase in 10 minutes), it generates a Buy signal on the assumption that upward momentum will continue. If the price is more than the negative threshold below its level 10 periods ago (e.g., >1% drop), it generates a Sell signal to follow the downward momentum. If the price change is within ±1% over the last 10 minutes (no strong momentum), the output is Hold."
    },
    {
        "algoname": "volume_spike",
        "summary": "A volume-based strategy that detects unusual volume spikes coupled with price movement, often signaling breakouts or reversals. This algorithm compares the current volume to the average volume of the last 20 periods. If the current volume is exceptionally high (for example, more than 2 times the recent average) and the price is rising compared to the previous bar, it issues a Buy signal on the premise that a high-volume up-move indicates a bullish breakout. If the volume spikes and the price is falling (current close below previous close), it issues a Sell signal (high-volume sell-off). In all other cases, or if volume is not significantly higher than normal, it returns Hold."
    }
]

        })
    

class PreviewAlgorithm(views.APIView):
    # use returned code in frontend like:
    '''
    <ReactSyntaxHighlighter language="python">
        {codeFromBackend}
    </ReactSyntaxHighlighter>
    '''

    permission_classes = [IsAuthenticated]
    def post(self, request):
        try:
            algo_type = request.data.get("algo_type")
            algoname = request.data.get("algoname")
            if not algo_type or not algoname:
                return Response({"error": "Missing 'algo_type' or 'algoname'"}, status=400)

            sig = UserSignature.objects.get(user=request.user)

            if algo_type == "public":
                retrieve = f"public/{algoname}.py"
            else:
                retrieve = f"{sig.placeholder}{algoname}.py"

            s3 = boto3.client("s3")
            bucket_name = "stan-user-algos"
            obj = s3.get_object(Bucket=bucket_name, Key=retrieve)
            code = obj["Body"].read().decode("utf-8")

            return Response({
                "algoname": algoname,
                "code": code
            })

        except Exception as e:
            return Response({"error": str(e)}, status=400)


class DeleteUserAlgorithm(views.APIView):
    permission_classes = [IsAuthenticated]

    def delete(self, request):
        algo_name = request.data.get("algoname")
        try:
            sig = UserSignature.objects.get(user=request.user)
            s3 = boto3.client('s3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION_NAME")
            )
            bucket_name = os.getenv("AWS_STORAGE_BUCKET_NAME")
            prefix = f"{sig.placeholder}{algo_name}"

            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if "Contents" in response:
                for obj in response["Contents"]:
                    s3.delete_object(Bucket=bucket_name, Key=obj["Key"])

            pre_len = len(sig.algorithms)
            sig.algorithms = [a for a in sig.algorithms if a["algoname"] != algo_name]
            post_len = len(sig.algorithms)
            print("Number deleted: " + str(pre_len - post_len))
            sig.save()
            return Response({"status": "Deleted"})
        except Exception as e:
            return Response({"error": str(e)}, status=400)

class RunEvaluationWithDB(views.APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            user = request.user
            sig = UserSignature.objects.get(user=user)
            algos_requested = request.data.get("my_algos", [])
            public_algos = request.data.get("public_algos", [])
            compiled_algos = []
            s3 = boto3.client("s3")
            bucket_name = "stan-user-algos" 

            # getting the user-uploaded algos
            for algo_entry in sig.algorithms:
                if algo_entry["algoname"] in algos_requested:
                    compiled_algos.append(f"{sig.placeholder}{algo_entry['algoname']}.py")

            # getting public algos
            for algo_name in public_algos:
                compiled_algos.append(
                    f"public/{algo_name}.py"
                )

            # run the test using updated algorithm definitions with container funcitonality
            assets = request.data["assets"]
            position_length = int(request.data["position_length"])
            gain_percentage = float(request.data["gain_percentage"])
            loss_percentage = float(request.data["loss_percentage"])
            intercept_range = int(request.data["intercept_range"])
            clean_range = int(request.data["clean_range"])
            intercept_needed = int(request.data["intercept_needed"])
            print(compiled_algos)
            results = call_assets(
                assets,
                position_length,
                gain_percentage,
                loss_percentage,
                compiled_algos,  # now a list of dicts {func_name, code}
                intercept_range,
                clean_range,
                intercept_needed,
            )

            PastEvals.objects.create(
                user=user,
                gain_percentage=gain_percentage,
                loss_percentage=loss_percentage,
                position_length=position_length,
                algos_used=algos_requested,
                intercept_range=intercept_range,
                clean_range=clean_range,
                intercept_needed=intercept_needed,
                results=results,
            )

            return Response({"results": results})
        except Exception as e:
            return Response({"error": str(e)}, status=400)





''' 
class RunEvaluationWithDB(views.APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            user = request.user
            sig = UserSignature.objects.get(user=user)
            algos_requested = request.data.get("algos", [])
            compiled_algos = []

            s3 = boto3.client("s3")
            bucket_name = "stan-user-algos"

            print("1fine!")

            for algo_entry in sig.algorithms:
                if algo_entry["algoname"] in algos_requested:
                    prefix = f"{sig.placeholder}{algo_entry['algoname']}"
                    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

                    matching_keys = [
                        obj for obj in response.get("Contents", [])
                        if obj["Key"] == f"{sig.placeholder}{algo_entry['algoname']}.py"
                    ]

                    if matching_keys:
                        latest = max(matching_keys, key=lambda x: x["LastModified"])
                        obj = s3.get_object(Bucket=bucket_name, Key=latest["Key"])
                        code = obj["Body"].read().decode("utf-8")
                        print(f"--- File from S3  ---\n{code}\n--- End ---")
                        local_namespace = {}
                        exec(code, {}, local_namespace)
                        compiled_algos.append(local_namespace[algo_entry["algoname"]])

            print("2fine!")
            assets = request.data["assets"]
            position_length = int(request.data["position_length"])
            gain_percentage = float(request.data["gain_percentage"])
            loss_percentage = float(request.data["loss_percentage"])
            intercept_range = int(request.data["intercept_range"])
            clean_range = int(request.data["clean_range"])
            intercept_needed = int(request.data["intercept_needed"])

            results = call_assets(
                assets,
                position_length,
                gain_percentage,
                loss_percentage,
                compiled_algos,
                intercept_range,
                clean_range,
                intercept_needed,
            )

            PastEvals.objects.create(
                user=user,
                gain_percentage=gain_percentage,
                loss_percentage=loss_percentage,
                position_length=position_length,
                algos_used=algos_requested,
                intercept_range=intercept_range,
                clean_range=clean_range,
                intercept_needed=intercept_needed,
                results=results,
            )

            return Response({"results": results})
        except Exception as e:
            return Response({"error": str(e)}, status=400)
'''