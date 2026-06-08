# llm-based-diet-score
# Multi-Agent Personalized Dietary Assessment Framework

This repository showcases our personalized dietary assessment framework deployed with `vLLM`. The project supports overall dietary assessment together with disease-aware dietary scoring across 13 physiological systems.

This  version of the repository keeps a compact set of project assets:

- one overall `README.md`
- a compact CSV demo set in `data/demo.csv`
- the core code files used for inference and downstream survival analysis in `code/`
- a startup script for vLLM deployment and inference in `scripts/`
- a Python dependency list in `requirements.txt`

<img width="3491" height="2921" alt="Figure1" src="https://github.com/user-attachments/assets/899ab03a-747c-4c58-957b-75b4d63f524c" />


Project title: Large language model-based dietary assessment in large-scale populations

1. Overview

This project provides a large language model-based framework for personalized dietary health assessment in large-scale populations. The software generates an overall dietary health score and multiple physiological system-specific dietary scores from individual dietary information and personal health background text.

The code package consists of two main components.

The first component is the LLM inference module. It uses vLLM or another OpenAI-compatible API to perform inference. The module reads JSONL input files containing participant-level dietary and health background information, applies predefined prompts, calls the LLM API, and exports the model responses as JSONL and CSV files.

The second component is the downstream statistical analysis and interpretability module. This module generates the main analytical results and figures reported in the manuscript, including LASSO global surrogate models, SHAP individual- and population-level explanations, model scaling analyses, C-index comparisons, incremental prediction model evaluation, hazard ratio summaries, and mediation analysis visualizations.

2. Required content

This software package includes the following required content:

1. Source code for LLM inference, vLLM deployment, API-based inference, and downstream statistical analyses.

2. A small demonstration dataset for testing the software. The example input file should be placed at:

data/source_data/sample_input.jsonl

This file is in JSONL format, with one participant per line. Each line contains a participant ID and the full dietary and health background text.

3. This README file, which provides system requirements, installation instructions, demo instructions, expected output, expected run time, and instructions for running the software on user-provided data.

4. Where applicable, expected output files or example output files should be provided under:

data/result/

3. Directory structure

The recommended directory structure is as follows:

.
├── README.txt
├── deploy_vllm_model.sh
├── run_api_inference.sh
├── overall_diet_score_api_generate.py
├── 01_Survival_Analysis_and_Cohort_Validation/
├── 02_Proteomics_Analysis/
├── 03_Interpretability_and_Predictive_Modeling/
│   └── 03_Interpretability_and_Prediction.py
├── data/
│   ├── source_data/
│   │   └── sample_input.jsonl
│   └── result/
├── prompts/
│   ├── overall_diet_score_en.txt
│   ├── overall_diet_score_zh.txt
│   └── overall_diet_score_en_reasoning.txt
├── requirements_api.txt
├── requirements_vllm.txt
└── LICENSE

4. System requirements

4.1 Operating system

The software is recommended to be run on a Linux operating system, especially when deploying a local vLLM server and performing GPU-based inference. The recommended environment is:

Operating system: Ubuntu 20.04 or later
Python: Python 3.9 or later
CUDA: A CUDA version compatible with the installed PyTorch and vLLM versions
GPU: NVIDIA GPU is recommended for local vLLM inference

If users only call an already deployed OpenAI-compatible API and do not deploy vLLM locally, the API inference script can also be run on a standard CPU machine.

4.2 Software dependencies

This project separates the software environment into two parts.

The first environment is for vLLM model serving. It is used to deploy a local or server-side LLM API. Please install dependencies from:

requirements_vllm.txt

This file should include vLLM, PyTorch, Transformers, Ray, and other model-serving dependencies.

The second environment is for API-based inference. It is used to call an already running vLLM server or another OpenAI-compatible API. Please install dependencies from:

requirements_api.txt

This file should include requests, tqdm, and other packages required by the API calling script.

4.3 Required non-standard hardware

If the user intends to deploy an LLM locally, NVIDIA GPU hardware is usually required. The required GPU memory depends on the model size, maximum context length, and tensor parallel configuration. Large models may require multiple GPUs.

If the user only calls a remote API or an already deployed vLLM API server, no non-standard hardware is required.

5. Installation guide

5.1 Create a Python environment

We recommend using conda or venv to create an isolated Python environment. For example:

conda create -n diet_score python=3.9 -y
conda activate diet_score

Alternatively:

python -m venv diet_score_env
source diet_score_env/bin/activate

5.2 Install API inference dependencies

If users only need to run the API inference script, install:

python -m pip install -r requirements_api.txt

5.3 Install vLLM serving dependencies

If users need to deploy a local or server-side vLLM model, install:

python -m pip install -r requirements_vllm.txt

Note that vLLM, PyTorch, and CUDA versions should be mutually compatible. Users should select appropriate PyTorch and vLLM versions according to the CUDA version available on their server.

5.4 Typical installation time

On a normal desktop computer or server, installing the API inference environment usually takes approximately 5–10 minutes, depending on the network environment.

Installing the vLLM serving environment usually takes approximately 10–30 minutes, depending on CUDA, PyTorch, vLLM, and other dependencies.

6. Demo dataset

The example input file is:

data/source_data/sample_input.jsonl

The input file is in JSONL format, with one participant per line. An example line is:

{"id": "2848581", "input": "[Basic information]\nSex: ...\n[Dietary information]\n..."}

where:

id: unique participant identifier
input: participant-level basic information, dietary information, and health background text

The default demo file contains a small number of example participants and is intended to test whether the full inference pipeline runs correctly.

7. Prompt files

Prompt files are stored in:

prompts/

The main prompt files include:

overall_diet_score_en.txt: English score-only prompt, used by default
overall_diet_score_zh.txt: Chinese score-only prompt
overall_diet_score_en_reasoning.txt: English reasoning prompt, requiring both reasoning_process and score outputs

All prompt files use {input} as the placeholder for participant information. During inference, the script fills this placeholder with each participant's input text.

8. Running the demo

8.1 Start the vLLM API server

If users need to deploy the model locally, first start the vLLM API server:

MODEL_PATH=/path/to/model 
SERVED_MODEL_NAME=openchat 
HOST=0.0.0.0 
PORT=8000 
TENSOR_PARALLEL_SIZE=8 
bash deploy_vllm_model.sh

where:

MODEL_PATH: local path to the LLM
SERVED_MODEL_NAME: model name exposed by the API server
HOST: API server host
PORT: API server port
TENSOR_PARALLEL_SIZE: number of GPUs used for tensor parallelism

This command starts an OpenAI-compatible API server. The default endpoint is:

[http://HOST:PORT/v1/chat/completions](http://HOST:PORT/v1/chat/completions)

Please keep this process running while inference jobs are being performed.

8.2 Run API inference demo

After the vLLM API server is running, execute:

BASE_URL=http://localhost:8000/v1 
MODEL_NAME=openchat 
bash run_api_inference.sh

The script reads:

data/source_data/sample_input.jsonl

and uses the default prompt:

prompts/overall_diet_score_en.txt

The default output files are:

data/result/overall_diet_score_api_result.jsonl
data/result/overall_diet_score_api_result.csv

8.3 Calling a remote OpenAI-compatible API

If users do not deploy vLLM locally and instead call a remote API, use:

API_PROVIDER=openai 
BASE_URL=https://api.openai.com/v1 
MODEL_NAME=gpt-4o-mini 
API_KEY=your_api_key 
bash run_api_inference.sh

Please replace your_api_key with a valid API key.

9. Expected output

After running the demo, two default output files are generated:

1. JSONL output:

data/result/overall_diet_score_api_result.jsonl

2. CSV output:

data/result/overall_diet_score_api_result.csv

The CSV file usually contains the following columns:

id
information
raw_response

where:

id: participant ID
information: participant information sent to the model
raw_response: raw model response containing the dietary scores

The expected model response is a JSON object. For the English score-only prompt, the expected response is:

{
"overall_score": int,
"cancer_score": int,
"circulatory_score": int,
"endocrine_score": int,
"digestive_score": int,
"musculo_score": int,
"nervous_score": int,
"mental_score": int,
"respiratory_score": int,
"genito_score": int,
"blood_score": int,
"skin_score": int,
"ear_score": int,
"eye_score": int
}

If the reasoning prompt is used, the output should also contain:

{
"reasoning_process": string
}

10. Expected run time for the demo

The demo run time depends on the model size, GPU performance, number of input samples, and maximum generation length.

For a 5-case example dataset:

If a small or medium-sized model is deployed locally with vLLM, inference typically takes from several tens of seconds to a few minutes.

If a remote API is used, the run time mainly depends on the API response speed and usually ranges from several tens of seconds to a few minutes.

11. Instructions for use on user-provided data

Users can prepare their own input data in JSONL format and place it under:

data/source_data/

Each line should be a JSON object in the following format:

{"id": "participant_id", "input": "participant dietary and health background text"}

Then run:

INPUT_FILE=data/source_data/your_input.jsonl 
OUTPUT_FILE=data/result/your_output.jsonl 
OUTPUT_CSV=data/result/your_output.csv 
BASE_URL=http://localhost:8000/v1 
MODEL_NAME=openchat 
bash run_api_inference.sh

To switch prompt files, set PROMPT_FILE. For example, to use the Chinese prompt:

PROMPT_FILE=prompts/overall_diet_score_zh.txt 
INPUT_FILE=data/source_data/your_input.jsonl 
OUTPUT_FILE=data/result/your_output_zh.jsonl 
OUTPUT_CSV=data/result/your_output_zh.csv 
BASE_URL=http://localhost:8000/v1 
MODEL_NAME=openchat 
bash run_api_inference.sh

To use the English reasoning prompt:

PROMPT_FILE=prompts/overall_diet_score_en_reasoning.txt 
INPUT_FILE=data/source_data/your_input.jsonl 
OUTPUT_FILE=data/result/your_output_reasoning.jsonl 
OUTPUT_CSV=data/result/your_output_reasoning.csv 
BASE_URL=http://localhost:8000/v1 
MODEL_NAME=openchat 
bash run_api_inference.sh

12. Downstream analysis and figure generation

The downstream analysis code is located at:

03_Interpretability_and_Predictive_Modeling/03_Interpretability_and_Prediction.py

This script is used to generate interpretability and prediction model evaluation results reported in the manuscript, including:

LASSO global surrogate models
Food-level feature contributions to the overall score and 13 system-specific scores
SHAP individual-level explanations
SHAP population-level feature importance
Scaling law analysis of model size and C-index
C-index comparison between the LLM dietary score and established dietary indices
Incremental prediction model comparison using age, sex, lifestyle variables, and the LLM dietary score
Hazard ratio result summaries
Mediation analysis bubble plots

Before running this script, set the data root directory in the script:

DATA_ROOT = os.path.join("your", "data", "root", "path")

Then run:

python 03_Interpretability_and_Prediction.py

The analysis results are saved by default under the results folders within DATA_ROOT.

13. Reproduction instructions

To reproduce the main quantitative results in the manuscript, users should follow these steps:

1. Construct participant-level dietary and health background text from the original cohort data.

2. Use the LLM inference pipeline in this repository to generate the overall dietary score and system-specific dietary scores.

3. Merge the generated scores with cohort outcomes, covariates, proteomics data, and disease endpoint data.

4. Run the survival analysis and cohort validation code to generate hazard ratios, C-index values, and related statistical results.

5. Run the proteomics analysis code to generate protein association and mediation analysis results.

6. Run the interpretability and prediction modeling code to generate LASSO, SHAP, scaling law, C-index comparison, and incremental prediction model results.

Full reproduction requires access to large-scale cohort data, proteomics data, and the corresponding data permissions. Therefore, this code package provides a small example dataset only for demonstrating the software workflow.

14. Code functionality

The main functions of this code package are:

1. Reading JSONL data containing participant-level dietary and health information.

2. Filling predefined prompt templates with each participant's information.

3. Calling an LLM through vLLM or another OpenAI-compatible API.

4. Extracting the overall dietary score and system-specific dietary scores from the model response.

5. Saving inference results as JSONL and CSV files.

6. Performing cohort validation and survival analysis based on the generated dietary scores.

7. Evaluating the incremental predictive value of the LLM dietary score for mortality and disease risk prediction.

8. Interpreting the relationship between food features and model-generated dietary scores using LASSO and SHAP.

9. Exploring potential biological pathways through proteomics and mediation analyses.

10. Generating the main figures and supplementary analysis results reported in the manuscript.

15) License

16. Code repository

The code is available at:

https://github.com/AI4HEALTH-LAB-THU-DIET/llm-based-diet-score/tree/main

17. Contact

For questions about code execution, data formatting, or result reproduction, please contact the corresponding author or the code maintainer.
