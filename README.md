# Financial Fraud Detection assignment companion

This package contains the implementation, measured artifacts, and screenshots used in the Word report. The training data is the public ULB credit-card fraud benchmark, downloaded from the URL used by TensorFlow's official imbalanced-data tutorial. It is not bundled; the pipeline downloads it on first execution. No synthetic transaction dataset is substituted.

## Quick start on your computer

Use Python 3.12 and three terminals in this folder.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PREFECT_API_URL=http://127.0.0.1:4200/api
prefect server start
```

In terminal 2, activate the environment and set the same PREFECT_API_URL:

```bash
python pipeline.py
python pipeline.py --serve
```

Training runs the data flow and then trains both models. Allow about a minute or more depending on hardware. The second command registers a deployment with a 120-second interval and keeps a runner alive. Keep that terminal open. In terminal 3:

```bash
uvicorn app:app --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000 for the operations dashboard, http://127.0.0.1:8000/docs for prediction API documentation, http://127.0.0.1:4200 for Prefect, and http://127.0.0.1:4200/api/docs for the built-in orchestration API.

Windows activation is `.venv\Scripts\activate`; set variables using PowerShell `$env:PREFECT_API_URL='http://127.0.0.1:4200/api'`.

## Cloud deployment that completes the remaining assignment evidence

The included screenshots document an executed local orchestration demonstration in the report-generation environment. They do not establish deployment into your cloud account. The Docker configuration is supplied but was not executed here.

1. Create a Linux VM in your own cloud account with Docker Engine and Docker Compose v2. A starting configuration is 2 virtual CPUs, 8 GB RAM, and 15 GB free disk. This is a resource recommendation, not a benchmark guarantee.
2. Restrict inbound SSH to your IP. Do not expose ports 4200 or 8000 publicly. Use an SSH key and transfer the extracted project folder to the VM.
3. On the VM run `docker compose up --build -d`. The `train` service runs once and the `scheduler` and `api` services start after successful training. Named volumes persist the dataset, models and Prefect database.
4. Check `docker compose ps -a`, `docker compose logs train`, and `docker compose logs scheduler`. Wait at least five minutes after the scheduler starts. Both completed and future scheduled runs should appear.
5. From your laptop run `ssh -L 4200:localhost:4200 -L 8000:localhost:8000 USER@VM_IP`. Open the same localhost URLs above: this time the applications run on the cloud VM through the SSH tunnel.
6. Capture the provider instance overview (redact account details), VM hostname and `docker compose ps -a`, Prefect deployment interval, at least two completed scheduled runs with timestamps two minutes apart, task logs, model metrics, built-in API responses, and a successful prediction. Add these to the Word report and update its execution-status paragraph.
7. Run the API commands below on the VM and save actual JSON. Replace the report's demonstration UUIDs with your new UUIDs. Review group identifiers, teammate name, BITS IDs and actual contributions. Record the required narrated video using VIDEO_SCRIPT.md, then submit using the required group ID filename.

To stop the demonstration: `docker compose down`. Do not add `-v` unless you intend to delete the saved data and run history. Stop or delete the VM when no longer needed to avoid continued charges.

## Built-in Prefect API evidence

```bash
curl -s http://localhost:4200/api/health
curl -s -X POST http://localhost:4200/api/flows/filter -H 'Content-Type: application/json' -d '{"limit":10}'
curl -s -X POST http://localhost:4200/api/deployments/filter -H 'Content-Type: application/json' -d '{"limit":10}'
curl -s -X POST http://localhost:4200/api/flow_runs/filter -H 'Content-Type: application/json' -d '{"limit":20}'
curl -s -X POST http://localhost:4200/api/task_runs/filter -H 'Content-Type: application/json' -d '{"limit":30}'
curl -s -X POST http://localhost:4200/api/logs/filter -H 'Content-Type: application/json' -d '{"limit":100}'
curl -s -X POST http://localhost:8000/predict -H 'Content-Type: application/json' --data-binary @artifacts/sample_request.json
```

The four built-in details to demonstrate are flow name and ID, deployment name and ID, 120-second schedule with active status, and completed flow-run state with start/end timestamps. The custom `/application` endpoint is supplementary and is not substituted for the built-in platform APIs.

## Design boundaries

The 70/30 stratified split follows the rubric. Exact duplicate rows are removed before the split to reduce direct overlap. Median imputation and standardization are fitted on training rows only. Amount bands and one-hot encoding demonstrate the EDA requirement; models use the original 30 numeric predictors. No SMOTE, hyperparameter search, or test-set threshold tuning is performed. Random forest is the predeclared deployment candidate.

The two-minute flow reprocesses the same static snapshot and re-logs frozen holdout metrics. It is not live concept-drift monitoring. Prediction scores are uncalibrated estimates, and `review` is an analyst routing suggestion, not proof of fraud. API logs omit raw transaction vectors. Production use requires authentication, HTTPS, authorization, secure model delivery, drift monitoring and delayed labels. Model artifacts should only be loaded from a trusted source.

`evidence/` stores actual API responses, logs and application screenshots. `artifacts/` contains metrics, summaries, charts and serialized fitted pipelines.
