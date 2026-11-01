# Narrated demonstration guide

Record a five to six minute walkthrough after cloud deployment. This guide is not a submitted video. Replace all placeholders and show your own cloud account evidence. Do not display secrets or SSH private keys.

| Time | Presenter | Show | Explain |
|---|---|---|---|
| 00:00–00:35 | Mohnish | Report cover and architecture | State group ID, both names, the fraud review problem, and the three objectives. Explain historical training versus request-time inference. |
| 00:35–01:10 | Mohnish | Cloud VM overview and `docker compose ps -a` | Establish that the services run on the cloud VM. Explain private SSH access, persistent volumes, and service responsibilities. |
| 01:10–02:15 | Mohnish | Data quality dashboard, charts and CSV artifacts | Show 284,807 source records, deduplication, missing-value induction, train-only median imputation and scaling, correlation, amount bins, one-hot encoding, and univariate/bivariate charts. |
| 02:15–03:00 | Mohnish | Prefect deployment, run history and logs | Show the active 120-second schedule and two completed scheduled runs. Open a run and explain task-level logs and retries. |
| 03:00–04:00 | Teammate | Model metrics, model diagnostic chart and training code | Explain the stratified 70/30 split, balanced logistic regression and random forest. Compare accuracy, precision, recall and F1; show confusion-matrix counts and why accuracy alone misleads. |
| 04:00–05:00 | Teammate | Built-in Prefect REST docs and JSON responses | Call flows/filter, deployments/filter and flow_runs/filter. Identify at least four returned application details. Do not substitute only custom API metadata. |
| 05:00–05:40 | Teammate | FastAPI Swagger UI and dashboard scoring button | Send the supplied request with all 30 predictors. Show the returned score, review/allow decision and validation failure for an incomplete request. |
| 05:40–06:00 | Both | Contribution table and limitations | Confirm actual contributions. State static benchmark, delayed production labels and cloud evidence status. Close with the achieved objectives. |

Use screen recording with clear narration. Export in a portal-supported format, create the required Group Name folder, and upload the video. Submit the Word/PDF once for the group under the group ID filename. The assignment document lists a due date of 30 October 2026 at 11:55 pm; verify late-submission arrangements with the instructor.
