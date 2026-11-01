from pathlib import Path
from datetime import datetime,timezone
import json,time,os
import pandas as pd
import joblib
from fastapi import FastAPI,HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.docs import get_swagger_ui_html
from pydantic import BaseModel
ROOT=Path(__file__).resolve().parent;OUT=ROOT/'artifacts'
app=FastAPI(title='Financial Fraud Detection API',version='1.0.0',docs_url=None)
app.mount('/vendor',StaticFiles(directory=ROOT/'vendor'),name='vendor')
@app.get('/docs',include_in_schema=False)
def docs():
 return get_swagger_ui_html(openapi_url='/openapi.json',title='Fraud API documentation',swagger_js_url='/vendor/swagger-ui-bundle.js',swagger_css_url='/vendor/swagger-ui.css')
app.mount('/artifacts',StaticFiles(directory=OUT),name='artifacts')
def load(n): return json.loads((OUT/n).read_text())
class Prediction(BaseModel):
 features:dict[str,float]
@app.get('/health')
def health(): return {'status':'ok','model_ready':(OUT/'model.joblib').exists(),'utc':datetime.now(timezone.utc).isoformat()}
@app.get('/application')
def application():
 return {'name':app.title,'version':app.version,'environment':os.getenv('APP_ENV','local demonstration'),'model':load('metrics.json')['deployment_candidate'],'flow':'fraud-data-pipeline','schedule_seconds':120,'threshold':0.5}
@app.get('/metrics')
def metrics(): return load('metrics.json')
@app.get('/data-quality')
def quality(): return {'dataset':load('data.json'),'eda':load('eda.json')}
@app.post('/predict')
def predict(p:Prediction):
 meta=load('metrics.json');cols=meta['features']
 if set(p.features)!=set(cols): raise HTTPException(422,detail='Exactly the 30 documented features are required')
 import math
 if not all(math.isfinite(v) for v in p.features.values()): raise HTTPException(422,detail='Features must be finite')
 t=time.perf_counter();m=joblib.load(OUT/'model.joblib');score=float(m.predict_proba(pd.DataFrame([p.features],columns=cols))[0,1]);latency=(time.perf_counter()-t)*1000
 result={'fraud_probability':score,'decision':'review' if score>=.5 else 'allow','threshold':.5,'model':meta['deployment_candidate'],'latency_ms_including_model_load':round(latency,3)}
 with (OUT/'prediction_log.jsonl').open('a') as f:f.write(json.dumps({'utc':datetime.now(timezone.utc).isoformat(),**result})+'\n')
 return result
@app.get('/',response_class=HTMLResponse)
def dashboard():
 return (ROOT/'dashboard.html').read_text()
